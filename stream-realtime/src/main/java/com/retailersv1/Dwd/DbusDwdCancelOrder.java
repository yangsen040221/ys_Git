package com.retailersv1.Dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * 订单取消事实表处理类
 *
 * 该类负责处理订单取消的业务逻辑，从ODS层读取订单信息变更数据，
 * 过滤出订单取消的数据，并与下单明细数据进行关联，最终形成订单取消事实表。
 */
public class DbusDwdCancelOrder {
    // ODS层Kafka主题配置
    private static final String ODS_KAFKA_TOPIC = ConfigUtils.getString("kafka.cdc.db.topic");
    // DWD层订单取消明细Kafka主题配置
    private static final String DWD_TRADE_ORDER_CANCEL_DETAIL = ConfigUtils.getString("kafka.dwd.cancel.order_detail");
    // DWD层订单明细Kafka主题配置
    private static final String DWD_TRADE_ORDER_DETAIL = ConfigUtils.getString("kafka.dwd.trade.order.detail");
    
    public static void main(String[] args) {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);
        // 设置环境参数
        EnvironmentSettingUtils.defaultParameter(env);

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设置状态保留时间：30分钟+5秒
        tableEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(30*60+5));
        // 设置内存状态后端
        env.setStateBackend(new MemoryStateBackend());

        // 1. 创建ODS层数据表，用于读取原始数据库变更数据
        tableEnv.executeSql("CREATE TABLE ods_professional (\n" +
                "  `op` STRING,\n" +                      // 操作类型：c-插入, u-更新, d-删除
                "  `before` MAP<STRING,STRING>,\n" +     // 变更前数据
                "  `after` MAP<STRING,STRING>,\n" +      // 变更后数据
                "  `source` MAP<STRING,STRING>,\n" +     // 数据源信息
                "  `ts_ms` BIGINT,\n" +                  // 时间戳(毫秒)
                "   proc_time AS proctime()" +           // 处理时间
                ")" + SqlUtil.getKafka(ODS_KAFKA_TOPIC, "test"));

        // 2. 从ODS层数据中过滤出订单取消数据
        // 条件：表名为order_info，操作为更新，更新前订单状态为1001(待支付)，更新后订单状态为1003(已取消)
        Table orderCancel = tableEnv.sqlQuery("select " +
                " `after`['id'] id, " +                 // 订单ID
                " `after`['operate_time'] operate_time, " +  // 操作时间
                " `ts_ms` " +                           // 时间戳
                "from ods_professional " +
                "where `source`['table']='order_info' " +
                "and `op` = 'u' " +
                "and `before`['order_status']='1001' " +
                "and `after`['order_status']='1003' ");
        tableEnv.createTemporaryView("order_cancel", orderCancel);

        // 3. 创建并读取DWD层下单事务事实表数据
        tableEnv.executeSql(
                "create table dwd_order_detail(" +
                        "id string," +                   // 明细ID
                        "order_id string," +             // 订单ID
                        "user_id string," +              // 用户ID
                        "sku_id string," +               // SKU ID
                        "sku_name string," +             // SKU名称
                        "province_id string," +          // 省份ID
                        "activity_id string," +          // 活动ID
                        "activity_rule_id string," +     // 活动规则ID
                        "coupon_id string," +            // 优惠券ID
                        "date_id string," +              // 日期ID
                        "create_time string," +          // 创建时间
                        "sku_num string," +              // SKU数量
                        "split_original_amount string," + // 原始金额(未参与活动、优惠券前的金额)
                        "split_activity_amount string," + // 活动优惠金额
                        "split_coupon_amount string," +  // 优惠券优惠金额
                        "split_total_amount string," +   // 总金额
                        "ts bigint " +                   // 时间戳
                        ")" + SqlUtil.getKafka(DWD_TRADE_ORDER_DETAIL, "retailersv_dwd_order_cancel_detail"));

        // 4. 订单取消表和下单表进行关联，获取完整的订单取消信息
        Table result = tableEnv.sqlQuery(
                "select  " +
                        "od.id," +                      // 明细ID
                        "od.order_id," +                // 订单ID
                        "od.user_id," +                 // 用户ID
                        "od.sku_id," +                  // SKU ID
                        "od.sku_name," +                // SKU名称
                        "od.province_id," +             // 省份ID
                        "od.activity_id," +             // 活动ID
                        "od.activity_rule_id," +        // 活动规则ID
                        "od.coupon_id," +               // 优惠券ID
                        // 格式化订单取消日期
                        "date_format(FROM_UNIXTIME(cast(oc.operate_time as bigint) / 1000), 'yyyy-MM-dd') order_cancel_date_id," +
                        "oc.operate_time," +            // 订单取消时间
                        "od.sku_num," +                 // SKU数量
                        "od.split_original_amount," +   // 原始金额
                        "od.split_activity_amount," +   // 活动优惠金额
                        "od.split_coupon_amount," +     // 优惠券优惠金额
                        "od.split_total_amount," +      // 总金额
                        "oc.ts_ms " +                   // 时间戳
                        "from dwd_order_detail od " +
                        "join order_cancel oc " +
                        "on od.order_id=oc.id ");




        // 5. 创建结果表并写入数据到Kafka
        tableEnv.executeSql(
                "create table "+DWD_TRADE_ORDER_CANCEL_DETAIL+"(" +
                        "id string," +                  // 明细ID
                        "order_id string," +            // 订单ID
                        "user_id string," +             // 用户ID
                        "sku_id string," +              // SKU ID
                        "sku_name string," +            // SKU名称
                        "province_id string," +         // 省份ID
                        "activity_id string," +         // 活动ID
                        "activity_rule_id string," +    // 活动规则ID
                        "coupon_id string," +           // 优惠券ID
                        "date_id string," +             // 日期ID
                        "cancel_time string," +         // 取消时间
                        "sku_num string," +             // SKU数量
                        "split_original_amount string," + // 原始金额
                        "split_activity_amount string," + // 活动优惠金额
                        "split_coupon_amount string," + // 优惠券优惠金额
                        "split_total_amount string," +  // 总金额
                        "ts bigint ," +                 // 时间戳
                        "primary key(id) not enforced " + // 主键约束
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_TRADE_ORDER_CANCEL_DETAIL));

        // 执行插入操作，将结果写入Kafka
       // result.executeInsert(DWD_TRADE_ORDER_CANCEL_DETAIL);
    }
}
