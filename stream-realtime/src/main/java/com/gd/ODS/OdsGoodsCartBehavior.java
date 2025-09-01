package com.gd.ODS;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 商品加购行为ODS层处理类
 * 读取Kafka中模拟的商品加购流数据，创建ODS层表，筛选有效加购信息并发送至Kafka
 */
public class OdsGoodsCartBehavior {
    // 商品加购行为Kafka输入主题（源数据）
    private static final String ODS_GOODS_CART_INPUT_TOPIC = ConfigUtils.getString("kafka.ods.goods.cart.behavior");
    // 商品加购行为Kafka输出主题（筛选后有效数据）
    private static final String DWD_GOODS_CART_OUTPUT_TOPIC = ConfigUtils.getString("kafka.dwd.goods.cart.valid.behavior");

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 2. 初始化Table环境（基于流环境）
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 创建ODS层Kafka输入表
        tEnv.executeSql("CREATE TABLE ods_goods_cart_behavior (\n" +
                "  `user_id` STRING COMMENT '用户唯一标识，记录加购行为发起主体',\n" +
                "  `cart_time` TIMESTAMP(3) COMMENT '加购时间（精确到毫秒），确定统计周期内有效加购行为',\n" +  // 匹配文档实时统计时间精度
                "  `goods_id` STRING COMMENT '被加购商品唯一标识，用于【加购件数Top50】榜单商品维度关联',\n" +  // 对应文档【加购件数Top50】商品维度
                "  `cart_quantity` INT COMMENT '加购件数，直接对应文档中“加购件数=统计周期内件数之和”的指标定义',\n" +  // 文档核心指标字段
                "  `shop_id` STRING COMMENT '店铺唯一标识，定位数据所属店铺，匹配文档“店铺内商品”统计范围',\n" +  // 对应文档“店铺内”数据筛选要求
                "  `cart_status` STRING COMMENT '加购状态，枚举值：normal（正常加购）、cancel（取消加购），筛选有效加购数据',\n" +
                "  `data_source` STRING COMMENT '数据来源（日志采集、接口同步），用于数据溯源',\n" +
                "  `dt` STRING COMMENT '分区字段（格式：yyyyMMdd），按日期分区存储',\n" +
                "  `proc_time` AS proctime() COMMENT 'Flink处理时间，用于实时计算场景，匹配文档实时榜单需求',\n" +  // 支撑文档“实时维度”统计
                "  -- 事件时间水印（与TIMESTAMP(3)精度匹配，确保实时统计周期准确性）\n" +
                "  WATERMARK FOR cart_time AS cart_time - INTERVAL '3' SECOND \n" +
                ")" +
                SqlUtil.getKafka(ODS_GOODS_CART_INPUT_TOPIC, "0"));

        //过滤ods层数据准备插入到dwd层
        Table dwd_goods_cart = tEnv.sqlQuery("SELECT\n" +
                "  user_id,\n" +
                "  cart_time,\n" +
                "  goods_id,\n" +
                "  CASE WHEN cart_quantity < 0 THEN 0 ELSE cart_quantity END AS cart_quantity,\n" +
                "  shop_id,\n" +
                "  cart_status,\n" +
                "  data_source,\n" +
                "  dt,\n" +
                "  DATE_FORMAT(cart_time, 'HH') AS cart_hour\n" +
                "FROM\n" +
                "  ods_goods_cart_behavior\n" +
                "WHERE\n" +
                "  cart_status = 'normal'\n" +
                "  AND user_id IS NOT NULL\n" +
                "  AND goods_id IS NOT NULL\n" +
                "  AND shop_id IS NOT NULL;");

        tEnv.executeSql(
                "create table " + DWD_GOODS_CART_OUTPUT_TOPIC + "(" +
                        "`user_id` STRING COMMENT '用户唯一标识'," +
                        "`cart_time` TIMESTAMP(3) COMMENT '加购时间（精确到毫秒）'," +
                        "`goods_id` STRING COMMENT '被加购商品唯一标识'," +
                        "`cart_quantity` INT COMMENT '加购件数（非负处理后）'," +
                        "`shop_id` STRING COMMENT '店铺唯一标识'," +
                        "`cart_status` STRING COMMENT '加购状态（仅保留normal）'," +
                        "`data_source` STRING COMMENT '数据来源'," +
                        "`dt` STRING COMMENT '日期分区字段'," +
                        "`cart_hour` STRING COMMENT '小时分区（由cart_time提取）'," +
                        "`proc_time` AS proctime() COMMENT 'Flink处理时间'," +
                        "WATERMARK FOR cart_time AS cart_time - INTERVAL '3' SECOND," +
                        "primary key(user_id, goods_id) not enforced " +  // 复合主键，根据业务唯一性确定
                        ")" + SqlUtil.getUpsertKafkaDDL(DWD_GOODS_CART_OUTPUT_TOPIC));


        dwd_goods_cart.executeInsert(DWD_GOODS_CART_OUTPUT_TOPIC);


        // 6. 提交Flink作业
      //  env.execute();
    }
}