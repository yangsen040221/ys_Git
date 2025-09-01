package com.gd.ODS;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


/**
 * 订单支付行为ODS层处理类
 * 读取Kafka中模拟的订单支付流数据，创建ODS层表并清洗为DWD层数据写入Kafka
 */
public class OdsOrderPaymentBehavior {
    // 订单支付行为Kafka主题（从配置文件读取）
    private static final String ODS_ORDER_PAYMENT_TOPIC = ConfigUtils.getString("kafka.ods_order_payment_behavior");
    private static final String DWD_ORDER_PAYMENT_TOPIC = ConfigUtils.getString("kafka.dwd_order_payment_behavior");



    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env); // 加载默认环境参数（如并行度、Checkpoint等）

        // 2. 初始化Table环境（基于流环境，用于SQL操作）
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 3. 创建ODS层订单支付行为表（读取Kafka源数据）
        tEnv.executeSql("CREATE TABLE ods_order_payment_behavior (\n" +
                "  `order_id` STRING COMMENT '订单唯一标识',\n" +
                "  `user_id` STRING COMMENT '用户唯一标识',\n" +
                "  `payment_time` TIMESTAMP(3) COMMENT '支付时间（毫秒级精度）',\n" +
                "  `deposit_pay_time` TIMESTAMP(3) COMMENT '预售定金支付时间（毫秒级精度）',\n" +
                "  `goods_id` STRING COMMENT '商品唯一标识',\n" +
                "  `shop_id` STRING COMMENT '店铺唯一标识',\n" +
                "  `payment_amount` DECIMAL(18,2) COMMENT '基础支付金额',\n" +
                "  `refund_amount` DECIMAL(18,2) COMMENT '退款金额',\n" +
                "  `freight_amount` DECIMAL(18,2) COMMENT '运费金额',\n" +
                "  `red_packet_amount` DECIMAL(18,2) COMMENT '红包抵扣金额',\n" +
                "  `order_type` STRING COMMENT '订单类型（presale=预售，subsidy_managed=百亿补贴半托管等）',\n" +
                "  `payment_status` STRING COMMENT '支付状态（success=成功，fail=失败等）',\n" +
                "  `data_source` STRING COMMENT '数据来源（如APP、H5、PC）',\n" +
                "  `proc_time` AS proctime() COMMENT 'Flink处理时间（用于实时关联）',\n" +
                "  -- 基于支付时间定义水位线，允许3秒延迟（适配网络波动场景）\n" +
                "  WATERMARK FOR payment_time AS payment_time - INTERVAL '3' SECOND \n" +
                ")" +
                // 调用工具类生成Kafka连接配置（如bootstrap-servers、format、consumer.group.id等）
                SqlUtil.getKafka(ODS_ORDER_PAYMENT_TOPIC, "test"));

        // 4. 清洗ODS数据，生成DWD层订单支付表（过滤无效数据、计算统计字段）
        // 修复点：用TIMESTAMPADD替换DATE_ADD，适配TIMESTAMP类型字段的时间增减
        Table dwd_order_payment = tEnv.sqlQuery("SELECT\n" +
                "  order_id,\n" +
                "  user_id,\n" +
                "  payment_time,\n" +
                "  goods_id,\n" +
                "  shop_id,\n" +
                "  -- 计算统计口径支付金额：基础支付+退款+运费（按业务文档规则）\n" +
                "  (payment_amount + refund_amount + freight_amount) AS total_payment_amount,\n" +
                "  order_type,\n" +
                "  payment_status,\n" +
                "  data_source,\n" +
                "  -- 提取支付小时（如10表示10点，用于实时时段榜单分析）\n" +
                "  DATE_FORMAT(payment_time, 'HH') AS payment_hour\n" +
                "FROM\n" +
                "  ods_order_payment_behavior\n" +
                "WHERE\n" +
                "  -- 过滤条件1：仅保留支付成功的数据\n" +
                "  payment_status = 'success'\n" +
                "  -- 过滤条件2：排除非目标订单类型（百亿补贴半托管、官方竞价）\n" +
                "  AND order_type NOT IN ('subsidy_managed', 'official_bidding')\n" +
                "  -- 过滤条件3：预售订单仅保留付清尾款的（支付时间晚于定金支付+7天）\n" +
                "  AND NOT (order_type = 'presale' AND payment_time < TIMESTAMPADD(DAY, 7, deposit_pay_time))\n" +
                "  -- 过滤条件4：核心字段非空（保证数据完整性）\n" +
                "  AND user_id IS NOT NULL\n" +
                "  AND goods_id IS NOT NULL\n" +
                "  AND shop_id IS NOT NULL");


        // 5. 创建DWD层Kafka表（用于写入清洗后的数据）
        tEnv.executeSql("CREATE TABLE " + DWD_ORDER_PAYMENT_TOPIC + "(\n" +
                "`order_id` STRING COMMENT '订单唯一标识',\n" +
                "`user_id` STRING COMMENT '用户唯一标识',\n" +
                "`payment_time` TIMESTAMP(3) COMMENT '支付时间（毫秒级精度）',\n" +
                "`goods_id` STRING COMMENT '商品唯一标识',\n" +
                "`shop_id` STRING COMMENT '店铺唯一标识',\n" +
                "`total_payment_amount` DECIMAL(18,2) COMMENT '统计口径支付金额',\n" +
                "`order_type` STRING COMMENT '订单类型',\n" +
                "`payment_status` STRING COMMENT '支付状态',\n" +
                "`data_source` STRING COMMENT '数据来源',\n" +
                "`payment_hour` STRING COMMENT '支付小时（用于时段分析）',\n" +
                "`proc_time` AS proctime() COMMENT 'Flink处理时间',\n" +
                "WATERMARK FOR payment_time AS payment_time - INTERVAL '3' SECOND,\n" +
                "PRIMARY KEY(user_id, goods_id, order_id) NOT ENFORCED  -- 复合主键（确保数据唯一性）\n" +
                ")" +
                // 调用工具类生成Upsert-Kafka配置（支持更新/插入，适配数据重放场景）
                SqlUtil.getUpsertKafkaDDL(DWD_ORDER_PAYMENT_TOPIC));


        // 6. 将DWD层表数据写入目标Kafka主题
        dwd_order_payment.executeInsert(DWD_ORDER_PAYMENT_TOPIC);

        // 7. 执行Flink作业（显式触发，避免环境配置导致的隐式触发失效）
      //  env.execute("");
    }
}