package com.gd.Ods;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;

/**
 * 订单支付行为ODS层处理类
 * 读取Kafka中模拟的订单支付流数据，创建ODS层表
 */
public class OdsOrderPaymentBehavior {
    // 订单支付行为Kafka主题
    private static final String ODS_ORDER_PAYMENT_TOPIC = "ods_order_payment_behavior";

    public static void main(String[] args) {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(2);

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 创建ODS层订单支付行为表
        tEnv.executeSql("CREATE TABLE ods_order_payment_behavior (" +
                "order_id STRING COMMENT '订单唯一标识，关联订单整体信息'," +
                "user_id STRING COMMENT '用户唯一标识，用于支付买家数去重统计'," +
                "payment_time DATETIME COMMENT '支付时间（精确到秒），区分预售订单定金/尾款支付时间'," +
                "goods_id STRING COMMENT '支付商品唯一标识（多商品订单拆分行）'," +
                "shop_id STRING COMMENT '店铺唯一标识，定位数据所属店铺'," +
                "payment_amount DECIMAL(18,2) COMMENT '订单支付金额（不含退款）'," +
                "refund_amount DECIMAL(18,2) COMMENT '退款金额（含售中、售后退款）'," +
                "freight_amount DECIMAL(18,2) COMMENT '运费金额'," +
                "red_packet_amount DECIMAL(18,2) COMMENT '红包抵扣金额（含88VIP红包）'," +
                "order_type STRING COMMENT '订单类型，枚举值：normal、presale、try_first'," +
                "payment_status STRING COMMENT '支付状态，枚举值：success、fail'," +
                "data_source STRING COMMENT '数据来源（日志采集、接口同步）'," +
                "dt STRING COMMENT '分区字段（格式：yyyyMMdd）'," +
                "proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_ORDER_PAYMENT_TOPIC, "ods_order_payment_behavior_group") +
                "PARTITIONED BY (dt)" +
                "COMMENT '存储订单支付原始行为数据，支撑支付金额、支付买家数等指标计算'");

        // 可在此处添加数据处理逻辑（如清洗、过滤等）
        System.out.println("ODS层订单支付行为表创建完成");
    }
}