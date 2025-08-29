package com.gd.Ods;

import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import java.time.Duration;

/**
 * 店铺访问行为ODS层处理类
 * 读取Kafka中模拟的店铺访问流数据，创建ODS层表
 */
public class OdsShopVisitBehavior {
    // 店铺访问行为Kafka主题
    private static final String ODS_SHOP_VISIT_TOPIC = "ods_shop_visit_behavior";

    public static void main(String[] args) {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);
        env.setParallelism(2);

        // 创建表环境
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        // 创建ODS层店铺访问行为表
        tEnv.executeSql("CREATE TABLE ods_shop_visit_behavior (" +
                "user_id STRING COMMENT '用户唯一标识（如用户ID、设备ID），用于访客数去重统计'," +
                "visit_time DATETIME COMMENT '访问时间（精确到秒），确定统计周期内有效访问行为'," +
                "visit_behavior STRING COMMENT '访问行为类型，枚举值：直播间观看、短视频观看等'," +
                "goods_id STRING COMMENT '被访问商品唯一标识，非商品页面填NULL'," +
                "shop_id STRING COMMENT '店铺唯一标识，定位数据所属店铺'," +
                "visit_platform STRING COMMENT '访问平台（APP、H5、小程序）'," +
                "data_source STRING COMMENT '数据来源（日志采集、接口同步）'," +
                "dt STRING COMMENT '分区字段（格式：yyyyMMdd）'," +
                "proc_time AS proctime()" +
                ")" + SqlUtil.getKafka(ODS_SHOP_VISIT_TOPIC, "ods_shop_visit_behavior_group") +
                "PARTITIONED BY (dt)" +
                "COMMENT '存储店铺用户访问原始行为数据，支撑浏览量、访客数等指标计算'");

        // 可在此处添加数据处理逻辑（如清洗、过滤等）
        System.out.println("ODS层店铺访问行为表创建完成");
    }
}