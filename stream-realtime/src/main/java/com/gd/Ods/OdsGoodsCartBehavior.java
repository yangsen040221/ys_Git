package com.gd.Ods;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 商品加购行为ODS层处理类
 * 读取Kafka中模拟的商品加购流数据，创建ODS层表
 */
public class OdsGoodsCartBehavior {
    // 商品加购行为Kafka主题（根据实际业务调整）
    private static final String ODS_GOODS_CART_TOPIC = ConfigUtils.getString("kafka.ods.goods.cart.behavior");

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env); // 复用项目默认配置

        // 2. 初始化Table环境（基于流环境）
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 创建ODS层Kafka表（修复：调整水印时间精度）
        tEnv.executeSql("CREATE TABLE ods_goods_cart_behavior (\n" +
                "  `user_id` STRING COMMENT '用户唯一标识，记录加购行为发起主体',\n" +
                "  `cart_time` TIMESTAMP(3) COMMENT '加购时间（精确到毫秒），确定统计周期内有效加购行为',\n" +  // 调整精度为3位
                "  `goods_id` STRING COMMENT '被加购商品唯一标识，用于【加购件数Top50】榜单商品维度关联',\n" +
                "  `cart_quantity` INT COMMENT '加购件数，直接对应文档中“加购件数=统计周期内件数之和”的指标定义',\n" +
                "  `shop_id` STRING COMMENT '店铺唯一标识，定位数据所属店铺',\n" +
                "  `cart_status` STRING COMMENT '加购状态，枚举值：normal（正常加购）、cancel（取消加购），筛选有效加购数据',\n" +
                "  `data_source` STRING COMMENT '数据来源（日志采集、接口同步），用于数据溯源',\n" +
                "  `dt` STRING COMMENT '分区字段（格式：yyyyMMdd），按日期分区存储',\n" +
                "  `ts_ms` BIGINT COMMENT '加购行为的毫秒时间戳，用于时间校准和回溯',\n" +
                "  `proc_time` AS proctime() COMMENT 'Flink处理时间（当前节点系统时间），用于实时计算场景',\n" +
                "  -- 事件时间水印（修复：与TIMESTAMP(3)精度匹配）\n" +
                "  WATERMARK FOR cart_time AS cart_time - INTERVAL '3' SECOND \n" +
                ")" +
                SqlUtil.getKafka(ODS_GOODS_CART_TOPIC, "0"));

        System.out.println("ODS层表 ods_goods_cart_behavior 建表完成");

        // 4. 读取表数据并打印（测试用）
        tEnv.executeSql("SELECT user_id, cart_time, goods_id, cart_quantity, cart_status " +
                        "FROM ods_goods_cart_behavior " +
                        "WHERE cart_status = 'normal'")
                .print();

        // 5. 提交Flink作业
        env.execute("OdsGoodsCartBehaviorJob");
    }
}
