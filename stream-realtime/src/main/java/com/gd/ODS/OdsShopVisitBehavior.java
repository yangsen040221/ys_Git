package com.gd.ODS;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SqlUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 店铺访问行为ODS层处理类
 * 支撑工单要求的【访客数Top50】实时榜单数据预处理
 * 工单编号：大数据-用户画像-05-动态主题实时榜单
 */
public class OdsShopVisitBehavior {
    // 从配置文件读取Kafka主题，适配实时数据接入场景
    private static final String ODS_SHOP_VISIT_TOPIC = ConfigUtils.getString("kafka.ods_shop_visit_behavior");
    private static final String DWD_SHOP_VISIT_TOPIC = ConfigUtils.getString("kafka.dwd_shop_visit_behavior");

    public static void main(String[] args) throws Exception {
        // 1. 初始化Flink流执行环境，保证实时处理性能
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        // 2. 初始化Table环境，支持SQL化实时数据处理
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // 3. 创建ODS层店铺访问行为表（读取Kafka原始数据，保留核心字段）
        tEnv.executeSql("CREATE TABLE ods_shop_visit_behavior (" +
                "user_id STRING," + // 用户唯一标识（脚本中为数字字符串，匹配）
                "visit_time TIMESTAMP(3)," + // 访问时间（脚本中格式为%Y-%m-%d %H:%M:%S，可解析为TIMESTAMP）
                "visit_behavior STRING," + // 访问行为类型（匹配脚本中的VISIT_BEHAVIORS列表）
                "goods_id STRING," + // 被访问商品ID（脚本中存在null值，STRING类型可兼容）
                "shop_id STRING," + // 店铺ID（匹配脚本中的shop_xxx格式）
                "visit_platform STRING," + // 访问平台（匹配脚本中的VISIT_PLATFORMS列表）
                "data_source STRING," + // 数据来源（匹配脚本中的DATA_SOURCE列表）
                "dt STRING," + // 日期分区字段（脚本中为%Y%m%d格式，如'20240520'，注意与注释中的格式一致）
                "proc_time AS proctime()," + // Flink处理时间（仅ODS层使用，无需传入DWD）
                // 基于访问时间定义水位线，允许3秒延迟（适配日志采集乱序场景，合理）
                "WATERMARK FOR visit_time AS visit_time - INTERVAL '3' SECOND " +
                ")" + SqlUtil.getKafka(ODS_SHOP_VISIT_TOPIC, "test"));

        // 4. ODS层数据过滤，生成DWD层数据（关键修改：删除proc_time的SELECT，避免字段数量不匹配）
        Table dwd_shop_visit = tEnv.sqlQuery("-- ODS层店铺访问行为数据过滤查询（支撑工单【访客数Top50】指标统计）\n" +
                "SELECT\n" +
                "    user_id,          -- 用户唯一标识，用于工单“访客数去重”统计\n" +
                "    visit_time,       -- 访问时间，匹配工单“实时维度”下的时间窗口计算\n" +
                "    visit_behavior,   -- 访问行为类型，筛选工单定义的有效访问场景\n" +
                "    goods_id,         -- 被访问商品ID，关联商品维度统计（允许null，适配非商品页面访问）\n" +
                "    shop_id,          -- 店铺ID，限定工单“店铺内商品”的统计范围\n" +
                "    visit_platform,   -- 访问平台，保留用于多端访问分析（不影响核心指标）\n" +
                "    data_source,      -- 数据来源，确保数据可靠性（仅保留脚本中有效来源）\n" +
                "    dt                -- 日期分区，支撑工单指标按日回溯统计（删除proc_time，字段数量匹配DWD表）\n" +
                "FROM\n" +
                "    ods_shop_visit_behavior\n" +
                "WHERE\n" +
                "    -- 过滤条件1：核心字段非空，避免无效数据影响工单指标准确性\n" +
                "    user_id IS NOT NULL AND user_id <> ''\n" +
                "    AND shop_id IS NOT NULL AND shop_id <> ''\n" +
                "    AND visit_time IS NOT NULL\n" +
                "    AND dt IS NOT NULL AND dt <> ''\n" +
                "    -- 过滤条件2：仅保留工单定义的有效访问行为（与Kafka模拟数据的VISIT_BEHAVIORS匹配）\n" +
                "    AND visit_behavior IN (\n" +
                "        '直播间观看',    -- 对应工单“观看店铺自播直播间”\n" +
                "        '短视频观看',    -- 对应工单“观看自制全屏页短视频”\n" +
                "        '图文浏览',      -- 对应工单“浏览店铺自制图文”\n" +
                "        '微详情浏览',    -- 对应工单“浏览全屏微详情”\n" +
                "        '详情页访问'     -- 对应工单“访问宝贝详情页”\n" +
                "    )\n" +
                "    -- 过滤条件3：仅保留脚本中定义的有效数据源（确保数据可溯源）\n" +
                "    AND data_source IN ('日志采集', '接口同步');"); // 直接结束SQL，删除排序


        // 5. 创建DWD层Kafka表（proc_time为自动生成的处理时间列，无需手动传入）
        tEnv.executeSql("CREATE TABLE " + DWD_SHOP_VISIT_TOPIC + "(\n" +
                "`user_id` STRING COMMENT '用户唯一标识，用于访客数去重统计',\n" +
                "`visit_time` TIMESTAMP(3) COMMENT '访问时间，用于时间窗口计算',\n" +
                "`visit_behavior` STRING COMMENT '访问行为类型（如直播间观看、详情页访问等）',\n" +
                "`goods_id` STRING COMMENT '被访问商品ID，允许为null',\n" +
                "`shop_id` STRING COMMENT '店铺ID，用于限定统计范围',\n" +
                "`visit_platform` STRING COMMENT '访问平台（如APP、小程序等）',\n" +
                "`data_source` STRING COMMENT '数据来源（日志采集、接口同步）',\n" +
                "`dt` STRING COMMENT '日期分区字段，格式yyyy-MM-dd',\n" +
                "`proc_time` AS proctime() COMMENT 'Flink处理时间（自动生成，无需上游传入）',\n" +
                "WATERMARK FOR visit_time AS visit_time - INTERVAL '5' SECOND,\n" +
                "PRIMARY KEY(user_id, shop_id, visit_time) NOT ENFORCED  -- 复合主键确保数据唯一性\n" +
                ")" +
                SqlUtil.getUpsertKafkaDDL(DWD_SHOP_VISIT_TOPIC));


        // 6. 将过滤后的DWD层数据写入Kafka表（字段数量、顺序完全匹配，可正常写入）
        dwd_shop_visit.executeInsert(DWD_SHOP_VISIT_TOPIC);
    }
}