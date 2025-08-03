use gd;


-- 创建DWS层表（汇总数据）
CREATE TABLE IF NOT EXISTS dws_activity_effect_summary
(
    activity_id       BIGINT NOT NULL COMMENT '活动ID',
    stat_date         DATE   NOT NULL COMMENT '统计日期',
    activity_name     VARCHAR(100) COMMENT '活动名称',
    pv                BIGINT COMMENT '浏览量',
    uv                BIGINT COMMENT '独立访客数',
    click_count       BIGINT COMMENT '点击次数',
    share_count       BIGINT COMMENT '分享次数',
    participate_count BIGINT COMMENT '参与次数',
    conversion_count  BIGINT COMMENT '转化次数',
    conversion_rate   DECIMAL(10, 4) COMMENT '转化率',
    total_amount      DECIMAL(15, 2) COMMENT '总转化金额',
    avg_amount        DECIMAL(15, 2) COMMENT '平均转化金额',
    etl_time          DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(activity_id, stat_date)
COMMENT '活动效果汇总表'
DISTRIBUTED BY HASH(activity_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);


-- DWD到DWS层：活动效果汇总
INSERT INTO dws_activity_effect_summary
SELECT a.activity_id,
       DATE(b.behavior_time)                                                            AS stat_date,
       a.activity_name,
       COUNT(DISTINCT CASE WHEN b.behavior_type = 'view' THEN b.behavior_id END)        AS pv,
       COUNT(DISTINCT CASE WHEN b.behavior_type = 'view' THEN b.user_id END)            AS uv,
       COUNT(DISTINCT CASE WHEN b.behavior_type = 'click' THEN b.behavior_id END)       AS click_count,
       COUNT(DISTINCT CASE WHEN b.behavior_type = 'share' THEN b.behavior_id END)       AS share_count,
       COUNT(DISTINCT CASE WHEN b.behavior_type = 'participate' THEN b.behavior_id END) AS participate_count,
       COUNT(DISTINCT e.effect_id)                                                      AS conversion_count,
       -- 计算转化率
       CASE
           WHEN COUNT(DISTINCT b.behavior_id) = 0 THEN 0
           ELSE COUNT(DISTINCT e.effect_id) / COUNT(DISTINCT b.behavior_id)
           END                                                                          AS conversion_rate,
       SUM(e.amount)                                                                    AS total_amount,
       CASE
           WHEN COUNT(DISTINCT e.effect_id) = 0 THEN 0
           ELSE SUM(e.amount) / COUNT(DISTINCT e.effect_id)
           END                                                                          AS avg_amount,
       CURRENT_TIMESTAMP                                                                AS etl_time
FROM ods_marketing_activity a
         LEFT JOIN ods_user_behavior b ON a.activity_id = b.activity_id
         LEFT JOIN ods_marketing_effect e
                   ON a.activity_id = e.activity_id AND DATE(b.behavior_time) = DATE(e.conversion_time)
GROUP BY a.activity_id, a.activity_name, DATE(b.behavior_time);


select *
from dws_activity_effect_summary;


-- 用户分群活动效果汇总表
CREATE TABLE IF NOT EXISTS dws_user_segment_activity_summary
(
    age_group        VARCHAR(20) NOT NULL COMMENT '年龄组',
    city_level       TINYINT     NOT NULL COMMENT '城市等级',
    stat_date        DATE        NOT NULL COMMENT '统计日期',
    total_uv         BIGINT COMMENT '总独立用户数',
    participate_rate DECIMAL(10, 4) COMMENT '参与率',
    conversion_rate  DECIMAL(10, 4) COMMENT '转化率',
    avg_amount       DECIMAL(15, 2) COMMENT '平均转化金额',
    etl_time         DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(age_group, city_level, stat_date)
COMMENT '用户分群活动效果汇总表'
DISTRIBUTED BY HASH(age_group) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);


INSERT INTO dws_user_segment_activity_summary (
    age_group,
    city_level,
    stat_date,
    total_uv,
    participate_rate,
    conversion_rate,
    avg_amount
)
SELECT
    u.age_group,
    u.city_level,
    COALESCE(b.behavior_date, e.conversion_date) AS stat_date,
    -- 总独立用户数
    COUNT(DISTINCT b.user_id) AS total_uv,
    -- 参与率 = 参与用户数 / 总用户数
    CASE
        WHEN COUNT(DISTINCT b.user_id) = 0 THEN 0
        ELSE ROUND(COUNT(DISTINCT CASE WHEN b.behavior_type = 'participate' THEN b.user_id END)
                       / COUNT(DISTINCT b.user_id), 4)
        END AS participate_rate,
    -- 转化率 = 转化用户数 / 参与用户数
    CASE
        WHEN COUNT(DISTINCT CASE WHEN b.behavior_type = 'participate' THEN b.user_id END) = 0 THEN 0
        ELSE ROUND(COUNT(DISTINCT e.user_id)
                       / COUNT(DISTINCT CASE WHEN b.behavior_type = 'participate' THEN b.user_id END), 4)
        END AS conversion_rate,
    -- 平均转化金额
    CASE
        WHEN COUNT(DISTINCT e.effect_id) = 0 THEN 0
        ELSE ROUND(SUM(e.amount) / COUNT(DISTINCT e.effect_id), 2)
        END AS avg_amount
FROM dwd_user_detail u
-- 关联用户行为数据
         LEFT JOIN dwd_user_behavior_detail b ON u.user_id = b.user_id
-- 关联营销效果数据
         LEFT JOIN dwd_marketing_effect_detail e
                   ON u.user_id = e.user_id
                       AND e.conversion_date = b.behavior_date
GROUP BY u.age_group, u.city_level, COALESCE(b.behavior_date, e.conversion_date)
HAVING stat_date IS NOT NULL AND u.age_group IS NOT NULL AND u.city_level IS NOT NULL;



select * from dws_user_segment_activity_summary;







-- 渠道效果汇总表
CREATE TABLE IF NOT EXISTS dws_channel_effect_summary
(
    channel          VARCHAR(50) NOT NULL COMMENT '渠道',
    activity_type    VARCHAR(20) NOT NULL COMMENT '活动类型',
    stat_date        DATE        NOT NULL COMMENT '统计日期',
    pv               BIGINT COMMENT '浏览量',
    uv               BIGINT COMMENT '独立访客数',
    cost             DECIMAL(15, 2) COMMENT '渠道成本',
    conversion_count BIGINT COMMENT '转化次数',
    total_amount     DECIMAL(15, 2) COMMENT '总转化金额',
    roi              DECIMAL(10, 4) COMMENT '投资回报率',
    etl_time         DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(channel, activity_type, stat_date)
COMMENT '渠道效果每日汇总表'
DISTRIBUTED BY HASH(channel) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);






INSERT INTO dws_channel_effect_summary (
    channel,
    activity_type,
    stat_date,
    pv,
    uv,
    cost,
    conversion_count,
    total_amount,
    roi
)
SELECT
    a.channel,
    a.activity_type,
    COALESCE(b.behavior_date, e.conversion_date) AS stat_date,
    -- 浏览量
    COUNT(DISTINCT CASE WHEN b.behavior_type = 'view' THEN b.behavior_id END) AS pv,
    -- 独立访客数
    COUNT(DISTINCT b.user_id) AS uv,
    -- 渠道成本（按活动预算/活动天数分摊）
    ROUND(SUM(a.budget) / NULLIF(a.duration_days, 0), 2) AS cost,
    -- 转化次数
    COUNT(DISTINCT e.effect_id) AS conversion_count,
    -- 总转化金额
    SUM(e.amount) AS total_amount,
    -- 投资回报率 = (总转化金额 - 成本) / 成本
    CASE
        WHEN ROUND(SUM(a.budget) / NULLIF(a.duration_days, 0), 2) = 0 THEN 0
        ELSE ROUND((SUM(e.amount) - ROUND(SUM(a.budget) / NULLIF(a.duration_days, 0), 2))
                       / ROUND(SUM(a.budget) / NULLIF(a.duration_days, 0), 2), 4)
        END AS roi
FROM dwd_marketing_activity_detail a
-- 关联用户行为数据
         LEFT JOIN dwd_user_behavior_detail b
                   ON a.activity_id = b.activity_id
                       AND b.behavior_date BETWEEN DATE(a.start_time) AND DATE(a.end_time)
-- 关联营销效果数据
         LEFT JOIN dwd_marketing_effect_detail e
                   ON a.activity_id = e.activity_id
                       AND e.conversion_date = b.behavior_date
GROUP BY a.channel, a.activity_type, COALESCE(b.behavior_date, e.conversion_date), a.duration_days
HAVING stat_date IS NOT NULL AND a.channel IS NOT NULL;


select * from dws_channel_effect_summary;