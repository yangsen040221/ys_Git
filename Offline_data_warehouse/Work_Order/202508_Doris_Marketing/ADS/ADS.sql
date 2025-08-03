use gd;


-- 营销分析结果表
CREATE TABLE IF NOT EXISTS ads_marketing_analysis
(
    analysis_id            BIGINT NOT NULL AUTO_INCREMENT COMMENT '分析ID',
    stat_date              DATE   NOT NULL COMMENT '统计日期',
    channel                VARCHAR(50) COMMENT '渠道',
    activity_type          VARCHAR(20) COMMENT '活动类型',
    total_pv               BIGINT COMMENT '总浏览量',
    total_uv               BIGINT COMMENT '总独立访客数',
    total_conversion_count BIGINT COMMENT '总转化次数',
    total_conversion_rate  DECIMAL(10, 4) COMMENT '总转化率',
    total_amount           DECIMAL(15, 2) COMMENT '总转化金额',
    avg_conversion_amount  DECIMAL(15, 2) COMMENT '平均转化金额',
    roi                    DECIMAL(10, 4) COMMENT '投资回报率',
    create_time            DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE = OLAP DUPLICATE KEY(analysis_id)
COMMENT '营销分析结果表'
DISTRIBUTED BY HASH(channel) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2"
);


INSERT INTO ads_marketing_analysis (
    stat_date,
    channel,
    activity_type,
    total_pv,
    total_uv,
    total_conversion_count,
    total_conversion_rate,
    total_amount,
    avg_conversion_amount,
    roi
)
SELECT
    stat_date,
    channel,
    activity_type,
    SUM(pv) AS total_pv,
    SUM(uv) AS total_uv,
    SUM(conversion_count) AS total_conversion_count,
    -- 修正：总转化率 = 总转化次数 / 总浏览量（改用dws_channel_effect_summary中存在的pv字段）
    CASE
        WHEN SUM(pv) = 0 THEN 0  -- 用pv作为总行为次数的替代指标
        ELSE ROUND(SUM(conversion_count) / SUM(pv), 4)
        END AS total_conversion_rate,
    SUM(total_amount) AS total_amount,
    -- 平均转化金额 = 总转化金额 / 总转化次数
    CASE
        WHEN SUM(conversion_count) = 0 THEN 0
        ELSE ROUND(SUM(total_amount) / SUM(conversion_count), 2)
        END AS avg_conversion_amount,
    -- 平均ROI
    ROUND(AVG(roi), 4) AS roi
FROM dws_channel_effect_summary
GROUP BY stat_date, channel, activity_type
HAVING stat_date IS NOT NULL;



select * from ads_marketing_analysis;


-- 活动效果排名表
CREATE TABLE IF NOT EXISTS ads_activity_effect_ranking
                              (
                                  ranking_id           BIGINT NOT NULL AUTO_INCREMENT COMMENT '排名ID',
                                  stat_date            DATE   NOT NULL COMMENT '统计日期',
                                  activity_id          BIGINT NOT NULL COMMENT '活动ID',
                                  activity_name        VARCHAR(100) COMMENT '活动名称',
                                  conversion_rate_rank INT COMMENT '转化率排名',
                                  roi_rank             INT COMMENT 'ROI排名',
                                  total_amount_rank    INT COMMENT '转化金额排名',
                                  comprehensive_score  DECIMAL(10, 2) COMMENT '综合得分',  -- 修改为英文列名
                                  create_time          DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
                              ) ENGINE = OLAP DUPLICATE KEY(ranking_id)
COMMENT '活动效果排名表'
DISTRIBUTED BY HASH(activity_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2"
);


INSERT INTO ads_activity_effect_ranking (
    stat_date,
    activity_id,
    activity_name,
    conversion_rate_rank,
    roi_rank,
    total_amount_rank,
    comprehensive_score
)
SELECT
    t.stat_date,
    t.activity_id,
    t.activity_name,
    -- 转化率排名（1为最高）
    ROW_NUMBER() OVER (PARTITION BY t.stat_date ORDER BY t.conversion_rate DESC) AS conversion_rate_rank,
    -- ROI排名（1为最高）
    ROW_NUMBER() OVER (PARTITION BY t.stat_date ORDER BY t.roi DESC) AS roi_rank,
    -- 转化金额排名（1为最高）
    ROW_NUMBER() OVER (PARTITION BY t.stat_date ORDER BY t.total_amount DESC) AS total_amount_rank,
    -- 综合得分（加权计算：转化率40% + ROI30% + 转化金额30%）
    ROUND(
                (
                            (1 - ROW_NUMBER() OVER (PARTITION BY t.stat_date ORDER BY t.conversion_rate DESC)
                                / COUNT(*) OVER (PARTITION BY t.stat_date)) * 0.4 +
                            (1 - ROW_NUMBER() OVER (PARTITION BY t.stat_date ORDER BY t.roi DESC)
                                / COUNT(*) OVER (PARTITION BY t.stat_date)) * 0.3 +
                            (1 - ROW_NUMBER() OVER (PARTITION BY t.stat_date ORDER BY t.total_amount DESC)
                                / COUNT(*) OVER (PARTITION BY t.stat_date)) * 0.3
                    ) * 100, 2
        ) AS comprehensive_score
FROM (
         -- 子查询：通过活动类型+渠道+日期关联，获取ROI
         SELECT
             s.stat_date,
             s.activity_id,
             s.activity_name,
             AVG(s.conversion_rate) AS conversion_rate,  -- 活动日均转化率
             -- 关联渠道效果表的ROI（通过活动类型+渠道+日期组合关联）
             AVG(c.roi) AS roi,
             SUM(s.total_amount) AS total_amount  -- 活动当日总转化金额
         FROM dws_activity_effect_summary s
                  -- 关联活动表获取活动类型和渠道（用于与渠道效果表关联）
                  LEFT JOIN dwd_marketing_activity_detail a
                            ON s.activity_id = a.activity_id
             -- 关联渠道效果表，通过活动类型+渠道+日期匹配
                  LEFT JOIN dws_channel_effect_summary c
                            ON a.activity_type = c.activity_type  -- 活动类型匹配
                                AND a.channel = c.channel  -- 渠道匹配
                                AND s.stat_date = c.stat_date  -- 日期匹配
         GROUP BY s.stat_date, s.activity_id, s.activity_name
     ) t
WHERE t.stat_date IS NOT NULL;


select * from ads_activity_effect_ranking;


-- 用户分群价值分析表
CREATE TABLE IF NOT EXISTS ads_user_segment_value
(
    segment_id            BIGINT NOT NULL AUTO_INCREMENT COMMENT '分群ID',
    age_group             VARCHAR(20) COMMENT '年龄组',
    city_level            TINYINT COMMENT '城市等级',
    user_count            BIGINT COMMENT '用户数量',
    avg_purchase_count    DECIMAL(10, 2) COMMENT '平均购买次数',
    avg_purchase_amount   DECIMAL(15, 2) COMMENT '平均购买金额',
    total_purchase_amount DECIMAL(15, 2) COMMENT '总购买金额',
    user_value_score      DECIMAL(10, 2) COMMENT '用户价值评分',
    last_30d_active_rate  DECIMAL(10, 4) COMMENT '近30天活跃度',
    create_time           DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE = OLAP DUPLICATE KEY(segment_id)
COMMENT '用户分群价值分析表'
DISTRIBUTED BY HASH(age_group) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "storage_format" = "V2"
);


INSERT INTO ads_user_segment_value (
    age_group,
    city_level,
    user_count,
    avg_purchase_count,
    avg_purchase_amount,
    total_purchase_amount,
    user_value_score,
    last_30d_active_rate
)
SELECT
    u.age_group,
    u.city_level,
    -- 分群用户总数
    COUNT(DISTINCT u.user_id) AS user_count,
    -- 平均购买次数 = 总转化次数 / 分群用户数
    ROUND(COUNT(DISTINCT e.effect_id) / NULLIF(COUNT(DISTINCT u.user_id), 0), 2) AS avg_purchase_count,
    -- 平均购买金额 = 总转化金额 / 分群用户数
    ROUND(SUM(e.amount) / NULLIF(COUNT(DISTINCT u.user_id), 0), 2) AS avg_purchase_amount,
    -- 总购买金额
    SUM(e.amount) AS total_purchase_amount,
    -- 用户价值评分（综合总金额、购买次数、平均客单价）
    ROUND(
                (
                            PERCENT_RANK() OVER (ORDER BY SUM(e.amount) DESC) * 0.4 +
                            PERCENT_RANK() OVER (ORDER BY COUNT(DISTINCT e.effect_id) DESC) * 0.3 +
                            PERCENT_RANK() OVER (ORDER BY AVG(e.amount) DESC) * 0.3
                    ) * 100, 2
        ) AS user_value_score,
    -- 近30天活跃度 = 近30天活跃用户数 / 分群用户总数
    ROUND(
                COUNT(DISTINCT CASE
                                   WHEN b.behavior_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                                       THEN u.user_id
                    END) / NULLIF(COUNT(DISTINCT u.user_id), 0), 4
        ) AS last_30d_active_rate
FROM dwd_user_detail u
-- 关联用户行为数据
         LEFT JOIN dwd_user_behavior_detail b ON u.user_id = b.user_id
-- 关联营销效果数据
         LEFT JOIN dwd_marketing_effect_detail e ON u.user_id = e.user_id
WHERE u.age_group IS NOT NULL
  AND u.city_level IS NOT NULL
GROUP BY u.age_group, u.city_level;


select * from ads_user_segment_value;