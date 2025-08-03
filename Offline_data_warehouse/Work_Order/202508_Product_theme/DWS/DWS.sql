use gd;


CREATE TABLE `dws_product_relation_stats`
(
    `main_product_id`      INT  NOT NULL COMMENT '主商品ID',
    `related_product_id`   INT  NOT NULL COMMENT '关联商品ID',
    `same_visit_count`     INT DEFAULT 0 COMMENT '同时访问次数',
    `same_favor_count`     INT DEFAULT 0 COMMENT '同时收藏次数',
    `same_cart_count`      INT DEFAULT 0 COMMENT '同时加购次数',
    `same_pay_count`       INT DEFAULT 0 COMMENT '同时支付次数',
    `guide_visit_count`    INT DEFAULT 0 COMMENT '详情页引导访问次数',
    `total_relation_score` INT DEFAULT 0 COMMENT '关联总分（各指标加权和）',
    `stat_date`            DATE NOT NULL COMMENT '统计日期',
    PRIMARY KEY (`main_product_id`, `related_product_id`, `stat_date`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '商品关联指标汇总，用于分析TOP单品与其他商品的关联关系（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';






-- 计算关联指标（按天汇总）
INSERT INTO `dws_product_relation_stats`
SELECT
    main_product_id,
    related_product_id,
    SUM(CASE WHEN behavior_type = 1 THEN 1 ELSE 0 END) AS same_visit_count,
    SUM(CASE WHEN behavior_type = 2 THEN 1 ELSE 0 END) AS same_favor_count,
    SUM(CASE WHEN behavior_type = 3 THEN 1 ELSE 0 END) AS same_cart_count,
    SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) AS same_pay_count,
    SUM(CASE WHEN is_guide_from_detail = 1 AND behavior_type = 1 THEN 1 ELSE 0 END) AS guide_visit_count,
    -- 加权得分：访问1分+收藏2分+加购3分+支付5分
    SUM(CASE
            WHEN behavior_type = 1 THEN 1
            WHEN behavior_type = 2 THEN 2
            WHEN behavior_type = 3 THEN 3
            WHEN behavior_type = 4 THEN 5
            ELSE 0
        END) AS total_relation_score,
    dt AS stat_date
FROM
    dwd_user_behavior_detail
WHERE
    related_product_id IS NOT NULL
  AND dt = DATE_SUB(CURDATE(), INTERVAL 1 DAY)  -- 统计前一天
GROUP BY
    main_product_id, related_product_id, dt;



select * from dws_product_relation_stats;









CREATE TABLE `dws_product_behavior_sequence_stats` (
                                                       `main_product_id` INT NOT NULL COMMENT '主商品ID',
                                                       `visit_to_visit_rate` DECIMAL(10,2) COMMENT '访问→关联访问转化率（%）',
                                                       `visit_to_favor_rate` DECIMAL(10,2) COMMENT '访问→关联收藏转化率（%）',
                                                       `visit_to_cart_rate` DECIMAL(10,2) COMMENT '访问→关联加购转化率（%）',
                                                       `cart_to_pay_rate` DECIMAL(10,2) COMMENT '加购→关联支付转化率（%）',
                                                       `avg_behavior_interval` INT COMMENT '平均行为间隔时间（分钟）',
                                                       `stat_date` DATE NOT NULL COMMENT '统计日期',
                                                       PRIMARY KEY (`main_product_id`, `stat_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品行为序列转化指标（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';




INSERT INTO `dws_product_behavior_sequence_stats`
(
    `main_product_id`,
    `visit_to_visit_rate`,
    `visit_to_favor_rate`,
    `visit_to_cart_rate`,
    `cart_to_pay_rate`,
    `avg_behavior_interval`,
    `stat_date`
)
SELECT
    t1.main_product_id,
    -- 访问→关联访问转化率（使用SUM聚合t2的指标）
    ROUND(
            IF(t1.total_main_visit = 0, 0,
               SUM(t2.same_visit_count) / t1.total_main_visit * 100), 2
        ) AS visit_to_visit_rate,
    -- 访问→关联收藏转化率
    ROUND(
            IF(t1.total_main_visit = 0, 0,
               SUM(t2.same_favor_count) / t1.total_main_visit * 100), 2
        ) AS visit_to_favor_rate,
    -- 访问→关联加购转化率
    ROUND(
            IF(t1.total_main_visit = 0, 0,
               SUM(t2.same_cart_count) / t1.total_main_visit * 100), 2
        ) AS visit_to_cart_rate,
    -- 加购→关联支付转化率
    ROUND(
            IF(t1.total_main_cart = 0, 0,
               SUM(t2.same_pay_count) / t1.total_main_cart * 100), 2
        ) AS cart_to_pay_rate,
    -- 平均行为间隔时间
    AVG(t2.behavior_interval) AS avg_behavior_interval,
    t1.stat_date
FROM (
         -- 主商品总行为统计
         SELECT
             main_product_id,
             COUNT(DISTINCT CASE WHEN behavior_type = 1 THEN id END) AS total_main_visit,
             COUNT(DISTINCT CASE WHEN behavior_type = 3 THEN id END) AS total_main_cart,
             DATE(behavior_time) AS stat_date
         FROM `ods_user_behavior`
         WHERE DATE(behavior_time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
         GROUP BY main_product_id, DATE(behavior_time)
     ) t1
         LEFT JOIN (
    -- 关联商品行为指标
    SELECT
        main_product_id,
        related_product_id,
        user_id,
        SUM(CASE WHEN behavior_type = 1 THEN 1 ELSE 0 END) AS same_visit_count,
        SUM(CASE WHEN behavior_type = 2 THEN 1 ELSE 0 END) AS same_favor_count,
        SUM(CASE WHEN behavior_type = 3 THEN 1 ELSE 0 END) AS same_cart_count,
        SUM(CASE WHEN behavior_type = 4 THEN 1 ELSE 0 END) AS same_pay_count,
        TIMESTAMPDIFF(
                MINUTE,
                MAX(CASE WHEN is_main = 1 THEN behavior_time END),
                MAX(CASE WHEN is_main = 0 THEN behavior_time END)
            ) AS behavior_interval,
        DATE(behavior_time) AS stat_date
    FROM (
             -- 主商品与关联商品行为标记
             SELECT
                 user_id,
                 main_product_id,
                 related_product_id,
                 behavior_type,
                 behavior_time,
                 1 AS is_main
             FROM `ods_user_behavior`
             WHERE related_product_id IS NOT NULL
               AND DATE(behavior_time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
             UNION ALL
             SELECT
                 user_id,
                 related_product_id AS main_product_id,
                 main_product_id AS related_product_id,
                 behavior_type,
                 behavior_time,
                 0 AS is_main
             FROM `ods_user_behavior`
             WHERE related_product_id IS NOT NULL
               AND DATE(behavior_time) = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
         ) t3
    GROUP BY main_product_id, related_product_id, user_id, DATE(behavior_time)
) t2 ON t1.main_product_id = t2.main_product_id AND t1.stat_date = t2.stat_date
GROUP BY t1.main_product_id, t1.stat_date, t1.total_main_visit, t1.total_main_cart;


select * from dws_product_behavior_sequence_stats;







