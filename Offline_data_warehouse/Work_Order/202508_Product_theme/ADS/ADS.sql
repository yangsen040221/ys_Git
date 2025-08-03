use gd;


CREATE TABLE `ads_top_guide_product`
(
    `rank`                 INT          NOT NULL COMMENT '排名',
    `main_product_id`      INT          NOT NULL COMMENT '主商品ID',
    `main_product_name`    VARCHAR(200) NOT NULL COMMENT '主商品名称',
    `related_product_id`   INT          NOT NULL COMMENT '关联商品ID',
    `related_product_name` VARCHAR(200) NOT NULL COMMENT '关联商品名称',
    `guide_visit_count`    INT          NOT NULL COMMENT '引导访问次数',
    `conversion_rate`      DECIMAL(10, 2) COMMENT '转化率（支付/访问）',
    `stat_date`            DATE         NOT NULL COMMENT '统计日期',
    PRIMARY KEY (`rank`, `main_product_id`, `stat_date`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT 'TOP10引导能力商品及关联明细，用于看板展示（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';


-- 先初始化排名变量
SET @rank = 0;

INSERT INTO `ads_top_guide_product`
SELECT
    @rank := @rank + 1 AS `rank`,
    s.main_product_id,
    p1.product_name AS main_product_name,
    s.related_product_id,
    p2.product_name AS related_product_name,
    s.guide_visit_count,
    -- 计算转化率（支付次数/访问次数）
    ROUND(IF(s.same_visit_count = 0, 0, s.same_pay_count / s.same_visit_count * 100), 2) AS conversion_rate,
    s.stat_date
FROM (
         -- 先筛选引导能力TOP10的主商品
         SELECT
             main_product_id,
             SUM(guide_visit_count) AS total_guide
         FROM dws_product_relation_stats
         WHERE stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
         GROUP BY main_product_id
         ORDER BY total_guide DESC
         LIMIT 10
     ) t
         JOIN dws_product_relation_stats s
              ON t.main_product_id = s.main_product_id
         LEFT JOIN ods_product_info p1
                   ON s.main_product_id = p1.product_id
         LEFT JOIN ods_product_info p2
                   ON s.related_product_id = p2.product_id
WHERE
        s.stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
ORDER BY
    t.total_guide DESC,
    s.guide_visit_count DESC;


select * from ads_top_guide_product;





CREATE TABLE `ads_monitor_product_relation`
(
    `monitor_product_id`   INT          NOT NULL COMMENT '监控商品ID',
    `monitor_product_name` VARCHAR(200) NOT NULL COMMENT '监控商品名称',
    `related_product_id`   INT          NOT NULL COMMENT '关联商品ID',
    `related_product_name` VARCHAR(200) NOT NULL COMMENT '关联商品名称',
    `guide_visit_count`    INT          NOT NULL COMMENT '引导访问次数',
    `conversion_count`     INT          NOT NULL COMMENT '转化次数（支付）',
    `stat_date`            DATE         NOT NULL COMMENT '统计日期',
    PRIMARY KEY (`monitor_product_id`, `related_product_id`, `stat_date`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '监控商品连带效果，默认展示引导访客数前10商品（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';






-- 插入监控商品连带效果数据（默认取引导访客数前10的商品）
INSERT INTO `ads_monitor_product_relation`
(
    `monitor_product_id`,
    `monitor_product_name`,
    `related_product_id`,
    `related_product_name`,
    `guide_visit_count`,
    `conversion_count`,
    `stat_date`
)
SELECT
    t.main_product_id AS monitor_product_id,
    p1.product_name AS monitor_product_name,
    s.related_product_id,
    p2.product_name AS related_product_name,
    s.guide_visit_count,
    s.same_pay_count AS conversion_count,
    s.stat_date
FROM (
         -- 筛选引导访客数前10的商品作为默认监控商品
         SELECT
             main_product_id,
             SUM(guide_visit_count) AS total_guide_visits
         FROM `dws_product_relation_stats`
         WHERE stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)  -- 取前一天数据
         GROUP BY main_product_id
         ORDER BY total_guide_visits DESC
         LIMIT 10
     ) t
         INNER JOIN `dws_product_relation_stats` s
                    ON t.main_product_id = s.main_product_id
                        AND s.stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
         LEFT JOIN `ods_product_info` p1
                   ON t.main_product_id = p1.product_id
         LEFT JOIN `ods_product_info` p2
                   ON s.related_product_id = p2.product_id
WHERE p1.is_valid = 1  -- 只统计有效商品
  AND s.related_product_id IS NOT NULL;



select * from ads_monitor_product_relation;













drop table if exists ads_product_sequence_analysis;
CREATE TABLE `ads_product_sequence_analysis` (
                                                 `rank` INT NOT NULL COMMENT '排名',
                                                 `main_product_id` INT NOT NULL COMMENT '主商品ID',
                                                 `main_product_name` VARCHAR(200) NOT NULL COMMENT '主商品名称',
                                                 `visit_to_cart_rate` DECIMAL(10,2) COMMENT '访问→关联加购转化率（%）',
                                                 `cart_to_pay_rate` DECIMAL(10,2) COMMENT '加购→关联支付转化率（%）',
                                                 `avg_behavior_interval` INT COMMENT '平均行为间隔（分钟）',
                                                 `stat_date` DATE NOT NULL COMMENT '统计日期',
                                                 PRIMARY KEY (`rank`, `main_product_id`, `stat_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品行为序列转化分析（TOP10引导商品）（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';




SET @rank=0;
INSERT INTO `ads_product_sequence_analysis`
SELECT
    @rank:=@rank+1 AS `rank`,
    s.main_product_id,
    p.product_name AS main_product_name,
    s.visit_to_cart_rate,
    s.cart_to_pay_rate,
    s.avg_behavior_interval,
    s.stat_date
FROM (
         -- 取引导访问TOP10商品的序列转化指标
         SELECT
             t.main_product_id,
             s.visit_to_cart_rate,
             s.cart_to_pay_rate,
             s.avg_behavior_interval,
             s.stat_date
         FROM (
                  SELECT main_product_id, SUM(guide_visit_count) AS total_guide
                  FROM dws_product_relation_stats
                  WHERE stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
                  GROUP BY main_product_id
                  ORDER BY total_guide DESC
                  LIMIT 10
              ) t
                  JOIN dws_product_behavior_sequence_stats s
                       ON t.main_product_id = s.main_product_id
                           AND s.stat_date = DATE_SUB(CURDATE(), INTERVAL 1 DAY)
     ) s
         LEFT JOIN ods_product_info p ON s.main_product_id = p.product_id
ORDER BY s.visit_to_cart_rate DESC;




select * from ads_product_sequence_analysis;
