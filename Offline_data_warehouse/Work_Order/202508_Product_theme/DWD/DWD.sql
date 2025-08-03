use gd;

CREATE TABLE `dwd_user_behavior_detail`
(
    `user_id`              INT          NOT NULL COMMENT '用户ID',
    `main_product_id`      INT          NOT NULL COMMENT '主访问商品ID',
    `main_product_name`    VARCHAR(200) NOT NULL COMMENT '主商品名称',
    `related_product_id`   INT COMMENT '关联商品ID',
    `related_product_name` VARCHAR(200) COMMENT '关联商品名称',
    `behavior_type`        TINYINT      NOT NULL COMMENT '行为类型（1-访问 2-收藏 3-加购 4-支付）',
    `behavior_time`        DATETIME     NOT NULL COMMENT '行为时间',
    `is_guide_from_detail` TINYINT DEFAULT 0 COMMENT '是否从详情页引导',
    `dt`                   DATE         NOT NULL COMMENT '日期分区',
    PRIMARY KEY (`user_id`, `behavior_time`, `main_product_id`),
    KEY `idx_dt` (`dt`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '清洗后的用户行为明细，关联商品名称，用于后续汇总分析（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';








-- 同步近7天数据
INSERT INTO `dwd_user_behavior_detail`
SELECT
    u.user_id,
    u.main_product_id,
    p1.product_name AS main_product_name,
    u.related_product_id,
    p2.product_name AS related_product_name,
    u.behavior_type,
    u.behavior_time,
    u.is_guide_from_detail,
    DATE(u.behavior_time) AS dt
FROM
    ods_user_behavior u
        LEFT JOIN
    ods_product_info p1 ON u.main_product_id = p1.product_id
        LEFT JOIN
    ods_product_info p2 ON u.related_product_id = p2.product_id
WHERE
        u.behavior_time >= DATE_SUB(NOW(), INTERVAL 7 DAY)
  AND u.main_product_id IS NOT NULL;


select * from dwd_user_behavior_detail;