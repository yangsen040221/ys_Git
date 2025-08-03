create database if not exists gd;

use gd;

# 1. 生成商品基础数据（ods_product_info）
CREATE TABLE `ods_product_info`
(
    `product_id`   INT          NOT NULL COMMENT '商品ID',
    `product_name` VARCHAR(200) NOT NULL COMMENT '商品名称',
    `category_id`  INT          NOT NULL COMMENT '品类ID',
    `create_time`  DATETIME     NOT NULL COMMENT '创建时间',
    `is_valid`     TINYINT  DEFAULT 1 COMMENT '是否有效（1-有效 0-无效）',
    `etl_time`     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL时间',
    PRIMARY KEY (`product_id`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '商品基础信息原始表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';


select * from ods_product_info;




# 2. 生成用户行为数据（ods_user_behavior）
CREATE TABLE `ods_user_behavior`
(
    `id`                   BIGINT   NOT NULL AUTO_INCREMENT COMMENT '记录ID',
    `user_id`              INT      NOT NULL COMMENT '用户ID',
    `main_product_id`      INT      NOT NULL COMMENT '主访问商品ID',
    `related_product_id`   INT COMMENT '关联商品ID（同时操作的商品）',
    `behavior_type`        TINYINT  NOT NULL COMMENT '行为类型（1-访问 2-收藏 3-加购 4-支付）',
    `behavior_time`        DATETIME NOT NULL COMMENT '行为时间',
    `is_guide_from_detail` TINYINT  DEFAULT 0 COMMENT '是否从详情页引导（1-是 0-否）',
    `etl_time`             DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL时间',
    PRIMARY KEY (`id`),
    KEY `idx_main_product` (`main_product_id`),
    KEY `idx_behavior_time` (`behavior_time`)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '用户商品行为原始表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';


select * from ods_user_behavior;








