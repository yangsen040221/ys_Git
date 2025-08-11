use sx_one_3_03;

-- 开启动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

--------------------------------------------------------------------------------
-- 1. 订单事实表（dwd_fact_order）
-- 清洗规则：
--   ① 只保留订单状态为 completed
--   ② 去重（取最新 order_time 记录）
--   ③ 补充商品维度字段
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_fact_order (
                                              order_id        STRING,
                                              product_id      STRING,
                                              product_name    STRING,
                                              category_id     STRING,
                                              category_name   STRING,
                                              brand_id        STRING,
                                              brand_name      STRING,
                                              sku_id          STRING,
                                              user_id         STRING,
                                              order_time      TIMESTAMP,
                                              quantity        INT,
                                              price           DECIMAL(10,2),
    order_amount    DECIMAL(10,2)
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dwd_fact_order PARTITION (dt)
SELECT
    a.order_id,
    a.product_id,
    b.product_name,
    b.category_id,
    b.category_name,
    b.brand_id,
    b.brand_name,
    a.sku_id,
    a.user_id,
    a.order_time,
    a.quantity,
    a.price,
    a.order_amount,
    a.dt
FROM (
         SELECT *
         FROM (
                  SELECT *,
                         row_number() OVER (PARTITION BY order_id ORDER BY order_time DESC) AS rn
                  FROM ods_order_detail
                  WHERE order_status = 'completed'
              ) t
         WHERE rn = 1
     ) a
         LEFT JOIN ods_product_info b
                   ON a.product_id = b.product_id;

--------------------------------------------------------------------------------
-- 2. 流量事实表（dwd_fact_traffic）
-- 清洗规则：
--   ① 去重（log_id 最新）
--   ② 补充商品维度字段
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_fact_traffic (
                                                log_id          STRING,
                                                user_id         STRING,
                                                product_id      STRING,
                                                product_name    STRING,
                                                category_id     STRING,
                                                category_name   STRING,
                                                channel         STRING,
                                                visit_time      TIMESTAMP
)
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dwd_fact_traffic PARTITION (dt)
SELECT
    a.log_id,
    a.user_id,
    a.product_id,
    b.product_name,
    b.category_id,
    b.category_name,
    a.channel,
    a.visit_time,
    a.dt
FROM (
         SELECT *
         FROM (
                  SELECT *,
                         row_number() OVER (PARTITION BY log_id ORDER BY visit_time DESC) AS rn
                  FROM ods_traffic_log
              ) t
         WHERE rn = 1
     ) a
         LEFT JOIN ods_product_info b
                   ON a.product_id = b.product_id;

--------------------------------------------------------------------------------
-- 3. 价格变化事实表（dwd_fact_price）
-- 清洗规则：
--   ① 去重
--   ② old_price / new_price 必须大于0
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_fact_price (
                                              product_id      STRING,
                                              product_name    STRING,
                                              category_id     STRING,
                                              category_name   STRING,
                                              change_time     TIMESTAMP,
                                              old_price       DECIMAL(10,2),
    new_price       DECIMAL(10,2)
    )
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dwd_fact_price PARTITION (dt)
SELECT
    a.product_id,
    b.product_name,
    b.category_id,
    b.category_name,
    a.change_time,
    a.old_price,
    a.new_price,
    a.dt
FROM (
         SELECT *
         FROM (
                  SELECT *,
                         row_number() OVER (PARTITION BY product_id, change_time ORDER BY change_time DESC) AS rn
                  FROM ods_price_history
                  WHERE old_price > 0 AND new_price > 0
              ) t
         WHERE rn = 1
     ) a
         LEFT JOIN ods_product_info b
                   ON a.product_id = b.product_id;

--------------------------------------------------------------------------------
-- 4. 评价事实表（dwd_fact_review）
-- 清洗规则：
--   ① 去重（review_id 最新）
--   ② 评分必须在 1-5 之间
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_fact_review (
                                               review_id       STRING,
                                               product_id      STRING,
                                               product_name    STRING,
                                               sku_id          STRING,
                                               user_id         STRING,
                                               rating          INT,
                                               review_content  STRING,
                                               review_time     TIMESTAMP
)
    PARTITIONED BY (dt STRING)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dwd_fact_review PARTITION (dt)
SELECT
    a.review_id,
    a.product_id,
    b.product_name,
    a.sku_id,
    a.user_id,
    a.rating,
    a.review_content,
    a.review_time,
    a.dt
FROM (
         SELECT *
         FROM (
                  SELECT *,
                         row_number() OVER (PARTITION BY review_id ORDER BY review_time DESC) AS rn
                  FROM ods_review
                  WHERE rating BETWEEN 1 AND 5
              ) t
         WHERE rn = 1
     ) a
         LEFT JOIN ods_product_info b
                   ON a.product_id = b.product_id;

--------------------------------------------------------------------------------
-- 5. 用户维度表（dwd_dim_user）
-- 清洗规则：
--   ① 去重
--   ② 年龄范围必须合理（18-100）
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_dim_user (
                                            user_id         STRING,
                                            gender          STRING,
                                            age             INT,
                                            location        STRING,
                                            member_level    STRING
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dwd_dim_user
SELECT
    user_id,
    gender,
    age,
    location,
    member_level
FROM (
         SELECT
             user_id,
             gender,
             age,
             location,
             member_level,
             row_number() OVER (PARTITION BY user_id ORDER BY age DESC) AS rn
         FROM ods_user_profile
         WHERE age BETWEEN 18 AND 100
     ) t
WHERE rn = 1;


--------------------------------------------------------------------------------
-- 6. 商品维度表（dwd_dim_product）
-- 直接去重即可
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dwd_dim_product (
                                               product_id      STRING,
                                               product_name    STRING,
                                               category_id     STRING,
                                               category_name   STRING,
                                               brand_id        STRING,
                                               brand_name      STRING
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dwd_dim_product
SELECT
    product_id,
    product_name,
    category_id,
    category_name,
    brand_id,
    brand_name
FROM (
         SELECT
             product_id,
             product_name,
             category_id,
             category_name,
             brand_id,
             brand_name,
             row_number() OVER (PARTITION BY product_id ORDER BY product_name) AS rn
         FROM ods_product_info
     ) t
WHERE rn = 1;

