create database if not exists sx_one_3_03;

use sx_one_3_03;

-- 启用动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

--------------------------------------------------------------------------------
-- 1. 订单明细表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_order_detail (
                                                order_id        STRING,
                                                product_id      STRING,
                                                sku_id          STRING,
                                                user_id         STRING,
                                                order_time      TIMESTAMP,
                                                order_status    STRING,
                                                quantity        INT,
                                                price           DECIMAL(10,2),
                                                order_amount    DECIMAL(10,2)
)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ods_order_detail PARTITION (dt)
SELECT
    order_id,
    product_id,
    sku_id,
    user_id,
    order_time,
    order_status,
    quantity,
    price,
    cast(price * quantity as decimal(10,2)) AS order_amount,
    date_format(order_time, 'yyyy-MM-dd') AS dt
FROM (
         SELECT
             concat('O', lpad(cast(rand()*100000000 as int), 8, '0')) AS order_id,
             concat('P', lpad(cast(rand()*1000 as int), 5, '0')) AS product_id,
             concat('S', lpad(cast(rand()*10000 as int), 6, '0')) AS sku_id,
             concat('U', lpad(cast(rand()*5000 as int), 6, '0')) AS user_id,
             from_unixtime(unix_timestamp() - cast(rand()*30*24*3600 as int), 'yyyy-MM-dd HH:mm:ss') AS order_time,
             CASE WHEN rand() < 0.9 THEN 'completed' ELSE 'cancelled' END AS order_status,
             cast(rand()*5+1 as int) AS quantity,
             cast(rand()*200+10 as decimal(10,2)) AS price
         FROM (
                  SELECT explode(split(space(50000), ' ')) AS dummy
              ) t
     ) tmp;


select
    *
from ods_order_detail;

--------------------------------------------------------------------------------
-- 2. 商品信息表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_product_info (
                                                product_id      STRING,
                                                product_name    STRING,
                                                category_id     STRING,
                                                category_name   STRING,
                                                brand_id        STRING,
                                                brand_name      STRING
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ods_product_info
SELECT
    concat('P', lpad(cast(rand()*1000 as int), 5, '0')) AS product_id,
    concat('商品', cast(rand()*1000 as int)) AS product_name,
    concat('C', lpad(cast(rand()*50 as int), 4, '0')) AS category_id,
    concat('类目', cast(rand()*50 as int)) AS category_name,
    concat('B', lpad(cast(rand()*30 as int), 3, '0')) AS brand_id,
    concat('品牌', cast(rand()*30 as int)) AS brand_name
FROM (
         SELECT explode(split(space(1000), ' ')) AS dummy
     ) t;


select * from ods_product_info;

--------------------------------------------------------------------------------
-- 3. 流量日志表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_traffic_log (
                                               log_id          STRING,
                                               user_id         STRING,
                                               product_id      STRING,
                                               channel         STRING,
                                               visit_time      TIMESTAMP
)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ods_traffic_log PARTITION (dt)
SELECT
    log_id,
    user_id,
    product_id,
    channel,
    visit_time,
    date_format(visit_time, 'yyyy-MM-dd') AS dt
FROM (
         SELECT
             concat('L', lpad(cast(rand()*1000000 as int), 8, '0')) AS log_id,
             concat('U', lpad(cast(rand()*5000 as int), 6, '0')) AS user_id,
             concat('P', lpad(cast(rand()*1000 as int), 5, '0')) AS product_id,
             CASE WHEN rand() < 0.25 THEN '搜索'
                  WHEN rand() < 0.5 THEN '推荐'
                  WHEN rand() < 0.75 THEN '直通车'
                  ELSE '活动' END AS channel,
             from_unixtime(unix_timestamp() - cast(rand()*30*24*3600 as int), 'yyyy-MM-dd HH:mm:ss') AS visit_time
         FROM (
                  SELECT explode(split(space(20000), ' ')) AS dummy
              ) t
     ) tmp;



select * from ods_traffic_log;

--------------------------------------------------------------------------------
-- 4. 价格变动表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_price_history (
                                                 product_id      STRING,
                                                 change_time     TIMESTAMP,
                                                 old_price       DECIMAL(10,2),
                                                 new_price       DECIMAL(10,2)
)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ods_price_history PARTITION (dt)
SELECT
    product_id,
    change_time,
    old_price,
    new_price,
    date_format(change_time, 'yyyy-MM-dd') AS dt
FROM (
         SELECT
             concat('P', lpad(cast(rand()*1000 as int), 5, '0')) AS product_id,
             from_unixtime(unix_timestamp() - cast(rand()*30*24*3600 as int), 'yyyy-MM-dd HH:mm:ss') AS change_time,
             cast(rand()*200+10 as decimal(10,2)) AS old_price,
             cast(rand()*200+10 as decimal(10,2)) AS new_price
         FROM (
                  SELECT explode(split(space(3000), ' ')) AS dummy
              ) t
     ) tmp;


select * from ods_price_history;

--------------------------------------------------------------------------------
-- 5. 评价表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_review (
                                          review_id       STRING,
                                          product_id      STRING,
                                          sku_id          STRING,
                                          user_id         STRING,
                                          rating          INT,
                                          review_content  STRING,
                                          review_time     TIMESTAMP
)
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ods_review PARTITION (dt)
SELECT
    review_id,
    product_id,
    sku_id,
    user_id,
    rating,
    review_content,
    review_time,
    date_format(review_time, 'yyyy-MM-dd') AS dt
FROM (
         SELECT
             concat('R', lpad(cast(rand()*1000000 as int), 8, '0')) AS review_id,
             concat('P', lpad(cast(rand()*1000 as int), 5, '0')) AS product_id,
             concat('S', lpad(cast(rand()*10000 as int), 6, '0')) AS sku_id,
             concat('U', lpad(cast(rand()*5000 as int), 6, '0')) AS user_id,
             cast(rand()*5+1 as int) AS rating,
             concat('评价内容', cast(rand()*1000 as int)) AS review_content,
             from_unixtime(unix_timestamp() - cast(rand()*30*24*3600 as int), 'yyyy-MM-dd HH:mm:ss') AS review_time
         FROM (
                  SELECT explode(split(space(8000), ' ')) AS dummy
              ) t
     ) tmp;

select * from ods_review;


--------------------------------------------------------------------------------
-- 6. 用户画像表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ods_user_profile (
                                                user_id         STRING,
                                                gender          STRING,
                                                age             INT,
                                                location        STRING,
                                                member_level    STRING
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE ods_user_profile
SELECT
    concat('U', lpad(cast(rand()*5000 as int), 6, '0')) AS user_id,
    CASE WHEN rand() < 0.5 THEN '男' ELSE '女' END AS gender,
    cast(rand()*40+18 as int) AS age,
    concat('城市', cast(rand()*100 as int)) AS location,
    concat('V', cast(rand()*5+1 as int)) AS member_level
FROM (
         SELECT explode(split(space(5000), ' ')) AS dummy
     ) t;



select * from ods_user_profile;
