use sx_one_3_03;


-- 启用动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.exec.mode.local.auto=false;
SET mapreduce.map.memory.mb=2048;
SET mapreduce.reduce.memory.mb=2048;
SET hive.auto.convert.join=false; -- 防止 MapJoin 爆内存

--------------------------------------------------------------------------------
-- 1. 商品日汇总表
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_product_day_summary (
                                                       dt              STRING,
                                                       product_id      STRING,
                                                       product_name    STRING,
                                                       category_id     STRING,
                                                       category_name   STRING,
                                                       sales_qty       BIGINT,
                                                       gmv             DECIMAL(18,2),
                                                       uv              BIGINT,
                                                       pv              BIGINT,
                                                       conversion_rate DECIMAL(10,4)
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_day_summary
SELECT
    o.dt,
    o.product_id,
    o.product_name,
    o.category_id,
    o.category_name,
    SUM(o.quantity) AS sales_qty,
    SUM(o.order_amount) AS gmv,
    COUNT(DISTINCT o.user_id) AS uv,
    COUNT(t.user_id) AS pv,
    CASE WHEN COUNT(DISTINCT o.user_id) > 0
             THEN ROUND(SUM(o.quantity) / COUNT(DISTINCT o.user_id), 4)
         ELSE 0 END AS conversion_rate
FROM dwd_fact_order o
         LEFT JOIN dwd_fact_traffic t
                   ON o.product_id = t.product_id
                       AND o.dt = t.dt
GROUP BY o.dt, o.product_id, o.product_name, o.category_id, o.category_name;

--------------------------------------------------------------------------------
-- 2. SKU+属性热销分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_product_sku_attr_summary (
                                                            dt              STRING,
                                                            product_id      STRING,
                                                            sku_id          STRING,
                                                            sales_qty       BIGINT,
                                                            gmv             DECIMAL(18,2)
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_sku_attr_summary
SELECT
    dt,
    product_id,
    sku_id,
    SUM(quantity) AS sales_qty,
    SUM(order_amount) AS gmv
FROM dwd_fact_order
GROUP BY dt, product_id, sku_id;

--------------------------------------------------------------------------------
-- 3. 价格趋势分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_product_price_summary (
                                                         dt              STRING,
                                                         product_id      STRING,
                                                         product_name    STRING,
                                                         category_id     STRING,
                                                         category_name   STRING,
                                                         avg_price       DECIMAL(10,2),
                                                         min_price       DECIMAL(10,2),
                                                         max_price       DECIMAL(10,2)
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_price_summary
SELECT
    dt,
    product_id,
    product_name,
    category_id,
    category_name,
    ROUND(AVG(new_price),2) AS avg_price,
    MIN(new_price) AS min_price,
    MAX(new_price) AS max_price
FROM dwd_fact_price
GROUP BY dt, product_id, product_name, category_id, category_name;

--------------------------------------------------------------------------------
-- 4. 流量来源分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_product_channel_summary (
                                                           dt              STRING,
                                                           product_id      STRING,
                                                           product_name    STRING,
                                                           category_id     STRING,
                                                           category_name   STRING,
                                                           channel         STRING,
                                                           uv              BIGINT,
                                                           pv              BIGINT,
                                                           channel_conv_rate DECIMAL(10,4)
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_channel_summary
SELECT
    t.dt,
    t.product_id,
    t.product_name,
    t.category_id,
    t.category_name,
    t.channel,
    COUNT(DISTINCT t.user_id) AS uv,
    COUNT(t.user_id) AS pv,
    CASE WHEN COUNT(DISTINCT t.user_id) > 0
             THEN ROUND(SUM(CASE WHEN o.user_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(DISTINCT t.user_id), 4)
         ELSE 0 END AS channel_conv_rate
FROM dwd_fact_traffic t
         LEFT JOIN dwd_fact_order o
                   ON t.product_id = o.product_id
                       AND t.user_id = o.user_id
                       AND t.dt = o.dt
GROUP BY t.dt, t.product_id, t.product_name, t.category_id, t.category_name, t.channel;

--------------------------------------------------------------------------------
-- 5. 评价趋势分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_product_review_summary (
                                                          dt              STRING,
                                                          product_id      STRING,
                                                          product_name    STRING,
                                                          total_reviews   BIGINT,
                                                          positive_reviews BIGINT,
                                                          negative_reviews BIGINT,
                                                          avg_rating      DECIMAL(10,2),
                                                          positive_rate   DECIMAL(10,4)
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_review_summary
SELECT
    dt,
    product_id,
    product_name,
    COUNT(*) AS total_reviews,
    SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) AS positive_reviews,
    SUM(CASE WHEN rating <= 2 THEN 1 ELSE 0 END) AS negative_reviews,
    ROUND(AVG(rating),2) AS avg_rating,
    CASE WHEN COUNT(*) > 0
             THEN ROUND(SUM(CASE WHEN rating >= 4 THEN 1 ELSE 0 END) / COUNT(*), 4)
         ELSE 0 END AS positive_rate
FROM dwd_fact_review
GROUP BY dt, product_id, product_name;

--------------------------------------------------------------------------------
-- 6. 用户画像分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dws_product_audience_summary (
                                                            dt              STRING,
                                                            product_id      STRING,
                                                            product_name    STRING,
                                                            gender          STRING,
                                                            age_group       STRING,
                                                            location        STRING,
                                                            uv              BIGINT
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_audience_summary
SELECT
    o.dt,
    o.product_id,
    o.product_name,
    u.gender,
    CASE
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age < 35 THEN '25-34'
        WHEN u.age < 45 THEN '35-44'
        WHEN u.age < 55 THEN '45-54'
        ELSE '55+' END AS age_group,
    u.location,
    COUNT(DISTINCT o.user_id) AS uv
FROM dwd_fact_order o
         LEFT JOIN dwd_dim_user u
                   ON o.user_id = u.user_id
GROUP BY o.dt, o.product_id, o.product_name, u.gender,
         CASE
             WHEN u.age < 25 THEN '18-24'
             WHEN u.age < 35 THEN '25-34'
             WHEN u.age < 45 THEN '35-44'
             WHEN u.age < 55 THEN '45-54'
             ELSE '55+' END,
         u.location;
