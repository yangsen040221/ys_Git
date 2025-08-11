use sx_one_2_06;

-- 商品信息汇总表（dws_product_info）
DROP TABLE IF EXISTS dws_product_info;

CREATE TABLE IF NOT EXISTS dws_product_info
(
    product_id        STRING,         -- 商品ID
    product_name      STRING,         -- 商品名称
    product_image     STRING,         -- 商品主图
    product_sku       STRING,         -- 商品货号
    category          STRING,         -- 商品类别
    release_date      STRING,         -- 商品上架日期
    update_time       TIMESTAMP,      -- 数据更新时间
    sales_category    STRING,         -- 商品销售等级
    total_sales       DECIMAL(10, 2), -- 累计销售金额
    total_sales_count INT,            -- 累计销售次数
    first_sale_date   STRING,         -- 首次销售日期
    last_sale_date    STRING          -- 最后销售日期
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_info
SELECT p.product_id,                                                   -- 商品ID
       p.product_name,                                                 -- 商品名称
       p.product_image,                                                -- 商品主图
       p.product_sku,                                                  -- 商品货号
       p.category,                                                     -- 商品类别
       p.release_date,                                                 -- 商品上架日期
       p.update_time,                                                  -- 数据更新时间
       CASE
           WHEN s.total_sales > 1000 THEN 'High'
           WHEN s.total_sales BETWEEN 500 AND 1000 THEN 'Medium'
           ELSE 'Low'
           END                                     AS sales_category,  -- 商品销售等级
       COALESCE(s.total_sales, 0),                                     -- 累计销售金额
       COALESCE(s.total_sales_count, 0),                               -- 累计销售次数
       COALESCE(s.first_sale_date, p.release_date) AS first_sale_date, -- 首次销售日期
       COALESCE(s.last_sale_date, p.release_date)  AS last_sale_date   -- 最后销售日期
FROM dwd_product_info p
         LEFT JOIN (
            SELECT product_id,                                          -- 商品ID
                   SUM(total_sales) AS total_sales,                     -- 累计销售金额
                   SUM(total_sales_count) AS total_sales_count,         -- 累计销售次数
                   MIN(sales_date) AS first_sale_date,                  -- 首次销售日期
                   MAX(sales_date) AS last_sale_date                    -- 最后销售日期
            FROM dwd_product_sales 
            GROUP BY product_id
         ) s ON p.product_id = s.product_id;


-- 商品销售汇总表（dws_product_sales）
DROP TABLE IF EXISTS dws_product_sales;

CREATE TABLE IF NOT EXISTS dws_product_sales
(
    product_id        STRING,         -- 商品ID
    total_sales       DECIMAL(10, 2), -- 累计销售金额
    total_sales_count INT,            -- 累计销售次数
    total_orders      INT,            -- 唯一订单数
    last_update_time  TIMESTAMP       -- 最新更新时间
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE dws_product_sales
SELECT product_id,                                    -- 商品ID
       SUM(total_sales)         AS total_sales,       -- 累计销售金额
       SUM(total_sales_count)   AS total_sales_count, -- 累计销售次数
       SUM(total_orders)        AS total_orders,      -- 唯一订单数
       MAX(last_update_time)    AS last_update_time   -- 最新更新时间
FROM dwd_product_sales
GROUP BY product_id;


-- 新品监控汇总表（dws_new_product_monitoring）
DROP TABLE IF EXISTS dws_new_product_monitoring;

CREATE TABLE IF NOT EXISTS dws_new_product_monitoring
(
    product_id        STRING,         -- 商品ID
    product_name      STRING,         -- 商品名称
    total_sales       DECIMAL(10, 2), -- 累计销售金额
    total_sales_count INT,            -- 累计销售次数
    first_sale_date   STRING,         -- 首次销售日期
    last_sale_date    STRING          -- 最后销售日期
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE dws_new_product_monitoring
SELECT product_id,                                  -- 商品ID
       product_name,                                -- 商品名称
       SUM(total_sales)       AS total_sales,       -- 累计销售金额
       SUM(total_sales_count) AS total_sales_count, -- 累计销售次数
       MIN(first_sale_date)   AS first_sale_date,   -- 首次销售日期
       MAX(last_sale_date)    AS last_sale_date     -- 最后销售日期
FROM dwd_new_product_monitoring
GROUP BY product_id, product_name;


-- 用户行为汇总表（dws_user_behavior）
DROP TABLE IF EXISTS dws_user_behavior;

CREATE TABLE IF NOT EXISTS dws_user_behavior
(
    user_id              STRING,    -- 用户ID
    product_id           STRING,    -- 商品ID
    behavior_type        STRING,    -- 用户行为类型（浏览、加入购物车、购买）
    total_behavior_count INT,       -- 用户对商品的行为总次数
    first_action_time    TIMESTAMP, -- 用户首次行为时间
    last_action_time     TIMESTAMP  -- 用户最后行为时间
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_user_behavior
SELECT user_id,                                        -- 用户ID
       product_id,                                     -- 商品ID
       behavior_type,                                  -- 用户行为类型（浏览、加入购物车、购买）
       SUM(behavior_count)    AS total_behavior_count, -- 用户对商品的行为总次数
       MIN(first_action_time) AS first_action_time,    -- 用户首次行为时间
       MAX(last_action_time)  AS last_action_time      -- 用户最后行为时间
FROM dwd_user_behavior
GROUP BY user_id, product_id, behavior_type;

-- 商品库存汇总表（dws_product_inventory）
DROP TABLE IF EXISTS dws_product_inventory;

CREATE TABLE IF NOT EXISTS dws_product_inventory
(
    product_id       STRING,   -- 商品ID
    total_stock      INT,      -- 累计库存数量
    stock_status     STRING,   -- 商品库存状态（如：可用、缺货）
    last_update_time TIMESTAMP -- 最后更新时间
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE dws_product_inventory
SELECT product_id,                               -- 商品ID
       SUM(total_stock)      AS total_stock,     -- 累计库存数量
       MAX(stock_status)     AS stock_status,    -- 商品库存状态
       MAX(last_update_time) AS last_update_time -- 最后更新时间
FROM dwd_product_inventory
GROUP BY product_id;