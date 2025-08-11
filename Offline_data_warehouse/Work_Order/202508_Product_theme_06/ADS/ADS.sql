use sx_one_2_06;

-- 设置Hive执行参数以避免执行错误
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=10000;
set hive.exec.max.dynamic.partitions.pernode=1000;
set hive.exec.max.created.files=100000;
set hive.error.on.empty.partition=false;
set hive.mapred.mode=nonstrict;
set hive.optimize.ppd=true;
set hive.optimize.ppd.storage=true;

-- 商品信息分析表（ads_product_info）
DROP TABLE IF EXISTS ads_product_info;

CREATE TABLE IF NOT EXISTS ads_product_info
(
    product_id        STRING,         -- 商品ID
    product_name      STRING,         -- 商品名称
    product_image     STRING,         -- 商品主图
    product_sku       STRING,         -- 商品货号
    category          STRING,         -- 商品类别
    total_sales       DECIMAL(10, 2), -- 累计销售金额
    total_sales_count INT,            -- 累计销售次数
    sales_category    STRING,         -- 销售等级（High/Medium/Low）
    first_sale_date   STRING,         -- 首次销售日期
    last_sale_date    STRING,         -- 最后销售日期
    stock_status      STRING          -- 库存状态（可用/缺货）
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE ads_product_info
SELECT p.product_id,                                                     -- 商品ID
       p.product_name,                                                   -- 商品名称
       p.product_image,                                                  -- 商品主图
       p.product_sku,                                                    -- 商品货号
       p.category,                                                       -- 商品类别
       COALESCE(s.total_sales, 0)                  AS total_sales,       -- 累计销售金额
       COALESCE(s.total_sales_count, 0)            AS total_sales_count, -- 累计销售次数
       p.sales_category,                                                 -- 销售等级
       p.first_sale_date,                                                -- 首次销售日期
       p.last_sale_date,                                                 -- 最后销售日期
       i.stock_status                                                    -- 库存状态
FROM dws_product_info p
         LEFT JOIN dws_product_sales s ON p.product_id = s.product_id
         LEFT JOIN (
             SELECT product_id, 
                    MAX(stock_status) AS stock_status 
             FROM dws_product_inventory 
             GROUP BY product_id
         ) i ON p.product_id = i.product_id;


-- 商品销售分析表（ads_product_sales）
DROP TABLE IF EXISTS ads_product_sales;

CREATE TABLE IF NOT EXISTS ads_product_sales
(
    product_id        STRING,         -- 商品ID
    total_sales       DECIMAL(10, 2), -- 累计销售金额
    total_sales_count INT,            -- 累计销售次数
    total_orders      INT,            -- 唯一订单数
    sales_trend       STRING          -- 销售趋势（上升/下降/稳定）
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE ads_product_sales
SELECT product_id,            -- 商品ID
       total_sales,           -- 累计销售金额
       total_sales_count,     -- 累计销售次数
       total_orders,          -- 唯一订单数
       'Stable' AS sales_trend -- 销售趋势（由于没有时间序列数据，默认为稳定）
FROM dws_product_sales;


-- 用户行为分析表（ads_user_behavior）
DROP TABLE IF EXISTS ads_user_behavior;

CREATE TABLE IF NOT EXISTS ads_user_behavior
(
    user_id              STRING,    -- 用户ID
    product_id           STRING,    -- 商品ID
    total_behavior_count INT,       -- 用户对商品的行为总次数
    first_action_time    TIMESTAMP, -- 用户首次行为时间
    last_action_time     TIMESTAMP, -- 用户最后行为时间
    behavior_type        STRING     -- 用户行为类型（浏览、加入购物车、购买）
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_user_behavior
SELECT user_id,              -- 用户ID
       product_id,           -- 商品ID
       total_behavior_count, -- 用户对商品的行为总次数
       first_action_time,    -- 用户首次行为时间
       last_action_time,     -- 用户最后行为时间
       behavior_type         -- 用户行为类型（浏览、加入购物车、购买）
FROM dws_user_behavior;


-- 商品库存分析表（ads_product_inventory）
DROP TABLE IF EXISTS ads_product_inventory;

CREATE TABLE IF NOT EXISTS ads_product_inventory
(
    product_id   STRING, -- 商品ID
    total_stock  INT,    -- 总库存数量
    stock_status STRING  -- 商品库存状态（可用/缺货）
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE ads_product_inventory
SELECT product_id,   -- 商品ID
       total_stock,  -- 总库存数量
       stock_status  -- 商品库存状态
FROM dws_product_inventory;