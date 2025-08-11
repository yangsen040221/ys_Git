use sx_one_2_06;


-- DWD 层：商品信息汇总表
CREATE TABLE IF NOT EXISTS dwd_product_info AS
SELECT
    product_id,            -- 商品ID
    product_name,          -- 商品名称
    product_image,         -- 商品主图
    product_sku,           -- 商品货号
    category,              -- 商品类别
    release_date,          -- 商品上架日期
    update_time            -- 数据更新时间
FROM ods_product_info
WHERE product_id IS NOT NULL  -- 过滤掉无效商品数据
GROUP BY product_id, product_name, product_image, product_sku, category, release_date, update_time;


-- 插入数据到 dwd_product_info
INSERT INTO TABLE dwd_product_info
SELECT
    product_id,              -- 商品ID
    product_name,            -- 商品名称
    product_image,           -- 商品主图
    product_sku,             -- 商品货号
    category,                -- 商品类别
    release_date,            -- 商品上架日期
    update_time              -- 数据更新时间
FROM ods_product_info
WHERE product_id IS NOT NULL  -- 过滤掉无效商品数据
GROUP BY product_id, product_name, product_image, product_sku, category, release_date, update_time;



-- DWD 层：商品销售数据汇总表
CREATE TABLE IF NOT EXISTS dwd_product_sales AS
SELECT
    product_id,                 -- 商品ID
    SUM(sales_amount) AS total_sales,  -- 累计销售金额
    SUM(sales_count) AS total_sales_count,  -- 累计销售次数
    sales_date,                 -- 销售日期
    COUNT(DISTINCT order_id) AS total_orders,  -- 唯一订单数
    MAX(update_time) AS last_update_time  -- 最新更新时间
FROM ods_product_sales
WHERE product_id IS NOT NULL  -- 过滤无效销售数据
GROUP BY product_id, sales_date;


-- 插入数据到 dwd_product_sales
INSERT INTO TABLE dwd_product_sales
SELECT
    product_id,                           -- 商品ID
    SUM(sales_amount) AS total_sales,      -- 累计销售金额
    SUM(sales_count) AS total_sales_count, -- 累计销售次数
    sales_date,                            -- 销售日期
    COUNT(DISTINCT order_id) AS total_orders,  -- 唯一订单数
    MAX(update_time) AS last_update_time   -- 最新更新时间
FROM ods_product_sales
WHERE product_id IS NOT NULL  -- 过滤掉无效销售数据
GROUP BY product_id, sales_date;




-- DWD 层：新品监控数据汇总表
CREATE TABLE IF NOT EXISTS dwd_new_product_monitoring AS
SELECT
    product_id,                      -- 商品ID
    product_name,                    -- 商品名称
    SUM(sales_amount) AS total_sales, -- 累计销售金额
    SUM(sales_count) AS total_sales_count,  -- 累计销售次数
    MIN(first_sale_date) AS first_sale_date,  -- 首次销售日期
    MAX(last_sale_date) AS last_sale_date,   -- 最后销售日期
    MAX(update_time) AS last_update_time   -- 最后更新时间
FROM ods_new_product_monitoring
WHERE product_id IS NOT NULL  -- 过滤无效商品
GROUP BY product_id, product_name;


-- 插入数据到 dwd_new_product_monitoring
INSERT INTO TABLE dwd_new_product_monitoring
SELECT
    product_id,                          -- 商品ID
    product_name,                        -- 商品名称
    SUM(sales_amount) AS total_sales,    -- 累计销售金额
    SUM(sales_count) AS total_sales_count,  -- 累计销售次数
    MIN(first_sale_date) AS first_sale_date,  -- 首次销售日期
    MAX(last_sale_date) AS last_sale_date,    -- 最后销售日期
    MAX(update_time) AS last_update_time  -- 最后更新时间
FROM ods_new_product_monitoring
WHERE product_id IS NOT NULL  -- 过滤无效商品
GROUP BY product_id, product_name;




-- DWD 层：用户行为数据汇总表
CREATE TABLE IF NOT EXISTS dwd_user_behavior AS
SELECT
    user_id,                          -- 用户ID
    product_id,                       -- 商品ID
    behavior_type,                    -- 用户行为类型（浏览、加入购物车、购买）
    COUNT(*) AS behavior_count,        -- 用户对商品的行为次数
    MIN(action_time) AS first_action_time,  -- 用户首次行为时间
    MAX(action_time) AS last_action_time   -- 用户最后行为时间
FROM ods_user_behavior
WHERE user_id IS NOT NULL   -- 过滤无效用户数据
GROUP BY user_id, product_id, behavior_type;




-- 插入数据到 dwd_user_behavior
INSERT INTO TABLE dwd_user_behavior
SELECT
    user_id,                            -- 用户ID
    product_id,                         -- 商品ID
    behavior_type,                      -- 用户行为类型（浏览、加入购物车、购买）
    COUNT(*) AS behavior_count,          -- 用户对商品的行为次数
    MIN(action_time) AS first_action_time,  -- 用户首次行为时间
    MAX(action_time) AS last_action_time   -- 用户最后行为时间
FROM ods_user_behavior
WHERE user_id IS NOT NULL   -- 过滤无效用户数据
GROUP BY user_id, product_id, behavior_type;


-- DWD 层：商品库存数据汇总表
CREATE TABLE IF NOT EXISTS dwd_product_inventory AS
SELECT
    product_id,                          -- 商品ID
    SUM(stock_count) AS total_stock,      -- 总库存数量
    CASE
        WHEN SUM(stock_count) > 0 THEN 'available'   -- 如果库存大于0，商品状态为“可用”
        ELSE 'out_of_stock'                   -- 否则为“缺货”
        END AS stock_status,                   -- 商品库存状态
    MAX(update_time) AS last_update_time   -- 最后更新时间
FROM ods_product_inventory
WHERE product_id IS NOT NULL   -- 过滤无效库存数据
GROUP BY product_id;



-- 插入数据到 dwd_product_inventory
INSERT INTO TABLE dwd_product_inventory
SELECT
    product_id,                            -- 商品ID
    SUM(stock_count) AS total_stock,        -- 总库存数量
    CASE
        WHEN SUM(stock_count) > 0 THEN 'available'  -- 如果库存大于0，商品状态为“可用”
        ELSE 'out_of_stock'                   -- 否则为“缺货”
        END AS stock_status,                    -- 商品库存状态
    MAX(update_time) AS last_update_time    -- 最后更新时间
FROM ods_product_inventory
WHERE product_id IS NOT NULL  -- 过滤无效库存数据
GROUP BY product_id;

