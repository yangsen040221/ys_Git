create database if not exists sx_one_2_06;

use sx_one_2_06;


-- 商品基本信息表（ods_product_info）
CREATE TABLE ods_product_info
(
    product_id    STRING,   -- 商品ID
    product_name  STRING,   -- 商品名称
    product_image STRING,   -- 商品主图
    product_sku   STRING,   -- 商品货号
    category      STRING,   -- 商品类别
    release_date  STRING,   -- 商品上架日期
    update_time   TIMESTAMP -- 数据更新时间
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ods_product_info
SELECT
    concat('prod_', lpad(cast(rand()*5000 as int), 6, '0')) AS product_id,  -- 商品ID
    concat('Product ', cast(rand()*5000 as int)) AS product_name,             -- 商品名称
    concat('image_', cast(rand()*5000 as int), '.jpg') AS product_image,      -- 商品主图
    concat('sku_', lpad(cast(rand()*5000 as int), 6, '0')) AS product_sku,    -- 商品货号
    CASE WHEN rand() < 0.5 THEN 'Category A' ELSE 'Category B' END AS category,  -- 商品类别
    current_date() AS release_date,  -- 商品上架日期（假设为今天）
    current_timestamp() AS update_time  -- 数据更新时间
FROM (
         SELECT explode(split(space(5000), ' ')) AS dummy
     ) t;



-- 商品销售数据表（ods_product_sales）


CREATE TABLE ods_product_sales
(
    product_id   STRING,         -- 商品ID
    sales_amount DECIMAL(10, 2), -- 销售金额
    sales_count  INT,            -- 销售次数
    sales_date   STRING,         -- 销售日期
    order_id     STRING,         -- 订单ID
    update_time  TIMESTAMP       -- 数据更新时间
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE ods_product_sales
SELECT
    concat('prod_', lpad(cast(rand()*5000 as int), 6, '0')) AS product_id,  -- 商品ID
    round(rand()*5000, 2) AS sales_amount,  -- 销售金额
    cast(rand()*100 as int) AS sales_count,  -- 销售次数
    date_add(current_date(), cast(rand()*30 as int)) AS sales_date,  -- 销售日期（最近30天内）
    concat('order_', lpad(cast(rand()*5000 as int), 6, '0')) AS order_id,  -- 订单ID
    current_timestamp() AS update_time  -- 数据更新时间
FROM (
         SELECT explode(split(space(5000), ' ')) AS dummy
     ) t;



-- 新品监控数据表（ods_new_product_monitoring）


CREATE TABLE ods_new_product_monitoring
(
    product_id      STRING,         -- 商品ID
    product_name    STRING,         -- 商品名称
    sales_amount    DECIMAL(10, 2), -- 销售金额
    sales_count     INT,            -- 销售次数
    first_sale_date STRING,         -- 首次销售日期
    last_sale_date  STRING,         -- 最后销售日期
    update_time     TIMESTAMP       -- 数据更新时间
)
    STORED AS PARQUET;




INSERT OVERWRITE TABLE ods_new_product_monitoring
SELECT
    concat('prod_', lpad(cast(rand()*5000 as int), 6, '0')) AS product_id,  -- 商品ID
    concat('Product ', cast(rand()*5000 as int)) AS product_name,  -- 商品名称
    round(rand()*5000, 2) AS sales_amount,  -- 销售金额
    cast(rand()*100 as int) AS sales_count,  -- 销售次数
    date_add(current_date(), cast(rand()*30 as int)) AS first_sale_date,  -- 首次销售日期
    date_add(current_date(), cast(rand()*30 as int)) AS last_sale_date,  -- 最后销售日期
    current_timestamp() AS update_time  -- 数据更新时间
FROM (
         SELECT explode(split(space(5000), ' ')) AS dummy
     ) t;



-- 用户行为数据表（ods_user_behavior）

CREATE TABLE ods_user_behavior
(
    user_id       STRING,   -- 用户ID
    product_id    STRING,   -- 商品ID
    behavior_type STRING,   -- 用户行为类型（如浏览、购买等）
    action_time   TIMESTAMP -- 行为时间
)
    STORED AS PARQUET;



INSERT OVERWRITE TABLE ods_user_behavior
SELECT
    concat('user_', lpad(cast(rand()*1000 as int), 6, '0')) AS user_id,  -- 用户ID
    concat('prod_', lpad(cast(rand()*5000 as int), 6, '0')) AS product_id,  -- 商品ID
    CASE
        WHEN rand() < 0.33 THEN 'view'   -- 用户行为类型：浏览
        WHEN rand() < 0.66 THEN 'add_to_cart'  -- 用户行为类型：加入购物车
        ELSE 'purchase'  -- 用户行为类型：购买
        END AS behavior_type,
    date_add(current_date(), cast(rand()*30 as int)) AS action_time  -- 行为时间（最近30天内）
FROM (
         SELECT explode(split(space(5000), ' ')) AS dummy
     ) t;



-- 库存数据表（ods_product_inventory）


CREATE TABLE ods_product_inventory
(
    product_id  STRING,   -- 商品ID
    stock_count INT,      -- 库存数量
    status      STRING,   -- 商品状态（如：上架、下架）
    update_time TIMESTAMP -- 数据更新时间
)
    STORED AS PARQUET;


INSERT OVERWRITE TABLE ods_product_inventory
SELECT
    concat('prod_', lpad(cast(rand()*5000 as int), 6, '0')) AS product_id,  -- 商品ID
    cast(rand()*200 as int) AS stock_count,  -- 库存数量（随机生成）
    CASE
        WHEN rand() < 0.8 THEN 'available'  -- 商品状态：80%为"available"
        ELSE 'out_of_stock'  -- 商品状态：20%为"out_of_stock"
        END AS status,
    current_timestamp() AS update_time  -- 数据更新时间
FROM (
         SELECT explode(split(space(5000), ' ')) AS dummy
     ) t;

