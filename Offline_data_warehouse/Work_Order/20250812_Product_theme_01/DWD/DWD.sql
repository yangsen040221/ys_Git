USE sx_one_3_01;

-- ==============================================================
-- 1. DWD 商品信息表
-- 基于ODS层商品信息表创建DWD层商品信息表
-- 包含商品的基本信息：ID、名称、分类、价格和创建时间
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_info;
CREATE TABLE dwd_product_info
AS
SELECT
    product_id,           -- 商品ID
    product_name,         -- 商品名称
    category_id,          -- 分类ID
    category_name,        -- 分类名称
    price,                -- 商品价格
    create_time           -- 创建时间
FROM ods_product_info;

SELECT * FROM dwd_product_info;

-- ==============================================================
-- 2. DWD 商品访客明细表
-- 基于ODS层商品访客表和商品信息表创建
-- 新增字段：is_micro_detail（是否微详情访客）
-- 过滤条件：停留时长大于0，访问时间在最近30天内
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_visit;
CREATE TABLE dwd_product_visit
AS
SELECT
    v.product_id,         -- 商品ID
    p.product_name,       -- 商品名称
    p.category_id,        -- 分类ID
    p.category_name,      -- 分类名称
    v.user_id,            -- 用户ID
    v.visit_time,         -- 访问时间
    v.stay_seconds,       -- 停留时长(秒)
    v.terminal_type,      -- 终端类型
    v.is_micro_detail     -- 是否微详情访客(停留>=3秒)
FROM ods_product_visit v
         JOIN ods_product_info p ON v.product_id = p.product_id
WHERE v.stay_seconds > 0
  AND v.visit_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SELECT * FROM dwd_product_visit;

-- ==============================================================
-- 3. DWD 商品收藏明细表
-- 基于ODS层商品收藏表和商品信息表创建
-- 包含用户对商品的收藏信息
-- 过滤条件：收藏时间在最近30天内
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_favorite;
CREATE TABLE dwd_product_favorite
AS
SELECT
    f.product_id,         -- 商品ID
    p.product_name,       -- 商品名称
    p.category_id,        -- 分类ID
    p.category_name,      -- 分类名称
    f.user_id,            -- 用户ID
    f.fav_time            -- 收藏时间
FROM ods_product_favorite f
         JOIN ods_product_info p ON f.product_id = p.product_id
WHERE f.fav_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SELECT * FROM dwd_product_favorite;

-- ==============================================================
-- 4. DWD 商品加购明细表
-- 基于ODS层商品加购表和商品信息表创建
-- 包含用户将商品加入购物车的信息
-- 过滤条件：加购时间在最近30天内
-- ==============================================================
DROP TABLE IF EXISTS dwd_product_cart;
CREATE TABLE dwd_product_cart
AS
SELECT
    c.product_id,         -- 商品ID
    p.product_name,       -- 商品名称
    p.category_id,        -- 分类ID
    p.category_name,      -- 分类名称
    c.user_id,            -- 用户ID
    c.quantity,           -- 商品数量
    c.cart_time           -- 加购时间
FROM ods_product_cart c
         JOIN ods_product_info p ON c.product_id = p.product_id
WHERE c.cart_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SELECT * FROM dwd_product_cart;

-- ==============================================================
-- 5. DWD 订单明细表
-- 基于ODS层订单信息表和商品信息表创建
-- 新增字段：activity_type（活动类型）
-- 包含订单的详细信息
-- 过滤条件：下单时间在最近30天内
-- ==============================================================
DROP TABLE IF EXISTS dwd_order_info;
CREATE TABLE dwd_order_info
AS
SELECT
    o.order_id,           -- 订单ID
    o.product_id,         -- 商品ID
    p.product_name,       -- 商品名称
    p.category_id,        -- 分类ID
    p.category_name,      -- 分类名称
    o.user_id,            -- 用户ID
    o.quantity,           -- 商品数量
    o.order_amount,       -- 订单金额
    o.order_time,         -- 下单时间
    o.terminal_type,      -- 终端类型
    o.activity_type       -- 活动类型
FROM ods_order_info o
         JOIN ods_product_info p ON o.product_id = p.product_id
WHERE o.order_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SELECT * FROM dwd_order_info;

-- ==============================================================
-- 6. DWD 支付明细表
-- 基于ODS层支付信息表、商品信息表和订单信息表创建
-- 新增字段：activity_type（活动类型）
-- 包含支付的详细信息
-- 过滤条件：支付金额大于0，支付时间不早于下单时间，支付时间在最近30天内
-- ==============================================================
DROP TABLE IF EXISTS dwd_payment_info;
CREATE TABLE dwd_payment_info
AS
SELECT
    pay.order_id,         -- 订单ID
    pay.product_id,       -- 商品ID
    p.product_name,       -- 商品名称
    p.category_id,        -- 分类ID
    p.category_name,      -- 分类名称
    pay.user_id,          -- 用户ID
    pay.pay_amount,       -- 支付金额
    pay.pay_time,         -- 支付时间
    pay.terminal_type,    -- 终端类型
    pay.activity_type     -- 活动类型
FROM ods_payment_info pay
         JOIN ods_product_info p ON pay.product_id = p.product_id
         JOIN ods_order_info o ON pay.order_id = o.order_id
WHERE pay.pay_amount > 0
  AND pay.pay_time >= o.order_time
  AND pay.pay_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SELECT * FROM dwd_payment_info;

-- ==============================================================
-- 7. DWD 退款明细表
-- 基于ODS层退款信息表和商品信息表创建
-- 包含退款的详细信息
-- 过滤条件：退款时间在最近30天内
-- ==============================================================
DROP TABLE IF EXISTS dwd_refund_info;
CREATE TABLE dwd_refund_info
AS
SELECT
    r.order_id,           -- 订单ID
    r.product_id,         -- 商品ID
    p.product_name,       -- 商品名称
    p.category_id,        -- 分类ID
    p.category_name,      -- 分类名称
    r.user_id,            -- 用户ID
    r.refund_amount,      -- 退款金额
    r.refund_time,        -- 退款时间
    r.refund_type         -- 退款类型
FROM ods_refund_info r
         JOIN ods_product_info p ON r.product_id = p.product_id
WHERE r.refund_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

SELECT * FROM dwd_refund_info;