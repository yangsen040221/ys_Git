USE sx_one_3_01;

-- ==============================================================
-- 1. 商品主题 - 日指标
-- 从DWS层商品日汇总表中提取日维度指标数据
-- 包含商品的访问、收藏、加购、下单、支付、退款等核心指标
-- 数据粒度：按商品ID和日期进行汇总
-- ==============================================================
DROP TABLE IF EXISTS ads_product_day;
CREATE TABLE ads_product_day
(
    summary_date     DATE COMMENT '日期',
    product_id       BIGINT COMMENT '商品ID',
    product_name     VARCHAR(100) COMMENT '商品名称',
    category_id      BIGINT COMMENT '分类ID',
    category_name    VARCHAR(50) COMMENT '分类名称',
    uv               BIGINT COMMENT '访客数',
    pv               BIGINT COMMENT '浏览量',
    avg_stay_seconds DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
    bounce_rate      DECIMAL(10, 4) COMMENT '跳出率',
    fav_users        BIGINT COMMENT '收藏用户数',
    cart_users       BIGINT COMMENT '加购用户数',
    order_users      BIGINT COMMENT '下单用户数',
    pay_users        BIGINT COMMENT '支付用户数',
    pay_amount       DECIMAL(10, 2) COMMENT '支付金额',
    refund_amount    DECIMAL(10, 2) COMMENT '退款金额',
    PRIMARY KEY (summary_date, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS-商品主题日指标';

-- 直接从DWS层商品日汇总表中提取所有字段数据
-- 保持与DWS层相同的数据粒度和统计逻辑
INSERT INTO ads_product_day (summary_date, product_id, product_name, category_id, category_name,
                             uv, pv, avg_stay_seconds, bounce_rate, fav_users, cart_users,
                             order_users, pay_users, pay_amount, refund_amount)
SELECT summary_date,        -- 日期
       product_id,          -- 商品ID
       product_name,        -- 商品名称
       category_id,         -- 分类ID
       category_name,       -- 分类名称
       uv,                  -- 访客数
       pv,                  -- 浏览量
       avg_stay_seconds,    -- 平均停留时长(秒)
       bounce_rate,         -- 跳出率
       fav_users,           -- 收藏用户数
       cart_users,          -- 加购用户数
       order_users,         -- 下单用户数
       pay_users,           -- 支付用户数
       pay_amount,          -- 支付金额
       refund_amount        -- 退款金额
FROM dws_product_day_summary;

SELECT * FROM ads_product_day;

-- ==============================================================
-- 2. 商品主题 - 周指标
-- 基于DWS层商品日汇总表聚合生成周维度指标数据
-- 对一周内的各项指标进行汇总计算
-- 数据粒度：按商品ID和周进行汇总
-- ==============================================================
DROP TABLE IF EXISTS ads_product_week;
CREATE TABLE ads_product_week
(
    week_start       DATE COMMENT '周开始日期',
    product_id       BIGINT COMMENT '商品ID',
    product_name     VARCHAR(100) COMMENT '商品名称',
    category_id      BIGINT COMMENT '分类ID',
    category_name    VARCHAR(50) COMMENT '分类名称',
    uv               BIGINT COMMENT '访客数',
    pv               BIGINT COMMENT '浏览量',
    avg_stay_seconds DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
    bounce_rate      DECIMAL(10, 4) COMMENT '跳出率',
    fav_users        BIGINT COMMENT '收藏用户数',
    cart_users       BIGINT COMMENT '加购用户数',
    order_users      BIGINT COMMENT '下单用户数',
    pay_users        BIGINT COMMENT '支付用户数',
    pay_amount       DECIMAL(10, 2) COMMENT '支付金额',
    refund_amount    DECIMAL(10, 2) COMMENT '退款金额',
    PRIMARY KEY (week_start, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS-商品主题周指标';

-- 基于DWS层商品日汇总表按周进行聚合
-- 聚合逻辑：
--   1. 周开始日期：一周中最早日期作为周标识
--   2. UV、PV、收藏用户数、加购用户数、下单用户数、支付用户数、支付金额、退款金额：直接求和
--   3. 平均停留时长：对每日平均值再求平均
--   4. 跳出率：各天跳出率的平均值
INSERT INTO ads_product_week (week_start, product_id, product_name, category_id, category_name,
                              uv, pv, avg_stay_seconds, bounce_rate, fav_users, cart_users,
                              order_users, pay_users, pay_amount, refund_amount)
SELECT MIN(summary_date)                     AS week_start,      -- 周开始日期：一周中最早的日期
       product_id,                           -- 商品ID
       product_name,                         -- 商品名称
       category_id,                          -- 分类ID
       category_name,                        -- 分类名称
       SUM(uv)                               AS uv,              -- 周访客数：一周内每日访客数之和
       SUM(pv)                               AS pv,              -- 周浏览量：一周内每日浏览量之和
       ROUND(AVG(avg_stay_seconds), 2)       AS avg_stay_seconds,-- 周平均停留时长：每日平均值的平均
       ROUND(SUM(bounce_rate) / COUNT(*), 4) AS bounce_rate,     -- 周跳出率：各天跳出率的平均值
       SUM(fav_users)                        AS fav_users,       -- 周收藏用户数：一周内每日收藏用户数之和
       SUM(cart_users)                       AS cart_users,      -- 周加购用户数：一周内每日加购用户数之和
       SUM(order_users)                      AS order_users,     -- 周下单用户数：一周内每日下单用户数之和
       SUM(pay_users)                        AS pay_users,       -- 周支付用户数：一周内每日支付用户数之和
       SUM(pay_amount)                       AS pay_amount,      -- 周支付金额：一周内每日支付金额之和
       SUM(refund_amount)                    AS refund_amount    -- 周退款金额：一周内每日退款金额之和
FROM dws_product_day_summary
GROUP BY YEARWEEK(summary_date, 1), product_id, product_name, category_id, category_name;

SELECT * FROM ads_product_week;

-- ==============================================================
-- 3. 商品主题 - 月指标
-- 基于DWS层商品日汇总表聚合生成月维度指标数据
-- 对一月内的各项指标进行汇总计算
-- 数据粒度：按商品ID和月份进行汇总
-- ==============================================================
DROP TABLE IF EXISTS ads_product_month;
CREATE TABLE ads_product_month
(
    month_key        CHAR(7) COMMENT '月份',
    product_id       BIGINT COMMENT '商品ID',
    product_name     VARCHAR(100) COMMENT '商品名称',
    category_id      BIGINT COMMENT '分类ID',
    category_name    VARCHAR(50) COMMENT '分类名称',
    uv               BIGINT COMMENT '访客数',
    pv               BIGINT COMMENT '浏览量',
    avg_stay_seconds DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
    bounce_rate      DECIMAL(10, 4) COMMENT '跳出率',
    fav_users        BIGINT COMMENT '收藏用户数',
    cart_users       BIGINT COMMENT '加购用户数',
    order_users      BIGINT COMMENT '下单用户数',
    pay_users        BIGINT COMMENT '支付用户数',
    pay_amount       DECIMAL(10, 2) COMMENT '支付金额',
    refund_amount    DECIMAL(10, 2) COMMENT '退款金额',
    PRIMARY KEY (month_key, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS-商品主题月指标';

-- 基于DWS层商品日汇总表按月进行聚合
-- 聚合逻辑：
--   1. 月份：使用日期格式化函数提取年月作为月份标识
--   2. UV、PV、收藏用户数、加购用户数、下单用户数、支付用户数、支付金额、退款金额：直接求和
--   3. 平均停留时长：对每日平均值再求平均
--   4. 跳出率：各天跳出率的平均值
INSERT INTO ads_product_month (month_key, product_id, product_name, category_id, category_name,
                               uv, pv, avg_stay_seconds, bounce_rate, fav_users, cart_users,
                               order_users, pay_users, pay_amount, refund_amount)
SELECT DATE_FORMAT(summary_date, '%Y-%m')    AS month_key,        -- 月份：格式化为'YYYY-MM'
       product_id,                           -- 商品ID
       product_name,                         -- 商品名称
       category_id,                          -- 分类ID
       category_name,                        -- 分类名称
       SUM(uv)                               AS uv,               -- 月访客数：一月内每日访客数之和
       SUM(pv)                               AS pv,               -- 月浏览量：一月内每日浏览量之和
       ROUND(AVG(avg_stay_seconds), 2)       AS avg_stay_seconds, -- 月平均停留时长：每日平均值的平均
       ROUND(SUM(bounce_rate) / COUNT(*), 4) AS bounce_rate,      -- 月跳出率：各天跳出率的平均值
       SUM(fav_users)                        AS fav_users,        -- 月收藏用户数：一月内每日收藏用户数之和
       SUM(cart_users)                       AS cart_users,       -- 月加购用户数：一月内每日加购用户数之和
       SUM(order_users)                      AS order_users,      -- 月下单用户数：一月内每日下单用户数之和
       SUM(pay_users)                        AS pay_users,        -- 月支付用户数：一月内每日支付用户数之和
       SUM(pay_amount)                       AS pay_amount,       -- 月支付金额：一月内每日支付金额之和
       SUM(refund_amount)                    AS refund_amount     -- 月退款金额：一月内每日退款金额之和
FROM dws_product_day_summary
GROUP BY DATE_FORMAT(summary_date, '%Y-%m'), product_id, product_name, category_id, category_name;

SELECT * FROM ads_product_month;

-- ==============================================================
-- 4. 平台交易概览（日）
-- 从DWS层交易日汇总表中提取平台级日维度交易指标数据
-- 包含平台整体的访客、订单、支付、退款等核心指标
-- 数据粒度：按日期进行汇总
-- ==============================================================
DROP TABLE IF EXISTS ads_trade_day;
CREATE TABLE ads_trade_day
(
    summary_date        DATE COMMENT '日期',
    uv                  BIGINT COMMENT '访客数',
    order_count         BIGINT COMMENT '订单数',
    total_order_amount  DECIMAL(10, 2) COMMENT '下单总金额',
    pay_user_count      BIGINT COMMENT '支付用户数',
    total_pay_amount    DECIMAL(10, 2) COMMENT '支付总金额',
    total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
    PRIMARY KEY (summary_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS-交易主题日指标';

-- 直接从DWS层交易日汇总表中提取所有字段数据
-- 保持与DWS层相同的数据粒度和统计逻辑
INSERT INTO ads_trade_day (summary_date, uv, order_count, total_order_amount,
                           pay_user_count, total_pay_amount, total_refund_amount)
SELECT summary_date,         -- 日期
       uv,                   -- 访客数
       order_count,          -- 订单数
       total_order_amount,   -- 下单总金额
       pay_user_count,       -- 支付用户数
       total_pay_amount,     -- 支付总金额
       total_refund_amount   -- 退款总金额
FROM dws_trade_day_summary;

SELECT * FROM ads_trade_day;

-- ==============================================================
-- 5. 平台交易概览（周）
-- 基于DWS层交易日汇总表聚合生成平台级周维度交易指标数据
-- 对一周内的各项交易指标进行汇总计算
-- 数据粒度：按周进行汇总
-- ==============================================================
DROP TABLE IF EXISTS ads_trade_week;
CREATE TABLE ads_trade_week
(
    week_start          DATE COMMENT '周开始日期',
    uv                  BIGINT COMMENT '访客数',
    order_count         BIGINT COMMENT '订单数',
    total_order_amount  DECIMAL(10, 2) COMMENT '下单总金额',
    pay_user_count      BIGINT COMMENT '支付用户数',
    total_pay_amount    DECIMAL(10, 2) COMMENT '支付总金额',
    total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
    PRIMARY KEY (week_start)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS-交易主题周指标';

-- 基于DWS层交易日汇总表按周进行聚合
-- 聚合逻辑：
--   1. 周开始日期：一周中最早日期作为周标识
--   2. UV、订单数、下单总金额、支付用户数、支付总金额、退款总金额：直接求和
INSERT INTO ads_trade_week (week_start, uv, order_count, total_order_amount,
                            pay_user_count, total_pay_amount, total_refund_amount)
SELECT MIN(summary_date)        AS week_start,          -- 周开始日期：一周中最早的日期
       SUM(uv)                  AS uv,                   -- 周访客数：一周内每日访客数之和
       SUM(order_count)         AS order_count,          -- 周订单数：一周内每日订单数之和
       SUM(total_order_amount)  AS total_order_amount,   -- 周下单总金额：一周内每日下单金额之和
       SUM(pay_user_count)      AS pay_user_count,       -- 周支付用户数：一周内每日支付用户数之和
       SUM(total_pay_amount)    AS total_pay_amount,     -- 周支付总金额：一周内每日支付金额之和
       SUM(total_refund_amount) AS total_refund_amount  -- 周退款总金额：一周内每日退款金额之和
FROM dws_trade_day_summary
GROUP BY YEARWEEK(summary_date, 1);

SELECT * FROM ads_trade_week;

-- ==============================================================
-- 6. 平台交易概览（月）
-- 基于DWS层交易日汇总表聚合生成平台级月维度交易指标数据
-- 对一月内的各项交易指标进行汇总计算
-- 数据粒度：按月份进行汇总
-- ==============================================================
DROP TABLE IF EXISTS ads_trade_month;
CREATE TABLE ads_trade_month
(
    month_key           CHAR(7) COMMENT '月份',
    uv                  BIGINT COMMENT '访客数',
    order_count         BIGINT COMMENT '订单数',
    total_order_amount  DECIMAL(10, 2) COMMENT '下单总金额',
    pay_user_count      BIGINT COMMENT '支付用户数',
    total_pay_amount    DECIMAL(10, 2) COMMENT '支付总金额',
    total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
    PRIMARY KEY (month_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ADS-交易主题月指标';

-- 基于DWS层交易日汇总表按月进行聚合
-- 聚合逻辑：
--   1. 月份：使用日期格式化函数提取年月作为月份标识
--   2. UV、订单数、下单总金额、支付用户数、支付总金额、退款总金额：直接求和
INSERT INTO ads_trade_month (month_key, uv, order_count, total_order_amount,
                             pay_user_count, total_pay_amount, total_refund_amount)
SELECT DATE_FORMAT(summary_date, '%Y-%m') AS month_key,         -- 月份：格式化为'YYYY-MM'
       SUM(uv)                            AS uv,                 -- 月访客数：一月内每日访客数之和
       SUM(order_count)                   AS order_count,        -- 月订单数：一月内每日订单数之和
       SUM(total_order_amount)            AS total_order_amount, -- 月下单总金额：一月内每日下单金额之和
       SUM(pay_user_count)                AS pay_user_count,     -- 月支付用户数：一月内每日支付用户数之和
       SUM(total_pay_amount)              AS total_pay_amount,   -- 月支付总金额：一月内每日支付金额之和
       SUM(total_refund_amount)           AS total_refund_amount -- 月退款总金额：一月内每日退款金额之和
FROM dws_trade_day_summary
GROUP BY DATE_FORMAT(summary_date, '%Y-%m');

SELECT * FROM ads_trade_month;