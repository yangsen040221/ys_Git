use
sx_one_3_01;

-- ==============================================================
-- 1. 商品主题日汇总表
-- 基于DWD层各业务表创建商品维度的日汇总数据
-- 汇总商品的访问、收藏、加购、下单、支付、退款等核心指标
-- 数据粒度：按商品ID和日期进行汇总
-- 时间范围：最近30天的数据
-- ==============================================================
DROP TABLE IF EXISTS dws_product_day_summary;
CREATE TABLE dws_product_day_summary
(
    summary_date         DATE COMMENT '汇总日期',
    product_id           BIGINT COMMENT '商品ID',
    product_name         VARCHAR(100) COMMENT '商品名称',
    category_id          BIGINT COMMENT '分类ID',
    category_name        VARCHAR(50) COMMENT '分类名称',
    uv                   BIGINT COMMENT '访客数',
    pv                   BIGINT COMMENT '浏览量',
    avg_stay_seconds     DECIMAL(10, 2) COMMENT '平均停留时长(秒)',
    bounce_rate          DECIMAL(10, 4) COMMENT '跳出率',
    micro_detail_uv      BIGINT COMMENT '微详情访客数',
    fav_users            BIGINT COMMENT '收藏用户数',
    cart_users           BIGINT COMMENT '加购用户数',
    cart_qty             BIGINT COMMENT '加购数量',
    order_users          BIGINT COMMENT '下单用户数',
    order_qty            BIGINT COMMENT '下单数量',
    order_amount         DECIMAL(10, 2) COMMENT '下单金额',
    pay_users            BIGINT COMMENT '支付用户数',
    pay_qty              BIGINT COMMENT '支付商品数量',
    pay_amount           DECIMAL(10, 2) COMMENT '支付金额',
    juhuasuan_pay_amount DECIMAL(10, 2) COMMENT '聚划算支付金额',
    active_products      BIGINT COMMENT '动销商品数',
    item_unit_price      DECIMAL(10, 2) COMMENT '件单价',
    refund_amount        DECIMAL(10, 2) COMMENT '退款金额',
    PRIMARY KEY (summary_date, product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品主题日汇总表';

-- 从DWD层各业务表关联汇总商品相关指标
-- 关联表包括：商品访问表、商品收藏表、商品加购表、订单信息表、支付信息表、退款信息表
-- 统计逻辑：
--   1. UV：统计访问商品的独立用户数
--   2. PV：统计商品页面的总访问次数
--   3. 平均停留时长：所有用户访问该商品的平均停留时间
--   4. 跳出率：停留时间小于等于3秒的访问占比
--   5. 微详情访客数：访问微详情页面且停留时间>=3秒的独立用户数
--   6. 收藏用户数：对商品进行收藏操作的独立用户数
--   7. 加购用户数和加购数量：将商品加入购物车的用户数和商品总数量
--   8. 下单用户数、下单数量和下单金额：下单相关的统计数据
--   9. 支付用户数、支付数量和支付金额：支付相关的统计数据
--   10. 聚划算支付金额：通过聚划算活动支付的金额
--   11. 动销商品数：有支付行为的商品数量
--   12. 件单价：支付金额与下单数量的比值
--   13. 退款金额：商品的退款总金额
INSERT INTO dws_product_day_summary
SELECT
    DATE (v.visit_time) AS summary_date,                                                                    -- 汇总日期，按访问时间日期部分分组
    v.product_id,                                                                                           -- 商品ID
    v.product_name,                                                                                         -- 商品名称
    v.category_id,                                                                                          -- 分类ID
    v.category_name,                                                                                        -- 分类名称
    COUNT (DISTINCT v.user_id) AS uv,                                                                       -- 访客数：访问该商品的独立用户数
    COUNT (*) AS pv,                                                                                        -- 浏览量：该商品的总访问次数
    ROUND(AVG (v.stay_seconds), 2) AS avg_stay_seconds,                                                     -- 平均停留时长：用户访问该商品的平均停留秒数
    ROUND(SUM (CASE WHEN v.stay_seconds <= 3 THEN 1 ELSE 0 END) / COUNT (*), 4) AS bounce_rate,             -- 跳出率：停留时间<=3秒的访问占比
    COUNT (DISTINCT CASE WHEN v.is_micro_detail = 1 THEN v.user_id END) AS micro_detail_uv,                 -- 微详情访客数：访问微详情且停留>=3秒的独立用户数
    COUNT (DISTINCT f.user_id) AS fav_users,                                                                -- 收藏用户数：对该商品进行收藏的独立用户数
    COUNT (DISTINCT c.user_id) AS cart_users,                                                               -- 加购用户数：将该商品加入购物车的独立用户数
    SUM (c.quantity) AS cart_qty,                                                                           -- 加购数量：该商品被加入购物车的总数量
    COUNT (DISTINCT o.user_id) AS order_users,                                                              -- 下单用户数：对该商品下单的独立用户数
    SUM (o.quantity) AS order_qty,                                                                          -- 下单数量：该商品的总下单数量
    SUM (o.order_amount) AS order_amount,                                                                   -- 下单金额：该商品的总下单金额
    COUNT (DISTINCT pay.user_id) AS pay_users,                                                              -- 支付用户数：对该商品支付的独立用户数
    SUM (CASE WHEN pay.pay_amount > 0 THEN 1 ELSE 0 END) AS pay_qty,                                        -- 支付商品数量：有支付行为的商品件数
    SUM (pay.pay_amount) AS pay_amount,                                                                     -- 支付金额：该商品的总支付金额
    SUM (CASE WHEN pay.activity_type = 'JUHUASUAN' THEN pay.pay_amount ELSE 0 END) AS juhuasuan_pay_amount, -- 聚划算支付金额：通过聚划算活动支付的金额
    COUNT (DISTINCT CASE WHEN pay.pay_amount > 0 THEN v.product_id END) AS active_products,                 -- 动销商品数：有支付行为的商品数量
    ROUND(SUM (pay.pay_amount) / NULLIF (SUM (o.quantity), 0), 2) AS item_unit_price,                       -- 件单价：支付金额与下单数量的比值
    SUM (r.refund_amount) AS refund_amount                                                                  -- 退款金额：该商品的总退款金额
FROM dwd_product_visit v
    LEFT JOIN dwd_product_favorite f
ON v.product_id = f.product_id AND DATE (f.fav_time) = DATE (v.visit_time) AND v.user_id = f.user_id
    LEFT JOIN dwd_product_cart c ON v.product_id = c.product_id AND DATE (c.cart_time) = DATE (v.visit_time) AND v.user_id = c.user_id
    LEFT JOIN dwd_order_info o ON v.product_id = o.product_id AND DATE (o.order_time) = DATE (v.visit_time) AND v.user_id = o.user_id
    LEFT JOIN dwd_payment_info pay ON v.product_id = pay.product_id AND DATE (pay.pay_time) = DATE (v.visit_time) AND v.user_id = pay.user_id
    LEFT JOIN dwd_refund_info r ON v.product_id = r.product_id AND DATE (r.refund_time) = DATE (v.visit_time) AND v.user_id = r.user_id
WHERE v.visit_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
GROUP BY DATE (v.visit_time), v.product_id, v.product_name, v.category_id, v.category_name;

SELECT *
FROM dws_product_day_summary;

-- ==============================================================
-- 2. 用户主题日汇总表
-- 基于DWD层各业务表创建用户维度的日汇总数据
-- 汇总用户的访问、收藏、加购、下单、支付、退款等核心指标
-- 数据粒度：按用户ID和日期进行汇总
-- 时间范围：最近30天的数据
-- ==============================================================
DROP TABLE IF EXISTS dws_user_day_summary;
CREATE TABLE dws_user_day_summary
(
    summary_date        DATE COMMENT '汇总日期',
    user_id             BIGINT COMMENT '用户ID',
    visit_products      BIGINT COMMENT '访问商品数',
    pv                  BIGINT COMMENT '浏览量',
    fav_count           BIGINT COMMENT '收藏次数',
    cart_count          BIGINT COMMENT '加购次数',
    order_count         BIGINT COMMENT '下单次数',
    pay_count           BIGINT COMMENT '支付次数',
    total_pay_amount    DECIMAL(10, 2) COMMENT '支付总金额',
    total_refund_amount DECIMAL(10, 2) COMMENT '退款总金额',
    PRIMARY KEY (summary_date, user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户主题日汇总表';

-- 从DWD层各业务表关联汇总用户相关指标
-- 关联表包括：商品访问表、商品收藏表、商品加购表、订单信息表、支付信息表、退款信息表
-- 统计逻辑：
--   1. 访问商品数：用户访问的不同商品数量
--   2. 浏览量：用户总的页面浏览次数
--   3. 收藏次数：用户进行收藏操作的总次数
--   4. 加购次数：用户将商品加入购物车的总次数
--   5. 下单次数：用户下单的总次数
--   6. 支付次数：用户支付的总次数
--   7. 支付总金额：用户支付的总金额
--   8. 退款总金额：用户退款的总金额
INSERT INTO dws_user_day_summary
SELECT summary_date,                                     -- 汇总日期
       user_id,                                          -- 用户ID
       COUNT(DISTINCT product_id) AS visit_products,     -- 访问商品数：用户访问的不同商品数量
       SUM(pv)                    AS pv,                 -- 浏览量：用户总的页面浏览次数
       SUM(fav_flag)              AS fav_count,          -- 收藏次数：用户进行收藏操作的总次数
       SUM(cart_flag)             AS cart_count,         -- 加购次数：用户将商品加入购物车的总次数
       SUM(order_flag)            AS order_count,        -- 下单次数：用户下单的总次数
       SUM(pay_flag)              AS pay_count,          -- 支付次数：用户支付的总次数
       SUM(pay_amount)            AS total_pay_amount,   -- 支付总金额：用户支付的总金额
       SUM(refund_amount)         AS total_refund_amount -- 退款总金额：用户退款的总金额
FROM (SELECT
          DATE (v.visit_time) AS summary_date, -- 汇总日期，按访问时间日期部分分组
     v.user_id,                                -- 用户ID
     v.product_id,                             -- 商品ID
     COUNT(*) AS pv,                           -- 浏览量：针对特定商品的浏览次数
     CASE WHEN f.user_id IS NOT NULL THEN 1 ELSE 0 END AS fav_flag,      -- 收藏标记：该次访问是否产生收藏行为
             CASE WHEN c.user_id IS NOT NULL THEN 1 ELSE 0
END AS cart_flag,     -- 加购标记：该次访问是否产生加购行为
             CASE WHEN o.user_id IS NOT NULL THEN 1 ELSE 0
END AS order_flag,    -- 下单标记：该次访问是否产生下单行为
             CASE WHEN pay.user_id IS NOT NULL THEN 1 ELSE 0
END AS pay_flag,    -- 支付标记：该次访问是否产生支付行为
             COALESCE(pay.pay_amount,0) AS pay_amount,   -- 支付金额：该次访问关联的支付金额，为空则为0
             COALESCE(r.refund_amount,0) AS refund_amount-- 退款金额：该次访问关联的退款金额，为空则为0
         FROM dwd_product_visit v
                  LEFT JOIN dwd_product_favorite f ON v.product_id = f.product_id AND v.user_id = f.user_id AND DATE(f.fav_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_product_cart c ON v.product_id = c.product_id AND v.user_id = c.user_id AND DATE(c.cart_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_order_info o ON v.product_id = o.product_id AND v.user_id = o.user_id AND DATE(o.order_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_payment_info pay ON v.product_id = pay.product_id AND v.user_id = pay.user_id AND DATE(pay.pay_time) = DATE(v.visit_time)
                  LEFT JOIN dwd_refund_info r ON v.product_id = r.product_id AND v.user_id = r.user_id AND DATE(r.refund_time) = DATE(v.visit_time)
         WHERE v.visit_time >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
         GROUP BY DATE(v.visit_time), v.user_id, v.product_id, f.user_id, c.user_id, o.user_id, pay.user_id, pay.pay_amount, r.refund_amount
     ) t
GROUP BY summary_date, user_id;

SELECT *
FROM dws_user_day_summary;

-- ==============================================================
-- 3. 活动主题日汇总表
-- 基于订单信息表创建活动维度的日汇总数据
-- 汇总不同活动类型的访客数、下单金额、支付金额、退款金额等核心指标
-- 数据粒度：按活动类型和日期进行汇总
-- 时间范围：最近30天的数据
-- ==============================================================
DROP TABLE IF EXISTS dws_activity_day_summary;
CREATE TABLE dws_activity_day_summary
(
    summary_date        DATE COMMENT '汇总日期',
    activity_type       VARCHAR(20) COMMENT '活动类型',
    uv                  BIGINT COMMENT '访客数',
    total_order_amount  DECIMAL(10, 2) COMMENT '总下单金额',
    total_pay_amount    DECIMAL(10, 2) COMMENT '总支付金额',
    total_refund_amount DECIMAL(10, 2) COMMENT '总退款金额',
    PRIMARY KEY (summary_date, activity_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='活动主题日汇总表';

-- 从订单信息表关联支付信息表和退款信息表汇总活动相关指标
-- 关联表包括：订单信息表、支付信息表、退款信息表
-- 统计逻辑：
--   1. 访客数：参与该活动的独立用户数
--   2. 总下单金额：该活动的总下单金额
--   3. 总支付金额：该活动的总支付金额
--   4. 总退款金额：该活动的总退款金额
INSERT INTO dws_activity_day_summary
SELECT summary_date,                                  -- 汇总日期
       activity_type,                                 -- 活动类型
       COUNT(DISTINCT user_id) AS uv,                 -- 访客数：参与该活动的独立用户数
       SUM(order_amount)       AS total_order_amount, -- 总下单金额：该活动的总下单金额
       SUM(pay_amount)         AS total_pay_amount,   -- 总支付金额：该活动的总支付金额
       SUM(refund_amount)      AS total_refund_amount -- 总退款金额：该活动的总退款金额
FROM (SELECT
          DATE (o.order_time) AS summary_date,    -- 汇总日期，按下单时间日期部分分组
     o.activity_type,                             -- 活动类型
     o.user_id,                                   -- 用户ID
     o.order_amount,                              -- 订单金额
     COALESCE(pay.pay_amount, 0) AS pay_amount,   -- 支付金额，为空则为0
     COALESCE(r.refund_amount, 0) AS refund_amount-- 退款金额，为空则为0
    FROM dwd_order_info o
                  LEFT JOIN dwd_payment_info pay
ON o.order_id = pay.order_id
    LEFT JOIN dwd_refund_info r ON o.order_id = r.order_id
WHERE o.order_time >= DATE_SUB(CURDATE()
    , INTERVAL 30 DAY)
    ) t
GROUP BY summary_date, activity_type;

SELECT *
FROM dws_activity_day_summary;

-- ==============================================================
-- 4. 交易主题日汇总表
-- 基于订单信息表创建交易维度的日汇总数据
-- 汇总每日的访客数、订单数、下单金额、支付用户数、支付金额、退款金额等核心指标
-- 数据粒度：按日期进行汇总
-- 时间范围：最近30天的数据
-- ==============================================================
DROP TABLE IF EXISTS dws_trade_day_summary;
CREATE TABLE dws_trade_day_summary
(
    summary_date        DATE COMMENT '汇总日期',
    uv                  BIGINT COMMENT '访客数',
    order_count         BIGINT COMMENT '订单数',
    total_order_amount  DECIMAL(10, 2) COMMENT '总下单金额',
    pay_user_count      BIGINT COMMENT '支付用户数',
    total_pay_amount    DECIMAL(10, 2) COMMENT '总支付金额',
    total_refund_amount DECIMAL(10, 2) COMMENT '总退款金额',
    PRIMARY KEY (summary_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='交易主题日汇总表';

-- 从订单信息表关联支付信息表和退款信息表汇总交易相关指标
-- 关联表包括：订单信息表、支付信息表、退款信息表
-- 统计逻辑：
--   1. 访客数：产生下单行为的独立用户数
--   2. 订单数：总订单数量
--   3. 总下单金额：所有订单的总金额
--   4. 支付用户数：完成支付的独立用户数
--   5. 总支付金额：所有支付的总金额
--   6. 总退款金额：所有退款的总金额
INSERT INTO dws_trade_day_summary
SELECT summary_date,                                      -- 汇总日期
       COUNT(DISTINCT user_id)     AS uv,                 -- 访客数：产生下单行为的独立用户数
       COUNT(DISTINCT order_id)    AS order_count,        -- 订单数：总订单数量
       SUM(order_amount)           AS total_order_amount, -- 总下单金额：所有订单的总金额
       COUNT(DISTINCT pay_user_id) AS pay_user_count,     -- 支付用户数：完成支付的独立用户数
       SUM(pay_amount)             AS total_pay_amount,   -- 总支付金额：所有支付的总金额
       SUM(refund_amount)          AS total_refund_amount -- 总退款金额：所有退款的总金额
FROM (SELECT
          DATE (o.order_time) AS summary_date, -- 汇总日期，按下单时间日期部分分组
     o.order_id,                               -- 订单ID
     o.user_id,                                -- 用户ID（下单用户）
     o.order_amount,                           -- 订单金额
     pay.user_id AS pay_user_id,               -- 支付用户ID
     pay.pay_amount,                           -- 支付金额
     r.refund_amount                           -- 退款金额
    FROM dwd_order_info o
                  LEFT JOIN dwd_payment_info pay
ON o.order_id = pay.order_id
    LEFT JOIN dwd_refund_info r ON o.order_id = r.order_id
WHERE o.order_time >= DATE_SUB(CURDATE()
    , INTERVAL 30 DAY)
    ) t
GROUP BY summary_date;

SELECT *
FROM dws_trade_day_summary;