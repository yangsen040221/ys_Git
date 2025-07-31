use gd;

-- -------------------------------------------------------------------------------------------------
-- 用户行为日志表
-- -------------------------------------------------------------------------------------------------

-- 建表语句
CREATE TABLE IF NOT EXISTS ods_user_behavior_log (
    user_id VARCHAR(255) COMMENT '用户ID',
    item_id VARCHAR(255) COMMENT '商品ID',
    behavior_type VARCHAR(255) COMMENT '行为类型：visit-访问, collect-收藏, cart-加购, pay-支付',
    behavior_time DATETIME COMMENT '行为时间',
    page_from VARCHAR(255) COMMENT '来源页面（如商品详情页ID）',
    dt VARCHAR(255) COMMENT '分区日期'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
COMMENT '用户行为原始日志表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';

-- 插入100条模拟数据（近7天，覆盖4种行为类型）
INSERT INTO ods_user_behavior_log (
    user_id,
    item_id,
    behavior_type,
    behavior_time,
    page_from,
    dt
)
SELECT
    -- 生成30个用户ID，格式如user_001到user_030
    CONCAT('user_', LPAD(FLOOR(RAND() * 30) + 1, 3, '0')),
    -- 生成50个商品ID，格式如item_001到item_050
    CONCAT('item_', LPAD(FLOOR(RAND() * 50) + 1, 3, '0')),
    -- 随机生成行为类型
    ELT(FLOOR(RAND() * 4) + 1, 'visit', 'collect', 'cart', 'pay'),
    -- 生成近7天的随机时间
    DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 168) HOUR),
    -- 生成来源页面
    CONCAT('page_', LPAD(FLOOR(RAND() * 20) + 1, 3, '0')),
    -- 生成近7天的分区日期
    DATE(DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 7) DAY))
FROM (
    -- 生成1-10的数字序列
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
) t1
CROSS JOIN (
    -- 再次生成1-10的数字序列，与t1交叉连接生成100行数据
    SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
) t2;

-- 查看数据
SELECT * FROM ods_user_behavior_log;

-- -------------------------------------------------------------------------------------------------
-- 商品基础信息表
-- -------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS ods_item_info;

CREATE TABLE IF NOT EXISTS ods_item_info (
    item_id VARCHAR(255) COMMENT '商品ID',
    item_name VARCHAR(255) COMMENT '商品名称',
    category_id VARCHAR(255) COMMENT '分类ID',
    is_drainage VARCHAR(1) COMMENT '是否引流款：1-是, 0-否',
    is_hot VARCHAR(1) COMMENT '是否热销款：1-是, 0-否',
    create_time DATETIME COMMENT '创建时间',
    dt VARCHAR(255) COMMENT '分区日期（格式：YYYYMMDD）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
COMMENT '商品基础信息表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';

-- 使用更高效的方式生成1-100的数字序列
INSERT INTO ods_item_info (
    item_id,
    item_name,
    category_id,
    is_drainage,
    is_hot,
    create_time,
    dt
)
SELECT
    CONCAT('item_', LPAD(t, 3, '0')) AS item_id,
    CONCAT('商品', t, '-',
           CASE FLOOR(t % 5)
               WHEN 0 THEN '服装'
               WHEN 1 THEN '家电'
               WHEN 2 THEN '食品'
               WHEN 3 THEN '数码'
               ELSE '美妆'
           END) AS item_name,
    CONCAT('cat_', LPAD(CAST(FLOOR(t % 5) AS CHAR), 2, '0')) AS category_id,
    CASE WHEN t % 10 = 0 THEN '1' ELSE '0' END AS is_drainage,  -- 10%为引流款
    CASE WHEN t % 8 = 0 THEN '1' ELSE '0' END AS is_hot,        -- 12.5%为热销款
    @create_time := DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 30) DAY) AS create_time,  -- 近30天创建
    DATE(@create_time) AS dt  -- 与create_time保持一致的日期
FROM (
    SELECT 1 t UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
    UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    UNION ALL SELECT 11 UNION ALL SELECT 12 UNION ALL SELECT 13 UNION ALL SELECT 14 UNION ALL SELECT 15
    UNION ALL SELECT 16 UNION ALL SELECT 17 UNION ALL SELECT 18 UNION ALL SELECT 19 UNION ALL SELECT 20
    UNION ALL SELECT 21 UNION ALL SELECT 22 UNION ALL SELECT 23 UNION ALL SELECT 24 UNION ALL SELECT 25
    UNION ALL SELECT 26 UNION ALL SELECT 27 UNION ALL SELECT 28 UNION ALL SELECT 29 UNION ALL SELECT 30
    UNION ALL SELECT 31 UNION ALL SELECT 32 UNION ALL SELECT 33 UNION ALL SELECT 34 UNION ALL SELECT 35
    UNION ALL SELECT 36 UNION ALL SELECT 37 UNION ALL SELECT 38 UNION ALL SELECT 39 UNION ALL SELECT 40
    UNION ALL SELECT 41 UNION ALL SELECT 42 UNION ALL SELECT 43 UNION ALL SELECT 44 UNION ALL SELECT 45
    UNION ALL SELECT 46 UNION ALL SELECT 47 UNION ALL SELECT 48 UNION ALL SELECT 49 UNION ALL SELECT 50
    UNION ALL SELECT 51 UNION ALL SELECT 52 UNION ALL SELECT 53 UNION ALL SELECT 54 UNION ALL SELECT 55
    UNION ALL SELECT 56 UNION ALL SELECT 57 UNION ALL SELECT 58 UNION ALL SELECT 59 UNION ALL SELECT 60
    UNION ALL SELECT 61 UNION ALL SELECT 62 UNION ALL SELECT 63 UNION ALL SELECT 64 UNION ALL SELECT 65
    UNION ALL SELECT 66 UNION ALL SELECT 67 UNION ALL SELECT 68 UNION ALL SELECT 69 UNION ALL SELECT 70
    UNION ALL SELECT 71 UNION ALL SELECT 72 UNION ALL SELECT 73 UNION ALL SELECT 74 UNION ALL SELECT 75
    UNION ALL SELECT 76 UNION ALL SELECT 77 UNION ALL SELECT 78 UNION ALL SELECT 79 UNION ALL SELECT 80
    UNION ALL SELECT 81 UNION ALL SELECT 82 UNION ALL SELECT 83 UNION ALL SELECT 84 UNION ALL SELECT 85
    UNION ALL SELECT 86 UNION ALL SELECT 87 UNION ALL SELECT 88 UNION ALL SELECT 89 UNION ALL SELECT 90
    UNION ALL SELECT 91 UNION ALL SELECT 92 UNION ALL SELECT 93 UNION ALL SELECT 94 UNION ALL SELECT 95
    UNION ALL SELECT 96 UNION ALL SELECT 97 UNION ALL SELECT 98 UNION ALL SELECT 99 UNION ALL SELECT 100
) t;

-- 查看数据
SELECT * FROM ods_item_info;

-- -------------------------------------------------------------------------------------------------
-- 商品页面跳转日志表
-- -------------------------------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ods_page_jump_log (
    user_id VARCHAR(255) COMMENT '用户ID',
    from_item_id VARCHAR(255) COMMENT '来源商品ID（主商品）',
    to_item_id VARCHAR(255) COMMENT '跳转商品ID（关联商品）',
    jump_time DATETIME COMMENT '跳转时间',
    dt VARCHAR(10) COMMENT '分区日期（格式：YYYY-MM-DD）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 
COMMENT '商品详情页跳转日志表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';

-- 插入模拟数据
INSERT INTO ods_page_jump_log (
    user_id,
    from_item_id,
    to_item_id,
    jump_time,
    dt
)
SELECT
    -- 生成30个用户ID
    CONCAT('user_', LPAD(FLOOR(RAND() * 30) + 1, 3, '0')) AS user_id,
    -- 生成来源商品ID
    CONCAT('item_', LPAD(from_rand, 3, '0')) AS from_item_id,
    -- 生成跳转商品ID（确保与来源不同）
    CONCAT('item_', LPAD(
        CASE
            WHEN to_rand = from_rand THEN (to_rand % 49) + 1
            ELSE to_rand
        END, 3, '0')) AS to_item_id,
    -- 生成近7天的随机时间
    DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 168) HOUR) AS jump_time,
    -- 生成近7天的分区日期
    DATE(DATE_SUB(NOW(), INTERVAL FLOOR(RAND() * 7) DAY)) AS dt
FROM (
    -- 生成随机数并关联，确保from和to不同
    SELECT
        FLOOR(RAND() * 50) + 1 AS from_rand,
        FLOOR(RAND() * 50) + 1 AS to_rand
    FROM (
        SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
        UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) t1
    CROSS JOIN (
        SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5
        UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9 UNION ALL SELECT 10
    ) t2
) AS rand_data;

-- 查看数据
SELECT * FROM ods_page_jump_log;