use gd;


-- 创建DWD层表（清洗后明细数据）
CREATE TABLE IF NOT EXISTS dwd_user_detail
(
    user_id       BIGINT NOT NULL COMMENT '用户ID',
    user_name     VARCHAR(50) COMMENT '用户名',
    gender        TINYINT COMMENT '性别: 1-男, 2-女, 0-未知',
    age           INT COMMENT '年龄',
    age_group     VARCHAR(20) COMMENT '年龄组',
    city          VARCHAR(50) COMMENT '城市',
    city_level    TINYINT COMMENT '城市等级',
    register_time DATETIME COMMENT '注册时间',
    register_date DATE COMMENT '注册日期',
    channel       VARCHAR(20) COMMENT '注册渠道',
    is_active     TINYINT COMMENT '是否活跃: 0-否, 1-是',
    etl_time      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(user_id)
COMMENT '用户明细数据表'
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);


-- ODS到DWD层：用户数据清洗转换
INSERT INTO dwd_user_detail
SELECT user_id,
       user_name,
       gender,
       age,
       -- 年龄分组
       CASE
           WHEN age < 25 THEN '18-24'
           WHEN age < 35 THEN '25-34'
           WHEN age < 45 THEN '35-44'
           WHEN age < 55 THEN '45-54'
           ELSE '55+'
           END             AS age_group,
       city,
       -- 城市等级划分
       CASE
           WHEN city IN ('北京', '上海', '广州', '深圳') THEN 1
           WHEN city IN ('杭州', '成都', '武汉', '南京') THEN 2
           ELSE 3
           END             AS city_level,
       register_time,
       DATE(register_time) AS register_date,
       channel,
       -- 判断是否为活跃用户（近30天有行为）
       CASE
           WHEN EXISTS(
                   SELECT 1
                   FROM ods_user_behavior b
                   WHERE b.user_id = u.user_id
                     AND b.behavior_time >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
               ) THEN 1
           ELSE 0
           END             AS is_active,
       CURRENT_TIMESTAMP   AS etl_time
FROM ods_user u;



select * from dwd_user_detail;









-- 营销活动明细数据表
CREATE TABLE IF NOT EXISTS dwd_marketing_activity_detail
(
    activity_id         BIGINT NOT NULL COMMENT '活动ID',
    activity_name       VARCHAR(100) COMMENT '活动名称',
    activity_type       VARCHAR(20) COMMENT '活动类型',
    start_time          DATETIME COMMENT '开始时间',
    end_time            DATETIME COMMENT '结束时间',
    status              TINYINT COMMENT '状态: 0-未开始, 1-进行中, 2-已结束',
    channel             VARCHAR(50) COMMENT '活动渠道',
    budget              DECIMAL(15, 2) COMMENT '活动预算',
    duration_days       INT COMMENT '活动持续天数',
    is_weekend_included TINYINT COMMENT '是否包含周末: 0-否, 1-是',
    etl_time            DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(activity_id)
COMMENT '营销活动明细数据表(清洗后)'
DISTRIBUTED BY HASH(activity_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);


INSERT INTO dwd_marketing_activity_detail
SELECT
    activity_id,
    activity_name,
    activity_type,
    start_time,
    end_time,
    -- 处理状态异常值
    CASE WHEN status NOT IN (0, 1, 2) THEN
             CASE
                 WHEN end_time < CURRENT_TIMESTAMP THEN 2
                 WHEN start_time > CURRENT_TIMESTAMP THEN 0
                 ELSE 1
                 END
         ELSE status END AS status,
    channel,
    -- 处理预算异常值
    CASE WHEN budget < 0 THEN 0 ELSE budget END AS budget,
    -- 计算活动持续天数
    DATEDIFF(end_time, start_time) AS duration_days,
    -- 判断是否包含周末
    CASE
        WHEN (WEEKDAY(start_time) BETWEEN 5 AND 6) OR (WEEKDAY(end_time) BETWEEN 5 AND 6) THEN 1
        WHEN DATEDIFF(end_time, start_time) >= 7 THEN 1
        ELSE 0
        END AS is_weekend_included,
    CURRENT_TIMESTAMP AS etl_time
FROM ods_marketing_activity a;



select * from dwd_marketing_activity_detail;











-- 用户行为明细数据表
CREATE TABLE IF NOT EXISTS dwd_user_behavior_detail
(
    behavior_id   BIGINT NOT NULL COMMENT '行为ID',
    user_id       BIGINT COMMENT '用户ID',
    activity_id   BIGINT COMMENT '活动ID',
    behavior_type VARCHAR(20) COMMENT '行为类型',
    behavior_time DATETIME COMMENT '行为时间',
    behavior_date DATE COMMENT '行为日期',
    behavior_hour TINYINT COMMENT '行为小时',
    is_weekend    TINYINT COMMENT '是否周末: 0-否, 1-是',
    page          VARCHAR(50) COMMENT '页面',
    device        VARCHAR(50) COMMENT '设备',
    device_type   VARCHAR(20) COMMENT '设备类型: mobile-移动设备, pc-电脑',
    ip            VARCHAR(20) COMMENT 'IP地址',
    province      VARCHAR(50) COMMENT '省份',
    etl_time      DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(behavior_id)
COMMENT '用户行为明细数据表(清洗后)'
DISTRIBUTED BY HASH(user_id) BUCKETS 15
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);



INSERT INTO dwd_user_behavior_detail
SELECT
    behavior_id,
    b.user_id,  -- 明确指定来自行为表的user_id
    activity_id,
    -- 标准化行为类型
    CASE behavior_type
        WHEN 'click' THEN 'click'
        WHEN 'view' THEN 'view'
        WHEN 'share' THEN 'share'
        WHEN 'participate' THEN 'participate'
        ELSE 'other'
        END AS behavior_type,
    behavior_time,
    DATE(behavior_time) AS behavior_date,
    HOUR(behavior_time) AS behavior_hour,
    -- 判断是否周末(周六=5, 周日=6)
    CASE WHEN WEEKDAY(behavior_time) BETWEEN 5 AND 6 THEN 1 ELSE 0 END AS is_weekend,
    page,
    device,
    -- 设备类型归类
    CASE
        WHEN device IN ('android', 'ios') THEN 'mobile'
        WHEN device IN ('pc', 'mac') THEN 'pc'
        ELSE 'other'
        END AS device_type,
    ip,
    -- 根据IP解析省份(实际环境中可使用IP库解析)
    -- 这里使用城市信息推断省份
    CASE
        WHEN city IN ('北京', '上海', '天津', '重庆') THEN city
        WHEN city IN ('广州', '深圳', '珠海') THEN '广东'
        WHEN city IN ('杭州', '宁波', '温州') THEN '浙江'
        WHEN city IN ('成都', '绵阳') THEN '四川'
        WHEN city IN ('武汉', '宜昌') THEN '湖北'
        ELSE '其他'
        END AS province,
    CURRENT_TIMESTAMP AS etl_time
FROM ods_user_behavior b
-- 关联用户表获取城市信息
         LEFT JOIN ods_user u ON b.user_id = u.user_id;



select * from dwd_user_behavior_detail;







-- 营销效果明细数据表
CREATE TABLE IF NOT EXISTS dwd_marketing_effect_detail
(
    effect_id       BIGINT NOT NULL COMMENT '效果ID',
    activity_id     BIGINT COMMENT '活动ID',
    user_id         BIGINT COMMENT '用户ID',
    conversion_time DATETIME COMMENT '转化时间',
    conversion_date DATE COMMENT '转化日期',
    conversion_type VARCHAR(20) COMMENT '转化类型',
    amount          DECIMAL(15, 2) COMMENT '转化金额',
    is_new_user     TINYINT COMMENT '是否新用户: 0-否, 1-是',
    user_age_group  VARCHAR(20) COMMENT '用户年龄组',
    etl_time        DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL处理时间'
) ENGINE = OLAP DUPLICATE KEY(effect_id)
COMMENT '营销效果明细数据表(清洗后)'
DISTRIBUTED BY HASH(activity_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);











INSERT INTO dwd_marketing_effect_detail
SELECT
    effect_id,
    activity_id,
    e.user_id,
    conversion_time,
    DATE(conversion_time) AS conversion_date,
    conversion_type,
    -- 处理金额异常值
    CASE WHEN amount < 0 THEN 0 ELSE amount END AS amount,
    -- 判断是否为新用户(注册时间距转化时间小于30天)
    CASE
        WHEN DATEDIFF(e.conversion_time, u.register_time) <= 30 THEN 1
        ELSE 0
        END AS is_new_user,
    -- 获取用户年龄组
    CASE
        WHEN u.age < 25 THEN '18-24'
        WHEN u.age < 35 THEN '25-34'
        WHEN u.age < 45 THEN '35-44'
        WHEN u.age < 55 THEN '45-54'
        WHEN u.age >= 55 THEN '55+'
        ELSE NULL
        END AS user_age_group,
    CURRENT_TIMESTAMP AS etl_time
FROM ods_marketing_effect e
         LEFT JOIN ods_user u ON e.user_id = u.user_id;



select * from dwd_marketing_effect_detail;