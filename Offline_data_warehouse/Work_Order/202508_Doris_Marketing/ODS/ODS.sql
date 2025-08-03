CREATE DATABASE IF NOT EXISTS gd;
use gd;

-- 用户表
CREATE TABLE IF NOT EXISTS ods_user
(
    user_id       BIGINT NOT NULL COMMENT '用户ID',
    user_name     VARCHAR(50) COMMENT '用户名',
    gender        TINYINT COMMENT '性别: 1-男, 2-女, 0-未知',
    age           INT COMMENT '年龄',
    city          VARCHAR(50) COMMENT '城市',
    register_time DATETIME COMMENT '注册时间',
    channel       VARCHAR(20) COMMENT '注册渠道',
    mobile        VARCHAR(20) COMMENT '手机号',
    email         VARCHAR(100) COMMENT '邮箱',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间'
) ENGINE = OLAP DUPLICATE KEY(user_id)
COMMENT '用户原始数据表'
DISTRIBUTED BY HASH(user_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);


-- 营销活动表
CREATE TABLE IF NOT EXISTS ods_marketing_activity
(
    activity_id   BIGINT NOT NULL COMMENT '活动ID',
    activity_name VARCHAR(100) COMMENT '活动名称',
    activity_type VARCHAR(20) COMMENT '活动类型',
    start_time    DATETIME COMMENT '开始时间',
    end_time      DATETIME COMMENT '结束时间',
    status        TINYINT COMMENT '状态: 0-未开始, 1-进行中, 2-已结束',
    channel       VARCHAR(50) COMMENT '活动渠道',
    budget        DECIMAL(15, 2) COMMENT '活动预算',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间'
) ENGINE = OLAP DUPLICATE KEY(activity_id)
COMMENT '营销活动原始数据表'
DISTRIBUTED BY HASH(activity_id) BUCKETS 5
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);



-- 用户行为表
CREATE TABLE IF NOT EXISTS ods_user_behavior
(
    behavior_id   BIGINT NOT NULL COMMENT '行为ID',
    user_id       BIGINT COMMENT '用户ID',
    activity_id   BIGINT COMMENT '活动ID',
    behavior_type VARCHAR(20) COMMENT '行为类型: click-点击, view-浏览, share-分享, participate-参与',
    behavior_time DATETIME COMMENT '行为时间',
    page          VARCHAR(50) COMMENT '页面',
    device        VARCHAR(50) COMMENT '设备',
    ip            VARCHAR(20) COMMENT 'IP地址',
    create_time   DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间'
) ENGINE = OLAP DUPLICATE KEY(behavior_id)
COMMENT '用户行为原始数据表'
DISTRIBUTED BY HASH(user_id) BUCKETS 15
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);



-- 营销效果表
CREATE TABLE IF NOT EXISTS ods_marketing_effect
(
    effect_id       BIGINT NOT NULL COMMENT '效果ID',
    activity_id     BIGINT COMMENT '活动ID',
    user_id         BIGINT COMMENT '用户ID',
    conversion_time DATETIME COMMENT '转化时间',
    conversion_type VARCHAR(20) COMMENT '转化类型',
    amount          DECIMAL(15, 2) COMMENT '转化金额',
    create_time     DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '数据创建时间'
) ENGINE = OLAP DUPLICATE KEY(effect_id)
COMMENT '营销效果原始数据表'
DISTRIBUTED BY HASH(activity_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "1",
    "in_memory" = "false",
    "storage_format" = "V2"
);



select * from ods_marketing_activity;
select * from ods_marketing_effect;
select * from ods_user;
select * from ods_user_behavior;