use gd;

# 记录用户对两个商品的同时行为（访问 / 收藏 / 加购 / 支付）
drop table if exists dwd_user_same_behavior_factl;
CREATE TABLE IF NOT EXISTS dwd_user_same_behavior_fact (
    user_id VARCHAR(255) COMMENT '用户ID',
    main_item_id VARCHAR(255) COMMENT '主商品ID',
    related_item_id VARCHAR(255) COMMENT '关联商品ID',  -- 建议后续优化为英文命名如related_item_id
    behavior_type VARCHAR(20) COMMENT '行为类型：visit-访问, collect-收藏, cart-加购, pay-支付',
    behavior_time DATETIME COMMENT '行为时间',
    dt VARCHAR(10) COMMENT '分区日期（格式：YYYY-MM-DD）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT '用户同时行为事实表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';



INSERT INTO dwd_user_same_behavior_fact (
    user_id,
    main_item_id,
    related_item_id,
    behavior_type,
    behavior_time,
    dt
)
SELECT
    a.user_id,
    a.item_id AS main_item_id,
    b.item_id AS related_item_id,
    'visit' AS behavior_type,
    a.behavior_time AS behavior_time,
    '2025-07-31' AS dt
    FROM ods_user_behavior_log a
         JOIN ods_user_behavior_log b
              ON a.user_id = b.user_id
                  AND a.behavior_type = 'visit'
                  AND b.behavior_type = 'visit'
                  AND a.item_id < b.item_id  -- 避免重复关联
                  AND TIMESTAMPDIFF(SECOND, b.behavior_time, a.behavior_time) BETWEEN -3600 AND 3600  -- 1小时内视为同时
WHERE a.dt = '2025-07-31' AND b.dt = '2025-07-31';

select * from dwd_user_same_behavior_fact;



# 记录从主商品详情页引导至关联商品的行为

CREATE TABLE IF NOT EXISTS dwd_page_guide_fact (
   from_item_id VARCHAR(255) COMMENT '主商品ID',
   to_item_id VARCHAR(255) COMMENT '关联商品ID',
   user_id VARCHAR(255) COMMENT '用户ID',
   jump_time DATETIME COMMENT '引导时间',
   dt VARCHAR(10) COMMENT '分区日期（格式：YYYY-MM-DD）'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT '商品详情页引导事实表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';

INSERT INTO dwd_page_guide_fact (
    from_item_id,
    to_item_id,
    user_id,
    jump_time,
    dt
)
SELECT
    from_item_id,
    to_item_id,
    user_id,
    jump_time,
    '2025-07-31' AS dt
FROM ods_page_jump_log
WHERE dt = '2025-07-31';






select * from dwd_page_guide_fact;