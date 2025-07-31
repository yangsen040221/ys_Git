use gd;


CREATE TABLE IF NOT EXISTS dim_item_info (
   item_id VARCHAR(255) NOT NULL COMMENT '商品ID',
   item_name VARCHAR(255) COMMENT '商品名称',
   category_id VARCHAR(255) COMMENT '分类ID',
   category_name VARCHAR(255) COMMENT '分类名称',
   is_drainage VARCHAR(1) COMMENT '是否引流款：1-是, 0-否',
   is_hot VARCHAR(1) COMMENT '是否热销款：1-是, 0-否',
   update_time DATETIME COMMENT '维度更新时间',
   PRIMARY KEY (item_id)  -- 维度表通常以商品ID为主键
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
    COMMENT '商品维度表（工单编号：大数据-电商数仓-05-商品主题连带分析看板）';




INSERT INTO dim_item_info (
    item_id,
    item_name,
    category_id,
    category_name,
    is_drainage,
    is_hot,
    update_time
)
SELECT
    item_id,
    item_name,
    category_id,
    CONCAT('分类名称_', category_id) AS category_name,  -- MySQL用CONCAT拼接字符串，替代||
    is_drainage,
    is_hot,
    NOW() AS update_time  -- MySQL用NOW()获取当前时间，替代CURRENT_TIMESTAMP()
FROM ods_item_info
WHERE dt = '2025-07-31';  -- 筛选指定分区日期的数据

select * from dim_item_info;

