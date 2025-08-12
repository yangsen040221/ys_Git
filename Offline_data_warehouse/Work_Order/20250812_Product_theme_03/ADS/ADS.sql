use sx_one_3_03;

-- 启用动态分区，允许所有分区列都是动态的
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

--------------------------------------------------------------------------------
-- 1. SKU 热销榜 - 统计每个商品的SKU销售排名
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_sku_rank
(
    dt         STRING COMMENT '统计日期',
    product_id STRING COMMENT '商品ID',
    sku_id     STRING COMMENT 'SKU ID',
    sales_qty  BIGINT COMMENT '销售数量',
    gmv        DECIMAL(18, 2) COMMENT '商品交易总额',
    rank       INT COMMENT '销售排名'
)
    STORED AS PARQUET;

-- 插入SKU热销榜数据，按商品ID和日期分组，根据销售数量进行排名
INSERT OVERWRITE TABLE ads_product_sku_rank
SELECT dt,
       product_id,
       sku_id,
       sales_qty,
       gmv,
       RANK() OVER (PARTITION BY dt, product_id ORDER BY sales_qty DESC) AS rank
FROM dws_product_sku_attr_summary;

--------------------------------------------------------------------------------
-- 2. 价格趋势 - 分析商品价格变化趋势，包括同比和环比
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_price_trend
(
    dt         STRING COMMENT '统计日期',
    product_id STRING COMMENT '商品ID',
    avg_price  DECIMAL(10, 2) COMMENT '平均价格',
    min_price  DECIMAL(10, 2) COMMENT '最低价格',
    max_price  DECIMAL(10, 2) COMMENT '最高价格',
    yoy_change DECIMAL(10, 4) COMMENT '同比变化率(与去年同日比较)',
    mom_change DECIMAL(10, 4) COMMENT '环比变化率(与昨日比较)'
)
    STORED AS PARQUET;

-- 计算价格趋势数据，包括与去年同期和昨日的价格比较
INSERT OVERWRITE TABLE ads_product_price_trend
SELECT a.dt,
       a.product_id,
       a.avg_price,
       a.min_price,
       a.max_price,
       CASE
           WHEN b.avg_price IS NOT NULL AND b.avg_price != 0
               THEN ROUND((a.avg_price - b.avg_price) / b.avg_price, 4)
           ELSE NULL END AS yoy_change,  -- 同比变化率 = (当前价格-去年同期价格)/去年同期价格
       CASE
           WHEN c.avg_price IS NOT NULL AND c.avg_price != 0
               THEN ROUND((a.avg_price - c.avg_price) / c.avg_price, 4)
           ELSE NULL END AS mom_change   -- 环比变化率 = (当前价格-昨日价格)/昨日价格
FROM dws_product_price_summary a
         LEFT JOIN dws_product_price_summary b
                   ON a.product_id = b.product_id
                       AND date_sub(a.dt, 365) = b.dt  -- 关联去年同期数据
         LEFT JOIN dws_product_price_summary c
                   ON a.product_id = c.product_id
                       AND date_sub(a.dt, 1) = c.dt;   -- 关联昨日数据

--------------------------------------------------------------------------------
-- 3. 渠道来源明细 - 分析不同渠道的流量和转化情况
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_channel_analysis
(
    dt                STRING COMMENT '统计日期',
    product_id        STRING COMMENT '商品ID',
    channel           STRING COMMENT '渠道名称',
    uv                BIGINT COMMENT '独立访客数',
    pv                BIGINT COMMENT '页面浏览量',
    channel_conv_rate DECIMAL(10, 4) COMMENT '渠道转化率'
)
    STORED AS PARQUET;

-- 插入各渠道分析数据
INSERT OVERWRITE TABLE ads_product_channel_analysis
SELECT dt,
       product_id,
       channel,
       uv,
       pv,
       channel_conv_rate
FROM dws_product_channel_summary;

--------------------------------------------------------------------------------
-- 4. 评价分析 - 统计商品评价相关指标
--------------------------------------------------------------------------------
    CREATE TABLE IF NOT EXISTS ads_product_review_analysis
(
    dt               STRING COMMENT '统计日期',
    product_id       STRING COMMENT '商品ID',
    total_reviews    BIGINT COMMENT '总评价数',
    positive_reviews BIGINT COMMENT '正面评价数',
    negative_reviews BIGINT COMMENT '负面评价数',
    avg_rating       DECIMAL(10, 2) COMMENT '平均评分',
    positive_rate    DECIMAL(10, 4) COMMENT '好评率'
)
    STORED AS PARQUET;

-- 插入商品评价分析数据
INSERT OVERWRITE TABLE ads_product_review_analysis
SELECT dt,
       product_id,
       total_reviews,
       positive_reviews,
       negative_reviews,
       avg_rating,
       positive_rate
FROM dws_product_review_summary;

--------------------------------------------------------------------------------
-- 5. 用户画像分析 - 分析商品受众的用户画像
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_audience_profile
(
    dt         STRING COMMENT '统计日期',
    product_id STRING COMMENT '商品ID',
    gender     STRING COMMENT '性别',
    age_group  STRING COMMENT '年龄组',
    location   STRING COMMENT '地理位置',
    uv         BIGINT COMMENT '独立访客数'
)
    STORED AS PARQUET;

-- 插入用户画像数据
INSERT OVERWRITE TABLE ads_product_audience_profile
SELECT dt,
       product_id,
       gender,
       age_group,
       location,
       uv
FROM dws_product_audience_summary;


-- 创建 ADS 层商品360度宽表 - 整合商品所有维度的指标
    CREATE TABLE IF NOT EXISTS ads_product_360
(
    dt                  STRING COMMENT '统计日期',
    product_id          STRING COMMENT '商品ID',
    product_name        STRING COMMENT '商品名称',
    category_id         STRING COMMENT '类目ID',
    category_name       STRING COMMENT '类目名称',
    sales_qty           BIGINT COMMENT '销售数量',
    gmv                 DECIMAL(18, 2) COMMENT '商品交易总额',
    uv                  BIGINT COMMENT '独立访客数',
    pv                  BIGINT COMMENT '页面浏览量',
    conversion_rate     DECIMAL(10, 4) COMMENT '整体转化率',

    -- 价格信息
    avg_price           DECIMAL(10, 2) COMMENT '平均价格',
    min_price           DECIMAL(10, 2) COMMENT '最低价格',
    max_price           DECIMAL(10, 2) COMMENT '最高价格',

    -- 渠道分析（示例：搜索、推荐、直通车、活动）
    search_uv           BIGINT COMMENT '搜索渠道访客数',
    search_conv_rate    DECIMAL(10, 4) COMMENT '搜索渠道转化率',
    recommend_uv        BIGINT COMMENT '推荐渠道访客数',
    recommend_conv_rate DECIMAL(10, 4) COMMENT '推荐渠道转化率',
    zhituche_uv         BIGINT COMMENT '直通车渠道访客数',
    zhituche_conv_rate  DECIMAL(10, 4) COMMENT '直通车渠道转化率',
    activity_uv         BIGINT COMMENT '活动渠道访客数',
    activity_conv_rate  DECIMAL(10, 4) COMMENT '活动渠道转化率',

    -- 评价指标
    total_reviews       BIGINT COMMENT '总评价数',
    positive_reviews    BIGINT COMMENT '正面评价数',
    negative_reviews    BIGINT COMMENT '负面评价数',
    avg_rating          DECIMAL(10, 2) COMMENT '平均评分',
    positive_rate       DECIMAL(10, 4) COMMENT '好评率'
)
    STORED AS PARQUET;

-- 构建商品360度宽表，整合各维度数据
INSERT OVERWRITE TABLE ads_product_360
SELECT d.dt,
       d.product_id,
       d.product_name,
       d.category_id,
       d.category_name,
       d.sales_qty,
       d.gmv,
       d.uv,
       d.pv,
       d.conversion_rate,

       p.avg_price,
       p.min_price,
       p.max_price,

       -- 按渠道类型提取UV和转化率数据
       MAX(CASE WHEN c.channel = '搜索' THEN c.uv ELSE 0 END)                  AS search_uv,
       MAX(CASE WHEN c.channel = '搜索' THEN c.channel_conv_rate ELSE 0 END)   AS search_conv_rate,

       MAX(CASE WHEN c.channel = '推荐' THEN c.uv ELSE 0 END)                  AS recommend_uv,
       MAX(CASE WHEN c.channel = '推荐' THEN c.channel_conv_rate ELSE 0 END)   AS recommend_conv_rate,

       MAX(CASE WHEN c.channel = '直通车' THEN c.uv ELSE 0 END)                AS zhituche_uv,
       MAX(CASE WHEN c.channel = '直通车' THEN c.channel_conv_rate ELSE 0 END) AS zhituche_conv_rate,

       MAX(CASE WHEN c.channel = '活动' THEN c.uv ELSE 0 END)                  AS activity_uv,
       MAX(CASE WHEN c.channel = '活动' THEN c.channel_conv_rate ELSE 0 END)   AS activity_conv_rate,

       r.total_reviews,
       r.positive_reviews,
       r.negative_reviews,
       r.avg_rating,
       r.positive_rate
FROM dws_product_day_summary d
         LEFT JOIN dws_product_price_summary p
                   ON d.dt = p.dt AND d.product_id = p.product_id
         LEFT JOIN dws_product_channel_summary c
                   ON d.dt = c.dt AND d.product_id = c.product_id
         LEFT JOIN dws_product_review_summary r
                   ON d.dt = r.dt AND d.product_id = r.product_id
GROUP BY d.dt,
         d.product_id,
         d.product_name,
         d.category_id,
         d.category_name,
         d.sales_qty,
         d.gmv,
         d.uv,
         d.pv,
         d.conversion_rate,
         p.avg_price,
         p.min_price,
         p.max_price,
         r.total_reviews,
         r.positive_reviews,
         r.negative_reviews,
         r.avg_rating,
         r.positive_rate;