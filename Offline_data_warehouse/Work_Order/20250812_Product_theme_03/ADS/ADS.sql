use sx_one_3_03;

-- 启用动态分区
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

--------------------------------------------------------------------------------
-- 1. SKU 热销榜
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_sku_rank (
                                                    dt              STRING,
                                                    product_id      STRING,
                                                    sku_id          STRING,
                                                    sales_qty       BIGINT,
                                                    gmv             DECIMAL(18,2),
    rank            INT
    )
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_sku_rank
SELECT
    dt,
    product_id,
    sku_id,
    sales_qty,
    gmv,
    RANK() OVER (PARTITION BY dt, product_id ORDER BY sales_qty DESC) AS rank
FROM dws_product_sku_attr_summary;

--------------------------------------------------------------------------------
-- 2. 价格趋势
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_price_trend (
                                                       dt              STRING,
                                                       product_id      STRING,
                                                       avg_price       DECIMAL(10,2),
    min_price       DECIMAL(10,2),
    max_price       DECIMAL(10,2),
    yoy_change      DECIMAL(10,4), -- 同比变化率
    mom_change      DECIMAL(10,4)  -- 环比变化率
    )
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_price_trend
SELECT
    a.dt,
    a.product_id,
    a.avg_price,
    a.min_price,
    a.max_price,
    CASE
        WHEN b.avg_price IS NOT NULL AND b.avg_price != 0
        THEN ROUND((a.avg_price - b.avg_price) / b.avg_price, 4)
        ELSE NULL END AS yoy_change,
    CASE
        WHEN c.avg_price IS NOT NULL AND c.avg_price != 0
        THEN ROUND((a.avg_price - c.avg_price) / c.avg_price, 4)
        ELSE NULL END AS mom_change
FROM dws_product_price_summary a
         LEFT JOIN dws_product_price_summary b
                   ON a.product_id = b.product_id
                       AND date_sub(a.dt, 365) = b.dt
         LEFT JOIN dws_product_price_summary c
                   ON a.product_id = c.product_id
                       AND date_sub(a.dt, 1) = c.dt;

--------------------------------------------------------------------------------
-- 3. 渠道来源明细
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_channel_analysis (
                                                            dt              STRING,
                                                            product_id      STRING,
                                                            channel         STRING,
                                                            uv              BIGINT,
                                                            pv              BIGINT,
                                                            channel_conv_rate DECIMAL(10,4)
    )
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_channel_analysis
SELECT
    dt,
    product_id,
    channel,
    uv,
    pv,
    channel_conv_rate
FROM dws_product_channel_summary;

--------------------------------------------------------------------------------
-- 4. 评价分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_review_analysis (
                                                           dt                  STRING,
                                                           product_id          STRING,
                                                           total_reviews       BIGINT,
                                                           positive_reviews    BIGINT,
                                                           negative_reviews    BIGINT,
                                                           avg_rating          DECIMAL(10,2),
    positive_rate       DECIMAL(10,4)
    )
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_review_analysis
SELECT
    dt,
    product_id,
    total_reviews,
    positive_reviews,
    negative_reviews,
    avg_rating,
    positive_rate
FROM dws_product_review_summary;

--------------------------------------------------------------------------------
-- 5. 用户画像分析
--------------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ads_product_audience_profile (
                                                            dt              STRING,
                                                            product_id      STRING,
                                                            gender          STRING,
                                                            age_group       STRING,
                                                            location        STRING,
                                                            uv              BIGINT
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_audience_profile
SELECT
    dt,
    product_id,
    gender,
    age_group,
    location,
    uv
FROM dws_product_audience_summary;




-- 创建 ADS 层商品360宽表
CREATE TABLE IF NOT EXISTS ads_product_360 (
                                               dt                  STRING,
                                               product_id          STRING,
                                               product_name        STRING,
                                               category_id         STRING,
                                               category_name       STRING,
                                               sales_qty           BIGINT,
                                               gmv                 DECIMAL(18,2),
                                               uv                  BIGINT,
                                               pv                  BIGINT,
                                               conversion_rate     DECIMAL(10,4),

    -- 价格信息
                                               avg_price           DECIMAL(10,2),
                                               min_price           DECIMAL(10,2),
                                               max_price           DECIMAL(10,2),

    -- 渠道分析（示例：搜索、推荐、直通车、活动）
                                               search_uv           BIGINT,
                                               search_conv_rate    DECIMAL(10,4),
                                               recommend_uv        BIGINT,
                                               recommend_conv_rate DECIMAL(10,4),
                                               zhituche_uv         BIGINT,
                                               zhituche_conv_rate  DECIMAL(10,4),
                                               activity_uv         BIGINT,
                                               activity_conv_rate  DECIMAL(10,4),

    -- 评价指标
                                               total_reviews       BIGINT,
                                               positive_reviews    BIGINT,
                                               negative_reviews    BIGINT,
                                               avg_rating          DECIMAL(10,2),
                                               positive_rate       DECIMAL(10,4)
)
    STORED AS PARQUET;

INSERT OVERWRITE TABLE ads_product_360
SELECT
    d.dt,
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

    -- 渠道 UV + 转化率
    MAX(CASE WHEN c.channel = '搜索' THEN c.uv ELSE 0 END) AS search_uv,
    MAX(CASE WHEN c.channel = '搜索' THEN c.channel_conv_rate ELSE 0 END) AS search_conv_rate,

    MAX(CASE WHEN c.channel = '推荐' THEN c.uv ELSE 0 END) AS recommend_uv,
    MAX(CASE WHEN c.channel = '推荐' THEN c.channel_conv_rate ELSE 0 END) AS recommend_conv_rate,

    MAX(CASE WHEN c.channel = '直通车' THEN c.uv ELSE 0 END) AS zhituche_uv,
    MAX(CASE WHEN c.channel = '直通车' THEN c.channel_conv_rate ELSE 0 END) AS zhituche_conv_rate,

    MAX(CASE WHEN c.channel = '活动' THEN c.uv ELSE 0 END) AS activity_uv,
    MAX(CASE WHEN c.channel = '活动' THEN c.channel_conv_rate ELSE 0 END) AS activity_conv_rate,

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
GROUP BY
    d.dt,
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
