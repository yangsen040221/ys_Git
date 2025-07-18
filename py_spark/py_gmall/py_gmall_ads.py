from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("HiveIntegration") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.sql.warehouse.dir", "/home/user/hive/warehouse") \
    .enableHiveSupport() \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("info")
date = "20250717"

spark.sql("use gmall_ads;")

# 优化SQL逻辑，减少不必要的计算和数据处理

##########ads_traffic_stats_by_channel
# insert_sql = f"""
# insert overwrite table ads_traffic_stats_by_channel
# select
#     {date} as dt,
#     recent_days,
#     channel,
#     cast(count(distinct mid_id) as bigint) uv_count,
#     cast(avg(during_time_1d)/1000 as bigint) avg_duration_sec,
#     cast(avg(page_count_1d) as bigint) avg_page_count,
#     cast(count(*) as bigint) sv_count,
#     cast(sum(case when page_count_1d=1 then 1 else 0 end)/count(*) as decimal(16,2)) bounce_rate
# from gmall_dws.dws_traffic_session_page_view_1d
# lateral view explode(array(1,7,30)) tmp as recent_days
# where dt<={date}
# group by recent_days, channel
# """

###############ads_page_path


# insert_sql = f"""
# insert overwrite table ads_page_path
# select
#     {date} dt,
#     source,
#     nvl(target,'null'),
#     count(*) path_count
# from
#     (
#         select
#             concat('step-',rn,':',page_id) source,
#             concat('step-',rn+1,':',next_page_id) target
#         from
#             (
#                 select
#                     page_id,
#                     lead(page_id,1,null) over(partition by session_id order by view_time) next_page_id,
#                         row_number() over (partition by session_id order by view_time) rn
#                 from gmall_dwd.dwd_traffic_page_view_inc
#                 where dt={date}
#             )t1
#     )t2
# group by source,target;
# """


###############ads_user_change

# insert_sql =f"""
#         insert overwrite table ads_user_change
# select * from ads_user_change
# union
# select
#     churn.dt,
#     user_churn_count,
#     user_back_count
# from
#     (
#         select
#             '{date}' dt,
#             count(*) user_churn_count
#         from gmall_dws.dws_user_user_login_td
#         where dt='{date}'
#           and login_date_last=date_add('{date}',-7)
#     )churn
#         join
#     (
#         select
#             '{date}' dt,
#             count(*) user_back_count
#         from
#             (
#                 select
#                     user_id,
#                     login_date_last
#                 from gmall_dws.dws_user_user_login_td
#                 where dt='{date}'
#                   and login_date_last = '{date}'
#             )t1
#                 join
#             (
#                 select
#                     user_id,
#                     login_date_last login_date_previous
#                 from gmall_dws.dws_user_user_login_td
#                 where dt=date_add('{date}',-1)
#             )t2
#             on t1.user_id=t2.user_id
#         where datediff(login_date_last,login_date_previous)>=8
#     )back
#     on churn.dt=back.dt;
# """




###############ads_user_retention

# insert_sql=f"""
#         insert overwrite table ads_user_retention
# select * from ads_user_retention
# union
# select '{date}' dt,
#        login_date_first create_date,
#        datediff('{date}', login_date_first) retention_day,
#        sum(if(login_date_last = '{date}', 1, 0)) retention_count,
#        count(*) new_user_count,
#        cast(sum(if(login_date_last = '{date}', 1, 0)) / count(*) * 100 as decimal(16, 2)) retention_rate
# from (
#          select user_id,
#                 login_date_last,
#                 login_date_first
#          from gmall_dws.dws_user_user_login_td
#          where dt = '{date}'
#            and login_date_first >= date_add('{date}', -7)
#            and login_date_first < '{date}'
#      ) t1
# group by login_date_first;
# """

###############ads_user_stats

# insert_sql=f"""
#         insert overwrite table ads_user_stats
# select * from ads_user_stats
# union
# select '{date}' as dt,
#        recent_days,
#        sum(if(login_date_first >= date_add('{date}', -recent_days + 1), 1, 0)) new_user_count,
#        count(*) active_user_count
# from gmall_dws.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days
# where dt = '{date}'
#   and login_date_last >= date_add('{date}', -recent_days + 1)
# group by recent_days;
# """



###############ads_user_action

# insert_sql=f"""
#         insert overwrite table ads_user_action
# select
#     '{date}' dt,
#     home_count,
#     good_detail_count,
#     cart_count,
#     order_count,
#     payment_count
# from
#     (
#         select
#             1 recent_days,
#             sum(if(page_id='home',1,0)) home_count,
#             sum(if(page_id='good_detail',1,0)) good_detail_count
#         from gmall_dws.dws_traffic_page_visitor_page_view_1d
#         where dt='{date}'
#           and page_id in ('home','good_detail')
#     )page
#         join
#     (
#         select
#             1 recent_days,
#             count(*) cart_count
#         from gmall_dws.dws_trade_user_cart_add_1d
#         where dt='{date}'
#     )cart
#     on page.recent_days=cart.recent_days
#         join
#     (
#         select
#             1 recent_days,
#             count(*) order_count
#         from gmall_dws.dws_trade_user_order_1d
#         where dt='{date}'
#     )ord
#     on page.recent_days=ord.recent_days
#         join
#     (
#         select
#             1 recent_days,
#             count(*) payment_count
#         from gmall_dws.dws_trade_user_payment_1d
#         where dt='{date}'
#     )pay
#     on page.recent_days=pay.recent_days;
# """



###############ads_new_order_user_stats

# insert_sql=f"""
#         insert overwrite table ads_new_order_user_stats
# select * from ads_new_order_user_stats
# union
# select
#     '{date}' as dt,
#     recent_days,
#     count(*) new_order_user_count
# from gmall_dws.dws_trade_user_order_td lateral view explode(array(1,7,30)) tmp as recent_days
# where dt='{date}'
#   and order_date_first>=date_add('{date}',-recent_days+1)
# group by recent_days;
# """



###############ads_order_continuously_user_count

# insert_sql=f"""
#         insert overwrite table ads_order_continuously_user_count
# select * from ads_order_continuously_user_count
# union
# select
#     '{date}',
#     7,
#     count(distinct(user_id))
# from
#     (
#         select
#             user_id,
#             datediff(lead(dt,2,'99991231') over(partition by user_id order by dt),dt) diff
#         from gmall_dws.dws_trade_user_order_1d
#         where dt>=date_add('{date}',-6)
#     )t1
# where diff=2;
# """



###############ads_repeat_purchase_by_tm

# insert_sql=f"""
#         insert overwrite table ads_repeat_purchase_by_tm
# select
#     '{date}',
#     30,
#     tm_id,
#     tm_name,
#     cast(sum(if(order_count>=2,1,0))/sum(if(order_count>=1,1,0)) as decimal(16,2))
# from
#     (
#         select
#             tm_id,
#             tm_name,
#             sum(order_count_30d) order_count
#         from gmall_dws.dws_trade_user_sku_order_nd
#         where dt='{date}'
#         group by  tm_id,tm_name
#     )t1
# group by tm_id,tm_name;
# """




###############ads_order_stats_by_tm

# insert_sql=f"""
#     insert overwrite table ads_order_stats_by_tm
# select
#     '{date}' dt,
#     recent_days,
#     tm_id,
#     tm_name,
#     order_count,
#     order_user_count
# from
#     (
#         select
#             1 recent_days,
#             tm_id,
#             tm_name,
#             sum(order_count_1d) order_count,
#             count(distinct(user_id)) order_user_count
#         from gmall_dws.dws_trade_user_sku_order_1d
#         where dt='{date}'
#         group by tm_id,tm_name
#         union all
#         select
#             recent_days,
#             tm_id,
#             tm_name,
#             sum(order_count),
#             count(distinct(if(order_count>0,user_id,null)))
#         from
#             (
#                 select
#                     recent_days,
#                     user_id,
#                     tm_id,
#                     tm_name,
#                     case recent_days
#                         when 7 then order_count_7d
#                         when 30 then order_count_30d
#                         end order_count
#                 from gmall_dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
#                 where dt='{date}'
#             )t1
#         group by recent_days,tm_id,tm_name
#     )odr;
# """

###############ads_order_stats_by_cate

# insert_sql=f"""
#     insert overwrite table ads_order_stats_by_cate
# select
#     '{date}' dt,
#     recent_days,
#     category1_id,
#     category1_name,
#     category2_id,
#     category2_name,
#     category3_id,
#     category3_name,
#     order_count,
#     order_user_count
# from
#     (
#         select
#             1 recent_days,
#             category1_id,
#             category1_name,
#             category2_id,
#             category2_name,
#             category3_id,
#             category3_name,
#             sum(order_count_1d) order_count,
#             count(distinct(user_id)) order_user_count
#         from gmall_dws.dws_trade_user_sku_order_1d
#         where dt='{date}'
#         group by category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
#         union all
#         select
#             recent_days,
#             category1_id,
#             category1_name,
#             category2_id,
#             category2_name,
#             category3_id,
#             category3_name,
#             sum(order_count),
#             count(distinct(if(order_count>0,user_id,null)))
#         from
#             (
#                 select
#                     recent_days,
#                     user_id,
#                     category1_id,
#                     category1_name,
#                     category2_id,
#                     category2_name,
#                     category3_id,
#                     category3_name,
#                     case recent_days
#                         when 7 then order_count_7d
#                         when 30 then order_count_30d
#                         end order_count
#                 from gmall_dws.dws_trade_user_sku_order_nd lateral view explode(array(7,30)) tmp as recent_days
#                 where dt='{date}'
#             )t1
#         group by recent_days,category1_id,category1_name,category2_id,category2_name,category3_id,category3_name
#     )odr;
# """



##################ads_sku_cart_num_top3_by_cate

# insert_sql=f"""
#         insert overwrite table ads_sku_cart_num_top3_by_cate
# select * from ads_sku_cart_num_top3_by_cate
# union
# select
#     '{date}' dt,
#     category1_id,
#     category1_name,
#     category2_id,
#     category2_name,
#     category3_id,
#     category3_name,
#     sku_id,
#     sku_name,
#     cart_num,
#     rk
# from
#     (
#         select
#             sku_id,
#             sku_name,
#             category1_id,
#             category1_name,
#             category2_id,
#             category2_name,
#             category3_id,
#             category3_name,
#             cart_num,
#             rank() over (partition by category1_id,category2_id,category3_id order by cart_num desc) rk
#         from
#             (
#                 select
#                     sku_id,
#                     sum(sku_num) cart_num
#                 from gmall_dwd.dwd_trade_cart_full
#                 where dt='{date}'
#                 group by sku_id
#             )cart
#                 left join
#             (
#                 select
#                     id,
#                     sku_name,
#                     category1_id,
#                     category1_name,
#                     category2_id,
#                     category2_name,
#                     category3_id,
#                     category3_name
#                 from gmall_dim.dim_sku_full
#                 where dt='{date}'
#             )sku
#             on cart.sku_id=sku.id
#     )t1
# where rk<=3;
# """


#####################ads_sku_favor_count_top3_by_tm
# insert_sql=f"""
#         insert overwrite table ads_sku_favor_count_top3_by_tm
# select
#     '{date}' dt,
#     tm_id,
#     tm_name,
#     sku_id,
#     sku_name,
#     favor_add_count_1d,
#     rk
# from
#     (
#         select
#             tm_id,
#             tm_name,
#             sku_id,
#             sku_name,
#             favor_add_count_1d,
#             rank() over (partition by tm_id order by favor_add_count_1d desc) rk
#         from gmall_dws.dws_interaction_sku_favor_add_1d
#         where dt='{date}'
#     )t1
# where rk<=3;
# """




#####################ads_order_to_pay_interval_avg

# insert_sql=f"""
#         insert overwrite table ads_order_to_pay_interval_avg
# select
#     '{date}',
#     cast(avg(to_unix_timestamp(payment_time)-to_unix_timestamp(order_time)) as bigint)
# from gmall_dwd.dwd_trade_trade_flow_acc
# where dt in ('99991231','{date}')
#   and payment_date_id='{date}';
# """



#####################ads_order_by_province

# insert_sql=f"""
#     insert overwrite table ads_order_by_province
# select * from ads_order_by_province
# union
# select
#     '{date}' as dt,
#     1 recent_days,
#     province_id,
#     province_name,
#     area_code,
#     iso_code,
#     iso_3166_2,
#     order_count_1d,
#     order_total_amount_1d
# from gmall_dws.dws_trade_province_order_1d
# where dt='{date}'
# union
# select
#     '{date}' as dt,
#     recent_days,
#     province_id,
#     province_name,
#     area_code,
#     iso_code,
#     iso_3166_2,
#     case recent_days
#         when 7 then order_count_7d
#         when 30 then order_count_30d
#         end order_count,
#     case recent_days
#         when 7 then order_total_amount_7d
#         when 30 then order_total_amount_30d
#         end order_total_amount
# from gmall_dws.dws_trade_province_order_nd lateral view explode(array(7,30)) tmp as recent_days
# where dt='{date}';
# """



#####################ads_coupon_stats

insert_sql=f"""
        insert overwrite table ads_coupon_stats
select
    '20250717' as dt,
    coupon_id,
    coupon_name,
    cast(sum(used_count_1d) as bigint),
    cast(count(*) as bigint)
from gmall_dws.dws_tool_user_coupon_coupon_used_1d
where dt='20250717'
group by coupon_id,coupon_name;
    """



# 执行插入操作（移除show()，避免额外数据收集和展示的资源消耗）
spark.sql(insert_sql).show()

# 显式关闭SparkSession，释放资源
spark.stop()