from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("py_tms_ads") \
    .master("local[*]") \
    .config("hive.metastore.uris", "thrift://cdh01:9083") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://cdh01:8020") \
    .enableHiveSupport() \
    .getOrCreate()

# 导入 Java 类并获取 HDFS 文件系统实例
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")

# 获取HDFS文件系统实例，用于后续HDFS操作
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

# 定义日期变量
dt = '20250718'

def create_hdfs_dir(path):
    jvm_path = spark.sparkContext._jvm.Path(path)  # 创建Hadoop Path对象
    if not fs.exists(jvm_path):  # 检查目录是否存在
        fs.mkdirs(jvm_path)  # 创建目录
        print(f"HDFS目录创建成功：{path}")
    else:
        print(f"HDFS目录已存在：{path}")

# 新增：创建Hive表的函数
def create_hive_table(table_name):
    table_schemas = {
        "ads_trans_order_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_trans_order_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
                `receive_order_count` bigint COMMENT '接单总数',
                `receive_order_amount` decimal(16,2) COMMENT '接单金额',
                `dispatch_order_count` bigint COMMENT '发单总数',
                `dispatch_order_amount` decimal(16,2) COMMENT '发单金额'
            ) COMMENT '运单相关统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_trans_order_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_trans_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_trans_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒'
            ) COMMENT '运输综合统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_trans_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_trans_order_stats_td": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_trans_order_stats_td(
                `dt` string COMMENT '统计日期',
                `bounding_order_count` bigint COMMENT '运输中运单总数',
                `bounding_order_amount` decimal(16,2) COMMENT '运输中运单金额'
            ) COMMENT '历史至今运单统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_trans_order_stats_td'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_order_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_order_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `order_count` bigint COMMENT '下单数',
                `order_amount` decimal(16,2) COMMENT '下单金额'
            ) COMMENT '运单综合统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_order_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_order_cargo_type_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_order_cargo_type_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `cargo_type` string COMMENT '货物类型',
                `cargo_type_name` string COMMENT '货物类型名称',
                `order_count` bigint COMMENT '下单数',
                `order_amount` decimal(16,2) COMMENT '下单金额'
            ) COMMENT '各类型货物运单统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_order_cargo_type_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_city_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_city_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` bigint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `order_count` bigint COMMENT '下单数',
                `order_amount` decimal(16,2) COMMENT '下单金额',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
                `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
            ) COMMENT '城市分析'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_city_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_org_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_org_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近1天,7:最近7天,30:最近30天',
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '机构名称',
                `order_count` bigint COMMENT '下单数',
                `order_amount` decimal(16,2) COMMENT '下单金额',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
                `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
            ) COMMENT '机构分析'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_org_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_shift_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_shift_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
                `shift_id` bigint COMMENT '班次ID',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                `trans_finish_order_count` bigint COMMENT '运输完成运单数'
            ) COMMENT '班次分析'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_shift_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_line_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_line_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
                `line_id` bigint COMMENT '线路ID',
                `line_name` string COMMENT '线路名称',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                `trans_finish_order_count` bigint COMMENT '运输完成运单数'
            ) COMMENT '线路分析'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_line_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_driver_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_driver_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,7:最近7天,30:最近30天',
                `driver_emp_id` bigint COMMENT '第一司机员工ID',
                `driver_name` string COMMENT '第一司机姓名',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
                `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒',
                `trans_finish_late_count` bigint COMMENT '逾期次数'
            ) COMMENT '司机分析'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_driver_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_truck_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_truck_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `truck_model_type` string COMMENT '卡车类别编码',
                `truck_model_type_name` string COMMENT '卡车类别名称',
                `trans_finish_count` bigint COMMENT '完成运输次数',
                `trans_finish_distance` decimal(16,2) COMMENT '完成运输里程',
                `trans_finish_dur_sec` bigint COMMENT '完成运输时长，单位：秒',
                `avg_trans_finish_distance` decimal(16,2) COMMENT '平均每次运输里程',
                `avg_trans_finish_dur_sec` bigint COMMENT '平均每次运输时长，单位：秒'
            ) COMMENT '卡车分析'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_truck_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_express_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_express_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `deliver_suc_count` bigint COMMENT '派送成功次数（订单数）',
                `sort_count` bigint COMMENT '分拣次数'
            ) COMMENT '快递综合统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_express_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_express_province_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_express_province_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `receive_order_count` bigint COMMENT '揽收次数',
                `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
                `deliver_suc_count` bigint COMMENT '派送成功次数',
                `sort_count` bigint COMMENT '分拣次数'
            ) COMMENT '各省份快递统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_express_province_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_express_city_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_express_city_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `receive_order_count` bigint COMMENT '揽收次数',
                `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
                `deliver_suc_count` bigint COMMENT '派送成功次数',
                `sort_count` bigint COMMENT '分拣次数'
            ) COMMENT '各城市快递统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_express_city_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "ads_express_org_stats": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_ads.ads_express_org_stats(
                `dt` string COMMENT '统计日期',
                `recent_days` tinyint COMMENT '最近天数,1:最近天数,1:最近1天,7:最近7天,30:最近30天',
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '机构名称',
                `receive_order_count` bigint COMMENT '揽收次数',
                `receive_order_amount` decimal(16,2) COMMENT '揽收金额',
                `deliver_suc_count` bigint COMMENT '派送成功次数',
                `sort_count` bigint COMMENT '分拣次数'
            ) COMMENT '各机构快递统计'
            STORED AS ORC
            LOCATION '/warehouse/py_tms/ads/ads_express_org_stats'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """
    }
    
    if table_name in table_schemas:
        spark.sql("DROP TABLE IF EXISTS py_tms_ads." + table_name)
        spark.sql(table_schemas[table_name])
        print(f"创建Hive表完成：py_tms_ads.{table_name}")
    else:
        print(f"未找到表 {table_name} 的定义")

# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== 1. 运单相关统计 ======================
def process_ads_trans_order_stats():
    print("开始处理运单相关统计 ads_trans_order_stats")
    create_hive_table("ads_trans_order_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_trans_order_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_trans_order_stats").select(
        "dt", "recent_days", "receive_order_count", "receive_order_amount",
        "dispatch_order_count", "dispatch_order_amount"
    )
    
    # 处理1日数据
    receive_1d = spark.table("py_tms_dws.dws_trans_org_receive_1d").filter(
        F.col("dt") == dt
    ).agg(
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("receive_order_count"),
        F.sum("order_amount").alias("receive_order_amount")
    )
    
    dispatch_1d = spark.table("py_tms_dws.dws_trans_dispatch_1d").filter(
        F.col("dt") == dt
    ).select(
        F.lit(1).alias("recent_days"),
        F.col("order_count").alias("dispatch_order_count"),
        F.col("order_amount").alias("dispatch_order_amount")
    )
    
    # 关联1日数据
    combined_1d = receive_1d.join(
        dispatch_1d,
        "recent_days",
        "outer"
    ).select(
        F.lit(dt).alias("dt"),
        "recent_days",
        F.coalesce("receive_order_count", F.lit(0)).alias("receive_order_count"),
        F.coalesce("receive_order_amount", F.lit(0)).alias("receive_order_amount"),
        F.coalesce("dispatch_order_count", F.lit(0)).alias("dispatch_order_count"),  # 正确列名
        F.coalesce("dispatch_order_amount", F.lit(0)).alias("dispatch_order_amount")  # 正确列名
    )
    
    # 处理n日数据
    receive_nd = spark.table("py_tms_dws.dws_trans_org_receive_nd").filter(
        F.col("dt") == dt
    ).groupBy("recent_days").agg(
        F.sum("order_count").alias("receive_order_count"),
        F.sum("order_amount").alias("receive_order_amount")
    )
    
    dispatch_nd = spark.table("py_tms_dws.dws_trans_dispatch_nd").filter(
        F.col("dt") == dt
    ).select(
        "recent_days",
        "order_count",
        "order_amount"
    )
    
    # 关联n日数据
    combined_nd = receive_nd.join(
        dispatch_nd,
        "recent_days",
        "outer"
    ).select(
        F.lit(dt).alias("dt"),
        "recent_days",
        F.coalesce("receive_order_count", F.lit(0)).alias("receive_order_count"),
        F.coalesce("receive_order_amount", F.lit(0)).alias("receive_order_amount"),
        F.coalesce("order_count", F.lit(0)).alias("dispatch_order_count"),  # 正确列名
        F.coalesce("order_amount", F.lit(0)).alias("dispatch_order_amount")  # 正确列名
    )
    
    # 合并所有数据
    result = history_data.unionByName(combined_1d).unionByName(combined_nd)
    
    # 验证数据量
    print_data_count(result, "ads_trans_order_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_trans_order_stats")

    print("运单相关统计 ads_trans_order_stats 处理完成\n")

# ====================== 2. 运输综合统计 ======================
def process_ads_trans_stats():
    print("开始处理运输综合统计 ads_trans_stats")
    create_hive_table("ads_trans_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_trans_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_trans_stats").select(
        "dt", "recent_days", "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec"
    )
    
    # 处理1日数据
    trans_1d = spark.table("py_tms_dws.dws_trans_org_truck_model_type_trans_finish_1d").filter(
        F.col("dt") == dt
    ).agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    )
    
    # 处理n日数据
    trans_nd = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).groupBy("recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec")
    )
    
    # 合并所有数据
    result = history_data.unionByName(trans_1d, allowMissingColumns=True).unionByName(trans_nd, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_trans_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_trans_stats")

    print("运输综合统计 ads_trans_stats 处理完成\n")

# ====================== 3. 历史至今运单统计 ======================
def process_ads_trans_order_stats_td():
    print("开始处理历史至今运单统计 ads_trans_order_stats_td")
    create_hive_table("ads_trans_order_stats_td")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_trans_order_stats_td")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_trans_order_stats_td").select(
        "dt", "bounding_order_count", "bounding_order_amount"
    )
    
    # 处理当日数据
    dispatch_td = spark.table("py_tms_dws.dws_trans_dispatch_td").filter(
        F.col("dt") == dt
    ).select(
        F.col("dt"),
        F.col("order_count").alias("bounding_order_count"),
        F.col("order_amount").alias("bounding_order_amount")
    )
    
    bound_finish_td = spark.table("py_tms_dws.dws_trans_bound_finish_td").filter(
        F.col("dt") == dt
    ).select(
        F.col("dt"),
        (F.col("order_count") * -1).alias("bounding_order_count"),
        (F.col("order_amount") * -1).alias("bounding_order_amount")
    )
    
    # 合并当日数据
    current_data = dispatch_td.unionByName(bound_finish_td).groupBy("dt").agg(
        F.sum("bounding_order_count").alias("bounding_order_count"),
        F.sum("bounding_order_amount").alias("bounding_order_amount")
    )
    
    # 合并所有数据
    result = history_data.unionByName(current_data, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_trans_order_stats_td")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_trans_order_stats_td")

    print("历史至今运单统计 ads_trans_order_stats_td 处理完成\n")

# ====================== 4. 运单综合统计 ======================
def process_ads_order_stats():
    print("开始处理运单综合统计 ads_order_stats")
    create_hive_table("ads_order_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_order_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_order_stats").select(
        "dt", "recent_days", "order_count", "order_amount"
    )
    
    # 处理1日数据
    order_1d = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_1d").filter(
        F.col("dt") == dt
    ).agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 处理n日数据
    order_nd = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_nd").filter(
        F.col("dt") == dt
    ).groupBy("recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 合并所有数据
    result = history_data.unionByName(order_1d, allowMissingColumns=True).unionByName(order_nd, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_order_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_order_stats")

    print("运单综合统计 ads_order_stats 处理完成\n")

# ====================== 5. 各类型货物运单统计 ======================
def process_ads_order_cargo_type_stats():
    print("开始处理各类型货物运单统计 ads_order_cargo_type_stats")
    create_hive_table("ads_order_cargo_type_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_order_cargo_type_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_order_cargo_type_stats").select(
        "dt", "recent_days", "cargo_type", "cargo_type_name", "order_count", "order_amount"
    )
    
    # 处理1日数据
    cargo_type_1d = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_1d").filter(
        F.col("dt") == dt
    ).groupBy("cargo_type", "cargo_type_name").agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 处理n日数据
    cargo_type_nd = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_nd").filter(
        F.col("dt") == dt
    ).groupBy("cargo_type", "cargo_type_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 合并所有数据
    result = history_data.unionByName(cargo_type_1d, allowMissingColumns=True).unionByName(cargo_type_nd, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_order_cargo_type_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_order_cargo_type_stats")

    print("各类型货物运单统计 ads_order_cargo_type_stats 处理完成\n")

# ====================== 6. 城市分析 ======================
def process_ads_city_stats():
    print("开始处理城市分析 ads_city_stats")
    create_hive_table("ads_city_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_city_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_city_stats").select(
        "dt", "recent_days", "city_id", "city_name", "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    )
    
    # 处理1日订单数据
    city_order_1d = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_1d").filter(
        F.col("dt") == dt
    ).groupBy("city_id", "city_name").agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 处理1日运输数据
    trans_1d_origin = spark.table("py_tms_dws.dws_trans_org_truck_model_type_trans_finish_1d").filter(
        F.col("dt") == dt
    ).select(
        "org_id",
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec"
    )
    
    organ = spark.table("tms_dim.dim_organ_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("org_id"),
        "org_level",
        "region_id"
    )
    
    city_for_level1 = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("city1_id"),
        F.col("name").alias("city1_name"),
        "parent_id"
    )
    
    city_for_level2 = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("city2_id"),
        F.col("name").alias("city2_name")
    )
    
    # 关联处理
    trans_1d_joined = trans_1d_origin.join(
        organ,
        "org_id",
        "left"
    ).join(
        city_for_level1,
        organ.region_id == city_for_level1.city1_id,
        "left"
    ).join(
        city_for_level2,
        city_for_level1.parent_id == city_for_level2.city2_id,
        "left"
    )
    
    # 根据机构等级选择城市
    trans_1d = trans_1d_joined.select(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.when(F.col("org_level") == 1, F.col("city1_id"))
         .otherwise(F.col("city2_id"))
         .alias("city_id"),
        F.when(F.col("org_level") == 1, F.col("city1_name"))
         .otherwise(F.col("city2_name"))
         .alias("city_name"),
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec"
    ).groupBy("dt", "recent_days", "city_id", "city_name").agg(
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )
    
    # 关联城市订单和运输数据
    city_1d = city_order_1d.join(
        trans_1d,
        ["dt", "recent_days", "city_id", "city_name"],
        "outer"
    ).select(
        "dt",
        "recent_days",
        "city_id",
        "city_name",
        F.coalesce(city_order_1d.order_count, F.lit(0)).alias("order_count"),
        F.coalesce(city_order_1d.order_amount, F.lit(0)).alias("order_amount"),
        F.coalesce(trans_1d.trans_finish_count, F.lit(0)).alias("trans_finish_count"),
        F.coalesce(trans_1d.trans_finish_distance, F.lit(0)).alias("trans_finish_distance"),
        F.coalesce(trans_1d.trans_finish_dur_sec, F.lit(0)).alias("trans_finish_dur_sec"),
        F.coalesce(trans_1d.avg_trans_finish_distance, F.lit(0)).alias("avg_trans_finish_distance"),
        F.coalesce(trans_1d.avg_trans_finish_dur_sec, F.lit(0)).alias("avg_trans_finish_dur_sec")
    )
    
    # 处理n日数据
    city_order_nd = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_nd").filter(
        F.col("dt") == dt
    ).groupBy("city_id", "city_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    city_trans_nd = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).groupBy("city_id", "city_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )
    
    # 关联城市订单和运输n日数据
    city_nd = city_order_nd.join(
        city_trans_nd,
        ["dt", "recent_days", "city_id", "city_name"],
        "outer"
    ).select(
        "dt",
        "recent_days",
        "city_id",
        "city_name",
        F.coalesce(city_order_nd.order_count, F.lit(0)).alias("order_count"),
        F.coalesce(city_order_nd.order_amount, F.lit(0)).alias("order_amount"),
        F.coalesce(city_trans_nd.trans_finish_count, F.lit(0)).alias("trans_finish_count"),
        F.coalesce(city_trans_nd.trans_finish_distance, F.lit(0)).alias("trans_finish_distance"),
        F.coalesce(city_trans_nd.trans_finish_dur_sec, F.lit(0)).alias("trans_finish_dur_sec"),
        F.coalesce(city_trans_nd.avg_trans_finish_distance, F.lit(0)).alias("avg_trans_finish_distance"),
        F.coalesce(city_trans_nd.avg_trans_finish_dur_sec, F.lit(0)).alias("avg_trans_finish_dur_sec")
    )
    
    # 合并所有数据
    result = history_data.unionByName(city_1d, allowMissingColumns=True).unionByName(city_nd, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_city_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_city_stats")

    print("城市分析 ads_city_stats 处理完成\n")

# ====================== 7. 机构分析 ======================
def process_ads_org_stats():
    print("开始处理机构分析 ads_org_stats")
    create_hive_table("ads_org_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_org_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_org_stats").select(
        "dt", "recent_days", "org_id", "org_name", "order_count", "order_amount",
        "trans_finish_count", "trans_finish_distance", "trans_finish_dur_sec",
        "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    )
    
    # 处理1日订单数据
    org_order_1d = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_1d").filter(
        F.col("dt") == dt
    ).groupBy("org_id", "org_name").agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 处理1日运输数据
    org_trans_1d = spark.table("py_tms_dws.dws_trans_org_truck_model_type_trans_finish_1d").filter(
        F.col("dt") == dt
    ).groupBy("org_id", "org_name").agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )
    
    # 关联机构订单和运输数据
    org_1d = org_order_1d.join(
        org_trans_1d,
        ["dt", "recent_days", "org_id", "org_name"],
        "outer"
    ).select(
        "dt",
        "recent_days",
        "org_id",
        "org_name",
        F.coalesce(org_order_1d.order_count, F.lit(0)).alias("order_count"),
        F.coalesce(org_order_1d.order_amount, F.lit(0)).alias("order_amount"),
        F.coalesce(org_trans_1d.trans_finish_count, F.lit(0)).alias("trans_finish_count"),
        F.coalesce(org_trans_1d.trans_finish_distance, F.lit(0)).alias("trans_finish_distance"),
        F.coalesce(org_trans_1d.trans_finish_dur_sec, F.lit(0)).alias("trans_finish_dur_sec"),
        F.coalesce(org_trans_1d.avg_trans_finish_distance, F.lit(0)).alias("avg_trans_finish_distance"),
        F.coalesce(org_trans_1d.avg_trans_finish_dur_sec, F.lit(0)).alias("avg_trans_finish_dur_sec")
    )
    
    # 处理n日订单数据
    org_order_nd = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_nd").filter(
        F.col("dt") == dt
    ).groupBy("org_id", "org_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    )
    
    # 处理n日运输数据
    org_trans_nd = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).groupBy("org_id", "org_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )
    
    # 关联机构订单和运输n日数据
    org_nd = org_order_nd.join(
        org_trans_nd,
        ["dt", "recent_days", "org_id", "org_name"],
        "inner"
    ).select(
        "dt",
        "recent_days",
        "org_id",
        "org_name",
        F.coalesce(org_order_nd.order_count, F.lit(0)).alias("order_count"),
        F.coalesce(org_order_nd.order_amount, F.lit(0)).alias("order_amount"),
        F.coalesce(org_trans_nd.trans_finish_count, F.lit(0)).alias("trans_finish_count"),
        F.coalesce(org_trans_nd.trans_finish_distance, F.lit(0)).alias("trans_finish_distance"),
        F.coalesce(org_trans_nd.trans_finish_dur_sec, F.lit(0)).alias("trans_finish_dur_sec"),
        F.coalesce(org_trans_nd.avg_trans_finish_distance, F.lit(0)).alias("avg_trans_finish_distance"),
        F.coalesce(org_trans_nd.avg_trans_finish_dur_sec, F.lit(0)).alias("avg_trans_finish_dur_sec")
    )
    
    # 合并所有数据
    result = history_data.unionByName(org_1d, allowMissingColumns=True).unionByName(org_nd, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_org_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_org_stats")
    

    print("机构分析 ads_org_stats 处理完成\n")

# ====================== 8. 班次分析 ======================
def process_ads_shift_stats():
    print("开始处理班次分析 ads_shift_stats")
    create_hive_table("ads_shift_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_shift_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_shift_stats").select(
        "dt", "recent_days", "shift_id", "trans_finish_count", "trans_finish_distance",
        "trans_finish_dur_sec", "trans_finish_order_count"
    )
    
    # 处理当日数据
    current_data = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).select(
        F.lit(dt).alias("dt"),
        "recent_days",
        "shift_id",
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_order_count"
    )
    
    # 合并所有数据
    result = history_data.unionByName(current_data, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_shift_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_shift_stats")

    print("班次分析 ads_shift_stats 处理完成\n")

# ====================== 9. 线路分析 ======================
def process_ads_line_stats():
    print("开始处理线路分析 ads_line_stats")
    create_hive_table("ads_line_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_line_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_line_stats").select(
        "dt", "recent_days", "line_id", "line_name", "trans_finish_count", "trans_finish_distance",
        "trans_finish_dur_sec", "trans_finish_order_count"
    )
    
    # 处理当日数据
    current_data = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).groupBy("line_id", "line_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        F.sum("trans_finish_order_count").alias("trans_finish_order_count")
    )
    
    # 合并所有数据
    result = history_data.unionByName(current_data, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_line_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_line_stats")

    print("线路分析 ads_line_stats 处理完成\n")

# ====================== 10. 司机分析 ======================
def process_ads_driver_stats():
    print("开始处理司机分析 ads_driver_stats")
    create_hive_table("ads_driver_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_driver_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_driver_stats").select(
        "dt", "recent_days", "driver_emp_id", "driver_name", "trans_finish_count", "trans_finish_distance",
        "trans_finish_dur_sec", "avg_trans_finish_distance", "avg_trans_finish_dur_sec", "trans_finish_late_count"
    )
    
    # 读取当日数据
    shift_data = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).select(
        "recent_days",
        "driver1_emp_id",
        "driver1_name",
        "driver2_emp_id",
        "driver2_name",
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_delay_count"
    )
    
    # 处理只有一个司机的情况
    driver_single = shift_data.filter(
        F.col("driver2_emp_id").isNull()
    ).select(
        F.lit(dt).alias("dt"),
        "recent_days",
        F.col("driver1_emp_id").alias("driver_emp_id"),
        F.col("driver1_name").alias("driver_name"),
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_delay_count"
    )
    
    # 处理有两个司机的情况
    driver_double = shift_data.filter(
        F.col("driver2_emp_id").isNotNull()
    ).select(
        "recent_days",
        F.array(
            F.array("driver1_emp_id", "driver1_name"),
            F.array("driver2_emp_id", "driver2_name")
        ).alias("driver_arr"),
        (F.col("trans_finish_count") / 2).alias("trans_finish_count"),
        (F.col("trans_finish_distance") / 2).alias("trans_finish_distance"),
        (F.col("trans_finish_dur_sec") / 2).alias("trans_finish_dur_sec"),
        (F.col("trans_finish_delay_count") / 2).alias("trans_finish_delay_count")
    )
    
    # 展开双司机数组
    driver_exploded = driver_double.select(
        F.lit(dt).alias("dt"),
        "recent_days",
        F.col("driver_arr")[0][0].cast(T.LongType()).alias("driver_emp_id"),
        F.col("driver_arr")[0][1].alias("driver_name"),
        "trans_finish_count",
        "trans_finish_distance",
        "trans_finish_dur_sec",
        "trans_finish_delay_count"
    ).union(
        driver_double.select(
            F.lit(dt).alias("dt"),
            "recent_days",
            F.col("driver_arr")[1][0].cast(T.LongType()).alias("driver_emp_id"),
            F.col("driver_arr")[1][1].alias("driver_name"),
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec",
            "trans_finish_delay_count"
        )
    )
    
    # 合并司机数据
    driver_data = driver_single.unionByName(driver_exploded).groupBy(
        "dt", "recent_days", "driver_emp_id", "driver_name"
    ).agg(
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec"),
        F.sum("trans_finish_delay_count").alias("trans_finish_late_count")
    )
    
    # 合并所有数据
    result = history_data.unionByName(driver_data, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_driver_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_driver_stats")

    print("司机分析 ads_driver_stats 处理完成\n")

# ====================== 11. 卡车分析 ======================
def process_ads_truck_stats():
    print("开始处理卡车分析 ads_truck_stats")
    create_hive_table("ads_truck_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_truck_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_truck_stats").select(
        "dt", "recent_days", "truck_model_type", "truck_model_type_name", "trans_finish_count", 
        "trans_finish_distance", "trans_finish_dur_sec", "avg_trans_finish_distance", "avg_trans_finish_dur_sec"
    )
    
    # 处理当日数据
    current_data = spark.table("py_tms_dws.dws_trans_shift_trans_finish_nd").filter(
        F.col("dt") == dt
    ).groupBy("truck_model_type", "truck_model_type_name", "recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("trans_finish_count").alias("trans_finish_count"),
        F.sum("trans_finish_distance").alias("trans_finish_distance"),
        F.sum("trans_finish_dur_sec").alias("trans_finish_dur_sec"),
        (F.sum("trans_finish_distance") / F.sum("trans_finish_count")).alias("avg_trans_finish_distance"),
        (F.sum("trans_finish_dur_sec") / F.sum("trans_finish_count")).alias("avg_trans_finish_dur_sec")
    )
    
    # 合并所有数据
    result = history_data.unionByName(current_data, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_truck_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_truck_stats")
    

    print("卡车分析 ads_truck_stats 处理完成\n")

# ====================== 12. 快递综合统计 ======================
def process_ads_express_stats():
    print("开始处理快递综合统计 ads_express_stats")
    create_hive_table("ads_express_stats")
    create_hdfs_dir("/warehouse/py_tms/ads/ads_express_stats")
    
    # 读取历史数据
    history_data = spark.table("py_tms_ads.ads_express_stats").select(
        "dt", "recent_days", "deliver_suc_count", "sort_count"
    )
    
    # 处理1日派送数据
    deliver_1d = spark.table("py_tms_dws.dws_trans_org_deliver_suc_1d").filter(
        F.col("dt") == dt
    ).agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("order_count").alias("deliver_suc_count")
    )
    
    # 处理1日分拣数据
    sort_1d = spark.table("py_tms_dws.dws_trans_org_sort_1d").filter(
        F.col("dt") == dt
    ).agg(
        F.lit(dt).alias("dt"),
        F.lit(1).alias("recent_days"),
        F.sum("sort_count").alias("sort_count")
    )
    
    # 关联1日数据
    combined_1d = deliver_1d.join(
        sort_1d,
        ["dt", "recent_days"],
        "outer"
    ).select(
        "dt",
        "recent_days",
        F.coalesce("deliver_suc_count", F.lit(0)).alias("deliver_suc_count"),
        F.coalesce("sort_count", F.lit(0)).alias("sort_count")
    )
    
    # 处理n日派送数据
    deliver_nd = spark.table("py_tms_dws.dws_trans_org_deliver_suc_nd").filter(
        F.col("dt") == dt
    ).groupBy("recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("order_count").alias("deliver_suc_count")
    )
    
    # 处理n日分拣数据
    sort_nd = spark.table("py_tms_dws.dws_trans_org_sort_nd").filter(
        F.col("dt") == dt
    ).groupBy("recent_days").agg(
        F.lit(dt).alias("dt"),
        F.sum("sort_count").alias("sort_count")
    )
    
    # 关联n日数据
    combined_nd = deliver_nd.join(
        sort_nd,
        ["dt", "recent_days"],
        "outer"
    ).select(
        "dt",
        "recent_days",
        F.coalesce("deliver_suc_count", F.lit(0)).alias("deliver_suc_count"),
        F.coalesce("sort_count", F.lit(0)).alias("sort_count")
    )
    
    # 合并所有数据
    result = history_data.unionByName(combined_1d, allowMissingColumns=True).unionByName(combined_nd, allowMissingColumns=True)
    
    # 验证数据量
    print_data_count(result, "ads_express_stats")
    
    # 写入数据
    result.write.mode("overwrite") \
        .orc("/warehouse/py_tms/ads/ads_express_stats")

    print("快递综合统计 ads_express_stats 处理完成\n")

# ====================== 主函数 ======================
if __name__ == "__main__":
    print("开始执行 TMS ADS 层汇总表处理任务")
    
    # 执行各个汇总表的处理
    process_ads_trans_order_stats()           # 1. 运单相关统计
    process_ads_trans_stats()                 # 2. 运输综合统计
    process_ads_trans_order_stats_td()        # 3. 历史至今运单统计
    process_ads_order_stats()                 # 4. 运单综合统计
    process_ads_order_cargo_type_stats()      # 5. 各类型货物运单统计
    process_ads_city_stats()                  # 6. 城市分析
    process_ads_org_stats()                   # 7. 机构分析
    process_ads_shift_stats()                 # 8. 班次分析
    process_ads_line_stats()                  # 9. 线路分析
    process_ads_driver_stats()                # 10. 司机分析
    process_ads_truck_stats()                 # 11. 卡车分析
    process_ads_express_stats()               # 12. 快递综合统计
    # 其他表的处理函数可以继续添加
    
    print("所有 TMS ADS 层汇总表处理任务执行完成")
    
    # 关闭 SparkSession
    spark.stop()