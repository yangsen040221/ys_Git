from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("py_tms_dws") \
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

# 定义 Hive 表分区修复函数：
def repair_hive_table(table_name):
    # 执行MSCK REPAIR TABLE命令修复分区
    spark.sql(f"MSCK REPAIR TABLE py_tms_dws.{table_name}")
    print(f"修复分区完成：py_tms_dws.{table_name}")

# 新增：创建Hive表的函数
def create_hive_table(table_name):
    table_schemas = {
        "dws_trade_org_cargo_type_order_1d": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trade_org_cargo_type_order_1d(
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '转运站名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `cargo_type` string COMMENT '货物类型',
                `cargo_type_name` string COMMENT '货物类型名称',
                `order_count` bigint COMMENT '下单数',
                `order_amount` decimal(16,2) COMMENT '下单金额'
            ) COMMENT '交易域机构货物类型粒度下单 1 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trade_org_cargo_type_order_1d'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_receive_1d": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_receive_1d(
                `org_id` bigint COMMENT '转运站ID',
                `org_name` string COMMENT '转运站名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `order_count` bigint COMMENT '揽收次数',
                `order_amount` decimal(16, 2) COMMENT '揽收金额'
            ) COMMENT '物流域转运站粒度揽收 1 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_receive_1d'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_dispatch_1d": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_dispatch_1d(
                `order_count` bigint COMMENT '发单总数',
                `order_amount` decimal(16,2) COMMENT '发单总金额'
            ) COMMENT '物流域发单 1 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_dispatch_1d'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_truck_model_type_trans_finish_1d": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_truck_model_type_trans_finish_1d(
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '机构名称',
                `truck_model_type` string COMMENT '卡车类别编码',
                `truck_model_type_name` string COMMENT '卡车类别名称',
                `trans_finish_count` bigint COMMENT '运输完成次数',
                `trans_finish_distance` decimal(16,2) COMMENT '运输完成里程',
                `trans_finish_dur_sec` bigint COMMENT '运输完成时长，单位：秒'
            ) COMMENT '物流域机构卡车类别粒度运输最近 1 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_truck_model_type_trans_finish_1d'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_deliver_suc_1d": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_deliver_suc_1d(
                `org_id` bigint COMMENT '转运站ID',
                `org_name` string COMMENT '转运站名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `order_count` bigint COMMENT '派送成功次数（订单数）'
            ) COMMENT '物流域转运站粒度派送成功 1 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_deliver_suc_1d'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_sort_1d": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_sort_1d(
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '机构名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `sort_count` bigint COMMENT '分拣次数'
            ) COMMENT '物流域机构粒度分拣 1 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_sort_1d'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trade_org_cargo_type_order_nd": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trade_org_cargo_type_order_nd(
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '转运站名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `cargo_type` string COMMENT '货物类型',
                `cargo_type_name` string COMMENT '货物类型名称',
                `recent_days` tinyint COMMENT '最近天数',
                `order_count` bigint COMMENT '下单数',
                `order_amount` decimal(16,2) COMMENT '下单金额'
            ) COMMENT '交易域机构货物类型粒度下单 n 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trade_org_cargo_type_order_nd'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_receive_nd": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_receive_nd(
                `org_id` bigint COMMENT '转运站ID',
                `org_name` string COMMENT '转运站名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `recent_days` tinyint COMMENT '最近天数',
                `order_count` bigint COMMENT '揽收次数',
                `order_amount` decimal(16, 2) COMMENT '揽收金额'
            ) COMMENT '物流域转运站粒度揽收 n 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_receive_nd'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_dispatch_nd": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_dispatch_nd(
                `recent_days` tinyint COMMENT '最近天数',
                `order_count` bigint COMMENT '发单总数',
                `order_amount` decimal(16,2) COMMENT '发单总金额'
            ) COMMENT '物流域发单 n 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_dispatch_nd'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_shift_trans_finish_nd": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_shift_trans_finish_nd(
                `shift_id` bigint COMMENT '班次ID',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '机构名称',
                `line_id` bigint COMMENT '线路ID',
                `line_name` string COMMENT '线路名称',
                `driver1_emp_id` bigint COMMENT '第一司机员工ID',
                `driver1_name` string COMMENT '第一司机姓名',
                `driver2_emp_id` bigint COMMENT '第二司机员工ID',
                `driver2_name` string COMMENT '第二司机姓名',
                `truck_model_type` string COMMENT '卡车类别编码',
                `truck_model_type_name` string COMMENT '卡车类别名称',
                `recent_days` tinyint COMMENT '最近天数',
                `trans_finish_count` bigint COMMENT '转运完成次数',
                `trans_finish_distance` decimal(16,2) COMMENT '转运完成里程',
                `trans_finish_dur_sec` bigint COMMENT '转运完成时长，单位：秒',
                `trans_finish_order_count` bigint COMMENT '转运完成运单数',
                `trans_finish_delay_count` bigint COMMENT '逾期次数'
            ) COMMENT '物流域班次粒度转运完成最近 n 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_shift_trans_finish_nd'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_deliver_suc_nd": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_deliver_suc_nd(
                `org_id` bigint COMMENT '转运站ID',
                `org_name` string COMMENT '转运站名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `recent_days` tinyint COMMENT '最近天数',
                `order_count` bigint COMMENT '派送成功次数（订单数）'
            ) COMMENT '物流域转运站粒度派送成功 n 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_deliver_suc_nd'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_org_sort_nd": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_org_sort_nd(
                `org_id` bigint COMMENT '机构ID',
                `org_name` string COMMENT '机构名称',
                `city_id` bigint COMMENT '城市ID',
                `city_name` string COMMENT '城市名称',
                `province_id` bigint COMMENT '省份ID',
                `province_name` string COMMENT '省份名称',
                `recent_days` tinyint COMMENT '最近天数',
                `sort_count` bigint COMMENT '分拣次数'
            ) COMMENT '物流域机构粒度分拣 n 日汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_org_sort_nd'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_dispatch_td": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_dispatch_td(
                `order_count` bigint COMMENT '发单数',
                `order_amount` decimal(16,2) COMMENT '发单金额'
            ) COMMENT '物流域发单历史至今汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_dispatch_td'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dws_trans_bound_finish_td": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dws.dws_trans_bound_finish_td(
                `order_count` bigint COMMENT '发单数',
                `order_amount` decimal(16,2) COMMENT '发单金额'
            ) COMMENT '物流域转运完成历史至今汇总表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dws/dws_trans_bound_finish_td'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """
    }
    
    if table_name in table_schemas:
        spark.sql(table_schemas[table_name])
        print(f"表 {table_name} 创建成功或已存在")
    else:
        print(f"未找到表 {table_name} 的定义")

# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count

# ====================== 1. 交易域机构货物类型粒度下单 1 日汇总表 ======================
def process_dws_trade_org_cargo_type_order_1d():
    print("开始处理交易域机构货物类型粒度下单 1 日汇总表 dws_trade_org_cargo_type_order_1d")
    create_hive_table("dws_trade_org_cargo_type_order_1d")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trade_org_cargo_type_order_1d")

    # 读取dwd层数据
    detail = spark.table("py_tms_dwd.dwd_trade_order_detail_inc").filter(
        F.col("dt_partition") == dt
    ).select(
        "order_id",
        "cargo_type",
        "cargo_type_name",
        "sender_district_id",
        "sender_city_id",
        "amount",
        "dt_partition"
    )

    # 去重处理
    distinct_detail = detail.groupBy(
        "order_id",
        "cargo_type",
        "cargo_type_name",
        "sender_district_id",
        "sender_city_id",
        "dt_partition"
    ).agg(
        F.max("amount").alias("amount")
    )

    # 关联机构维度表
    org = spark.table("tms_dim.dim_organ_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("org_id"),
        "org_name",
        F.col("region_id").alias("org_region_id")  # 避免与detail表中的sender_district_id混淆
    )

    # 关联地区维度表
    region = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("region_city_id"),      # 避免与detail表中的sender_city_id重命名后的city_id混淆
        F.col("name").alias("region_name")
    )

    # 聚合处理
    agg = distinct_detail.join(
        org, 
        distinct_detail.sender_district_id == org.org_region_id, 
        "left"
    ).groupBy(
        "org_id",
        "org_name",
        "cargo_type",
        "cargo_type_name",
        "sender_city_id",
        "dt_partition"
    ).agg(
        F.count("order_id").alias("order_count"),
        F.sum("amount").alias("order_amount")
    ).withColumnRenamed("sender_city_id", "city_id")

    # 最终关联地区维度表
    result = agg.join(
        region,
        agg.city_id == region.region_city_id,
        "left"
    ).select(
        "org_id",
        "org_name",
        "city_id",
        F.col("region_name").alias("city_name"),
        "cargo_type",
        "cargo_type_name",
        "order_count",
        "order_amount",
        F.col("dt_partition").alias("dt")
    )

    # 验证数据量
    print_data_count(result, "dws_trade_org_cargo_type_order_1d")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trade_org_cargo_type_order_1d")

    # 修复分区
    repair_hive_table("dws_trade_org_cargo_type_order_1d")
    print("交易域机构货物类型粒度下单 1 日汇总表 dws_trade_org_cargo_type_order_1d 处理完成\n")

# ====================== 2. 物流域转运站粒度揽收 1 日汇总表 ======================
def process_dws_trans_org_receive_1d():
    print("开始处理物流域转运站粒度揽收 1 日汇总表 dws_trans_org_receive_1d")
    create_hive_table("dws_trans_org_receive_1d")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_receive_1d")

    # 读取dwd层数据
    detail = spark.table("py_tms_dwd.dwd_trans_receive_detail_inc").filter(
        F.col("dt_partition") == dt
    ).select(
        "order_id",
        "amount",
        "sender_district_id",
        "dt_partition"
    )

    # 关联机构维度表
    organ = spark.table("tms_dim.dim_organ_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("org_id"),
        "org_name",
        F.col("region_id").alias("org_region_id")  # 避免与detail表中的sender_district_id混淆
    )

    # 关联地区维度表（区县）
    district = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("district_id"),
        F.col("parent_id").alias("district_parent_id")
    )

    # 关联地区维度表（城市）
    city = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("city_dim_id"),           # 避免与其他city_id混淆
        F.col("name").alias("city_name"),
        F.col("parent_id").alias("city_parent_id")
    )

    # 关联地区维度表（省份）
    province = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("province_id"),
        F.col("name").alias("province_name"),
        F.col("parent_id").alias("province_parent_id")
    )

    # 去重处理
    distinct_tb = detail.join(
        organ,
        detail.sender_district_id == organ.org_region_id,
        "left"
    ).join(
        district,
        organ.org_region_id == district.district_id,
        "left"
    ).join(
        city,
        district.district_parent_id == city.city_dim_id,
        "left"
    ).join(
        province,
        city.city_parent_id == province.province_id,
        "left"
    ).groupBy(
        "order_id",
        "org_id",
        "org_name",
        "city_dim_id",
        "city_name",
        "province_id",
        "province_name",
        "dt_partition"
    ).agg(
        F.max("amount").alias("distinct_amount")
    )

    # 聚合处理
    result = distinct_tb.groupBy(
        "org_id",
        "org_name",
        "city_dim_id",
        "city_name",
        "province_id",
        "province_name",
        "dt_partition"
    ).agg(
        F.count("order_id").alias("order_count"),
        F.sum("distinct_amount").alias("order_amount")
    ).withColumnRenamed("dt_partition", "dt").withColumnRenamed("city_dim_id", "city_id")

    # 验证数据量
    print_data_count(result, "dws_trans_org_receive_1d")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_receive_1d")

    # 修复分区
    repair_hive_table("dws_trans_org_receive_1d")
    print("物流域转运站粒度揽收 1 日汇总表 dws_trans_org_receive_1d 处理完成\n")

# ====================== 3. 物流域发单 1 日汇总表 ======================
def process_dws_trans_dispatch_1d():
    print("开始处理物流域发单 1 日汇总表 dws_trans_dispatch_1d")
    create_hive_table("dws_trans_dispatch_1d")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_dispatch_1d")

    # 读取dwd层数据
    detail = spark.table("py_tms_dwd.dwd_trans_dispatch_detail_inc").filter(
        F.col("dt_partition") == dt
    ).select(
        "order_id",
        "amount",
        "dt_partition"
    )

    # 去重处理
    distinct_info = detail.groupBy(
        "order_id",
        "dt_partition"
    ).agg(
        F.max("amount").alias("distinct_amount")
    )

    # 聚合处理
    result = distinct_info.groupBy(
        "dt_partition"
    ).agg(
        F.count("order_id").alias("order_count"),
        F.sum("distinct_amount").alias("order_amount")
    ).withColumnRenamed("dt_partition", "dt")

    # 验证数据量
    print_data_count(result, "dws_trans_dispatch_1d")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_dispatch_1d")

    # 修复分区
    repair_hive_table("dws_trans_dispatch_1d")
    print("物流域发单 1 日汇总表 dws_trans_dispatch_1d 处理完成\n")

# ====================== 4. 物流域机构卡车类别粒度运输最近 1 日汇总表 ======================
def process_dws_trans_org_truck_model_type_trans_finish_1d():
    print("开始处理物流域机构卡车类别粒度运输最近 1 日汇总表 dws_trans_org_truck_model_type_trans_finish_1d")
    create_hive_table("dws_trans_org_truck_model_type_trans_finish_1d")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_truck_model_type_trans_finish_1d")

    # 读取dwd层数据
    trans_finish = spark.table("py_tms_dwd.dwd_trans_trans_finish_inc").filter(
        F.col("dt") == dt
    ).select(
        "id",
        "start_org_id",
        "start_org_name",
        "truck_id",
        "actual_distance",
        "finish_dur_sec",
        "dt"
    ).withColumnRenamed("start_org_id", "org_id") \
     .withColumnRenamed("start_org_name", "org_name")

    # 关联卡车维度表
    truck_info = spark.table("tms_dim.dim_truck_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("truck_id"),
        "truck_model_type",
        "truck_model_type_name"
    )

    # 聚合处理
    result = trans_finish.join(
        truck_info,
        trans_finish.truck_id == truck_info.truck_id,
        "left"
    ).groupBy(
        "org_id",
        "org_name",
        "truck_model_type",
        "truck_model_type_name",
        "dt"
    ).agg(
        F.count("id").alias("trans_finish_count"),
        F.sum("actual_distance").alias("trans_finish_distance"),
        F.sum("finish_dur_sec").alias("trans_finish_dur_sec")
    )

    # 验证数据量
    print_data_count(result, "dws_trans_org_truck_model_type_trans_finish_1d")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_truck_model_type_trans_finish_1d")

    # 修复分区
    repair_hive_table("dws_trans_org_truck_model_type_trans_finish_1d")
    print("物流域机构卡车类别粒度运输最近 1 日汇总表 dws_trans_org_truck_model_type_trans_finish_1d 处理完成\n")

# ====================== 5. 物流域转运站粒度派送成功 1 日汇总表 ======================
def process_dws_trans_org_deliver_suc_1d():
    print("开始处理物流域转运站粒度派送成功 1 日汇总表 dws_trans_org_deliver_suc_1d")
    create_hive_table("dws_trans_org_deliver_suc_1d")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_deliver_suc_1d")

    # 读取dwd层数据并去重
    detail = spark.table("py_tms_dwd.dwd_trans_deliver_suc_detail_inc").filter(
        F.col("dt_partition") == dt
    ).select(
        "order_id",
        "receiver_district_id",
        "dt_partition"
    ).groupBy(
        "order_id",
        "receiver_district_id",
        "dt_partition"
    ).agg(
        F.lit(1).alias("dummy")  # 用于去重的占位符
    )

    # 关联机构维度表
    organ = spark.table("tms_dim.dim_organ_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("org_id"),
        "org_name",
        F.col("region_id").alias("org_region_id")  # 避免与detail表中的receiver_district_id混淆
    )

    # 关联地区维度表（区县）
    district = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("district_id"),
        F.col("parent_id").alias("district_city_id")  # 避免与city表的city_id混淆
    )

    # 关联地区维度表（城市）
    city = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("city_dim_id"),              # 避免与其他city_id混淆
        F.col("name").alias("city_name"),
        F.col("parent_id").alias("city_province_id")  # 避免与province表的province_id混淆
    )

    # 关联地区维度表（省份）
    province = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("province_id"),
        F.col("name").alias("province_name")
    )

    # 关联处理
    joined = detail.join(
        organ,
        detail.receiver_district_id == organ.org_region_id,
        "left"
    ).join(
        district,
        detail.receiver_district_id == district.district_id,
        "left"
    ).join(
        city,
        district.district_city_id == city.city_dim_id,
        "left"
    ).join(
        province,
        city.city_province_id == province.province_id,
        "left"
    )

    # 聚合处理
    result = joined.groupBy(
        "org_id",
        "org_name",
        "city_dim_id",
        "city_name",
        "province_id",
        "province_name",
        "dt_partition"
    ).agg(
        F.count("order_id").alias("order_count")
    ).withColumnRenamed("dt_partition", "dt").withColumnRenamed("city_dim_id", "city_id")

    # 验证数据量
    print_data_count(result, "dws_trans_org_deliver_suc_1d")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_deliver_suc_1d")

    # 修复分区
    repair_hive_table("dws_trans_org_deliver_suc_1d")
    print("物流域转运站粒度派送成功 1 日汇总表 dws_trans_org_deliver_suc_1d 处理完成\n")

# ====================== 6. 物流域机构粒度分拣 1 日汇总表 ======================
def process_dws_trans_org_sort_1d():
    print("开始处理物流域机构粒度分拣 1 日汇总表 dws_trans_org_sort_1d")
    create_hive_table("dws_trans_org_sort_1d")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_sort_1d")

    # 读取dwd层数据并聚合
    agg = spark.table("py_tms_dwd.dwd_bound_sort_inc").filter(
        F.col("dt") == dt
    ).groupBy(
        "org_id"
    ).agg(
        F.count("*").alias("sort_count"),
        F.first("dt").alias("dt")
    )

    # 关联机构维度表
    org = spark.table("tms_dim.dim_organ_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("org_dim_id"),     # 避免与agg表中的org_id混淆
        "org_name",
        "org_level",
        F.col("region_id").alias("org_region_id")
    )

    # 关联地区维度表（城市级别1）
    city_for_level1 = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("city1_id"),        # 避免与其他city_id混淆
        F.col("name").alias("city1_name"),
        F.col("parent_id").alias("city1_parent_id")
    )

    # 关联地区维度表（省份级别1）
    province_for_level1 = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("province1_id"),
        F.col("name").alias("province1_name"),
        F.col("parent_id").alias("province1_parent_id")
    )

    # 关联地区维度表（省份级别2）
    province_for_level2 = spark.table("tms_dim.dim_region_full").filter(
        F.col("dt") == dt
    ).select(
        F.col("id").alias("province2_id"),
        F.col("name").alias("province2_name")
    )

    # 关联处理
    joined = agg.join(
        org,
        agg.org_id == org.org_dim_id,
        "left"
    ).join(
        city_for_level1,
        org.org_region_id == city_for_level1.city1_id,
        "left"
    ).join(
        province_for_level1,
        city_for_level1.city1_parent_id == province_for_level1.province1_id,
        "left"
    ).join(
        province_for_level2,
        province_for_level1.province1_parent_id == province_for_level2.province2_id,
        "left"
    )

    # 处理城市和省份字段
    result = joined.select(
        F.col("org_dim_id").alias("org_id"),
        "org_name",
        F.when(F.col("org_level") == 1, F.col("city1_id"))
         .otherwise(F.col("province1_id"))
         .alias("city_id"),
        F.when(F.col("org_level") == 1, F.col("city1_name"))
         .otherwise(F.col("province1_name"))
         .alias("city_name"),
        F.when(F.col("org_level") == 1, F.col("province1_id"))
         .otherwise(F.col("province2_id"))
         .alias("province_id"),
        F.when(F.col("org_level") == 1, F.col("province1_name"))
         .otherwise(F.col("province2_name"))
         .alias("province_name"),
        "sort_count",
        "dt"
    )

    # 验证数据量
    print_data_count(result, "dws_trans_org_sort_1d")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_sort_1d")

    # 修复分区
    repair_hive_table("dws_trans_org_sort_1d")
    print("物流域机构粒度分拣 1 日汇总表 dws_trans_org_sort_1d 处理完成\n")

# ====================== 7. 交易域机构货物类型粒度下单 n 日汇总表 ======================
def process_dws_trade_org_cargo_type_order_nd():
    print("开始处理交易域机构货物类型粒度下单 n 日汇总表 dws_trade_org_cargo_type_order_nd")
    create_hive_table("dws_trade_org_cargo_type_order_nd")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trade_org_cargo_type_order_nd")

    # 读取1日汇总表数据
    detail_1d = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_1d").filter(
        F.col("dt") == dt
    ).select(
        "org_id",
        "org_name",
        "city_id",
        "city_name",
        "cargo_type",
        "cargo_type_name",
        "order_count",
        "order_amount",
        "dt"
    )

    # 创建最近天数数组
    recent_days_list = [7, 30]
    
    # 为每个最近天数生成数据
    results = []
    for recent_days in recent_days_list:
        start_date = F.date_add(F.lit(dt), -recent_days + 1)
        
        # 读取n日数据
        n_day_data = spark.table("py_tms_dws.dws_trade_org_cargo_type_order_1d").filter(
            F.col("dt") >= start_date
        ).select(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "cargo_type",
            "cargo_type_name",
            "order_count",
            "order_amount",
            "dt"
        )
        
        # 聚合处理
        agg_result = n_day_data.groupBy(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "cargo_type",
            "cargo_type_name"
        ).agg(
            F.lit(recent_days).alias("recent_days"),
            F.sum("order_count").alias("order_count"),
            F.sum("order_amount").alias("order_amount")
        ).withColumn("dt", F.lit(dt))
        
        results.append(agg_result)
    
    # 合并所有结果
    if results:
        final_result = results[0]
        for i in range(1, len(results)):
            final_result = final_result.union(results[i])
    else:
        # 如果没有数据，创建空的DataFrame
        final_result = spark.createDataFrame([], detail_1d.schema)

    # 验证数据量
    print_data_count(final_result, "dws_trade_org_cargo_type_order_nd")

    # 写入数据
    final_result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trade_org_cargo_type_order_nd")

    # 修复分区
    repair_hive_table("dws_trade_org_cargo_type_order_nd")
    print("交易域机构货物类型粒度下单 n 日汇总表 dws_trade_org_cargo_type_order_nd 处理完成\n")

# ====================== 8. 物流域转运站粒度揽收 n 日汇总表 ======================
def process_dws_trans_org_receive_nd():
    print("开始处理物流域转运站粒度揽收 n 日汇总表 dws_trans_org_receive_nd")
    create_hive_table("dws_trans_org_receive_nd")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_receive_nd")

    # 创建最近天数数组
    recent_days_list = [7, 30]
    
    # 为每个最近天数生成数据
    results = []
    for recent_days in recent_days_list:
        start_date = F.date_add(F.lit(dt), -recent_days + 1)
        
        # 读取n日数据
        n_day_data = spark.table("py_tms_dws.dws_trans_org_receive_1d").filter(
            F.col("dt") >= start_date
        ).select(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "province_id",
            "province_name",
            "order_count",
            "order_amount",
            "dt"
        )
        
        # 聚合处理
        agg_result = n_day_data.groupBy(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "province_id",
            "province_name"
        ).agg(
            F.lit(recent_days).alias("recent_days"),
            F.sum("order_count").alias("order_count"),
            F.sum("order_amount").alias("order_amount")
        ).withColumn("dt", F.lit(dt))
        
        results.append(agg_result)
    
    # 合并所有结果
    if results:
        final_result = results[0]
        for i in range(1, len(results)):
            final_result = final_result.union(results[i])
    else:
        # 如果没有数据，创建空的DataFrame
        final_result = spark.createDataFrame([], spark.table("py_tms_dws.dws_trans_org_receive_1d").schema)

    # 验证数据量
    print_data_count(final_result, "dws_trans_org_receive_nd")

    # 写入数据
    final_result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_receive_nd")

    # 修复分区
    repair_hive_table("dws_trans_org_receive_nd")
    print("物流域转运站粒度揽收 n 日汇总表 dws_trans_org_receive_nd 处理完成\n")

# ====================== 9. 物流域发单 n 日汇总表 ======================
def process_dws_trans_dispatch_nd():
    print("开始处理物流域发单 n 日汇总表 dws_trans_dispatch_nd")
    create_hive_table("dws_trans_dispatch_nd")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_dispatch_nd")

    # 创建最近天数数组
    recent_days_list = [7, 30]
    
    # 为每个最近天数生成数据
    results = []
    for recent_days in recent_days_list:
        start_date = F.date_add(F.lit(dt), -recent_days + 1)
        
        # 读取n日数据
        n_day_data = spark.table("py_tms_dws.dws_trans_dispatch_1d").filter(
            F.col("dt") >= start_date
        ).select(
            "order_count",
            "order_amount",
            "dt"
        )
        
        # 聚合处理
        agg_result = n_day_data.agg(
            F.lit(recent_days).alias("recent_days"),
            F.sum("order_count").alias("order_count"),
            F.sum("order_amount").alias("order_amount")
        ).withColumn("dt", F.lit(dt))
        
        results.append(agg_result)
    
    # 合并所有结果
    if results:
        final_result = results[0]
        for i in range(1, len(results)):
            final_result = final_result.union(results[i])
    else:
        # 如果没有数据，创建空的DataFrame
        final_result = spark.createDataFrame([], spark.table("py_tms_dws.dws_trans_dispatch_1d").schema)

    # 验证数据量
    print_data_count(final_result, "dws_trans_dispatch_nd")

    # 写入数据
    final_result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_dispatch_nd")

    # 修复分区
    repair_hive_table("dws_trans_dispatch_nd")
    print("物流域发单 n 日汇总表 dws_trans_dispatch_nd 处理完成\n")

# ====================== 10. 物流域班次粒度转运完成最近 n 日汇总表 ======================
def process_dws_trans_shift_trans_finish_nd():
    print("开始处理物流域班次粒度转运完成最近 n 日汇总表 dws_trans_shift_trans_finish_nd")
    create_hive_table("dws_trans_shift_trans_finish_nd")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_shift_trans_finish_nd")

    # 读取dwd层数据
    detail = spark.table("py_tms_dwd.dwd_trans_trans_finish_inc").filter(
        F.col("dt") == dt
    ).select(
        "id",
        "shift_id",
        "line_id",
        "truck_id",
        "start_org_id",
        "start_org_name",
        "driver1_emp_id",
        "driver1_name",
        "driver2_emp_id",
        "driver2_name",
        "actual_end_time",
        "estimate_end_time",
        "actual_distance",
        "finish_dur_sec",
        "order_num"
    ).withColumnRenamed("start_org_id", "org_id") \
     .withColumnRenamed("start_org_name", "org_name")

    # 创建最近天数数组并生成数据
    recent_days_list = [7, 30]
    
    results = []
    for recent_days in recent_days_list:
        start_date = F.date_add(F.lit(dt), -recent_days + 1)
        
        # 读取n日数据
        n_day_data = spark.table("py_tms_dwd.dwd_trans_trans_finish_inc").filter(
            F.col("dt") >= start_date
        ).select(
            "id",
            "shift_id",
            "line_id",
            "truck_id",
            "start_org_id",
            "start_org_name",
            "driver1_emp_id",
            "driver1_name",
            "driver2_emp_id",
            "driver2_name",
            "actual_end_time",
            "estimate_end_time",
            "actual_distance",
            "finish_dur_sec",
            "order_num"
        ).withColumnRenamed("start_org_id", "org_id") \
         .withColumnRenamed("start_org_name", "org_name")
        
        # 聚合处理
        aggregated = n_day_data.groupBy(
            "shift_id",
            "line_id",
            "truck_id",
            "org_id",
            "org_name",
            "driver1_emp_id",
            "driver1_name",
            "driver2_emp_id",
            "driver2_name"
        ).agg(
            F.lit(recent_days).alias("recent_days"),
            F.count("id").alias("trans_finish_count"),
            F.sum("actual_distance").alias("trans_finish_distance"),
            F.sum("finish_dur_sec").alias("trans_finish_dur_sec"),
            F.sum("order_num").alias("trans_finish_order_count"),
            F.sum(F.when(F.col("actual_end_time") > F.col("estimate_end_time"), 1).otherwise(0)).alias("trans_finish_delay_count")
        )
        
        # 关联维度表
        first = spark.table("tms_dim.dim_organ_full").filter(
            F.col("dt") == dt
        ).select(
            F.col("id").alias("org_dim_id"),
            "org_level",
            F.col("region_id").alias("org_region_id"),     # 避免与其他表的region_id混淆
            F.col("region_name").alias("org_region_name")  # 避免与其他表的region_name混淆
        )
        
        parent = spark.table("tms_dim.dim_region_full").filter(
            F.col("dt") == dt
        ).select(
            F.col("id").alias("region_dim_id"),
            F.col("parent_id").alias("region_parent_id")
        )
        
        city = spark.table("tms_dim.dim_region_full").filter(
            F.col("dt") == dt
        ).select(
            F.col("id").alias("city_dim_id"),
            F.col("name").alias("city_dim_name")
        )
        
        for_line_name = spark.table("tms_dim.dim_shift_full").filter(
            F.col("dt") == dt
        ).select(
            F.col("id").alias("shift_dim_id"),
            F.col("line_name").alias("line_dim_name")
        )
        
        truck_info = spark.table("tms_dim.dim_truck_full").filter(
            F.col("dt") == dt
        ).select(
            F.col("id").alias("truck_dim_id"),
            "truck_model_type",
            "truck_model_type_name"
        )
        
        # 关联处理
        joined = aggregated.join(
            first,
            aggregated.org_id == first.org_dim_id,
            "left"
        ).join(
            parent,
            first.org_region_id == parent.region_dim_id,
            "left"
        ).join(
            city,
            parent.region_parent_id == city.city_dim_id,
            "left"
        ).join(
            for_line_name,
            aggregated.shift_id == for_line_name.shift_dim_id,
            "left"
        ).join(
            truck_info,
            aggregated.truck_id == truck_info.truck_dim_id,
            "left"
        )
        
        # 最终选择字段
        result = joined.select(
            "shift_id",
            F.when(F.col("org_level") == 1, F.col("org_region_id"))
             .otherwise(F.col("city_dim_id"))
             .alias("city_id"),
            F.when(F.col("org_level") == 1, F.col("org_region_name"))
             .otherwise(F.col("city_dim_name"))
             .alias("city_name"),
            F.col("org_dim_id").alias("org_id"),
            "org_name",
            "line_id",
            F.col("line_dim_name").alias("line_name"),
            "driver1_emp_id",
            "driver1_name",
            "driver2_emp_id",
            "driver2_name",
            "truck_model_type",
            "truck_model_type_name",
            "recent_days",
            "trans_finish_count",
            "trans_finish_distance",
            "trans_finish_dur_sec",
            "trans_finish_order_count",
            "trans_finish_delay_count",
            F.lit(dt).alias("dt")
        )
        
        results.append(result)
    
    # 合并所有结果
    if results:
        final_result = results[0]
        for i in range(1, len(results)):
            final_result = final_result.union(results[i])
    else:
        # 如果没有数据，创建空的DataFrame
        final_result = spark.createDataFrame([], detail.schema)

    # 验证数据量
    print_data_count(final_result, "dws_trans_shift_trans_finish_nd")

    # 写入数据
    final_result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_shift_trans_finish_nd")

    # 修复分区
    repair_hive_table("dws_trans_shift_trans_finish_nd")
    print("物流域班次粒度转运完成最近 n 日汇总表 dws_trans_shift_trans_finish_nd 处理完成\n")

# ====================== 11. 物流域转运站粒度派送成功 n 日汇总表 ======================
def process_dws_trans_org_deliver_suc_nd():
    print("开始处理物流域转运站粒度派送成功 n 日汇总表 dws_trans_org_deliver_suc_nd")
    create_hive_table("dws_trans_org_deliver_suc_nd")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_deliver_suc_nd")

    # 创建最近天数数组
    recent_days_list = [7, 30]
    
    # 为每个最近天数生成数据
    results = []
    for recent_days in recent_days_list:
        start_date = F.date_add(F.lit(dt), -recent_days + 1)
        
        # 读取n日数据
        n_day_data = spark.table("py_tms_dws.dws_trans_org_deliver_suc_1d").filter(
            F.col("dt") >= start_date
        ).select(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "province_id",
            "province_name",
            "order_count",
            "dt"
        )
        
        # 聚合处理
        agg_result = n_day_data.groupBy(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "province_id",
            "province_name"
        ).agg(
            F.lit(recent_days).alias("recent_days"),
            F.sum("order_count").alias("order_count")
        ).withColumn("dt", F.lit(dt))
        
        results.append(agg_result)
    
    # 合并所有结果
    if results:
        final_result = results[0]
        for i in range(1, len(results)):
            final_result = final_result.union(results[i])
    else:
        # 如果没有数据，创建空的DataFrame
        final_result = spark.createDataFrame([], spark.table("py_tms_dws.dws_trans_org_deliver_suc_1d").schema)

    # 验证数据量
    print_data_count(final_result, "dws_trans_org_deliver_suc_nd")

    # 写入数据
    final_result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_deliver_suc_nd")

    # 修复分区
    repair_hive_table("dws_trans_org_deliver_suc_nd")
    print("物流域转运站粒度派送成功 n 日汇总表 dws_trans_org_deliver_suc_nd 处理完成\n")

# ====================== 12. 物流域机构粒度分拣 n 日汇总表 ======================
def process_dws_trans_org_sort_nd():
    print("开始处理物流域机构粒度分拣 n 日汇总表 dws_trans_org_sort_nd")
    create_hive_table("dws_trans_org_sort_nd")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_org_sort_nd")

    # 创建最近天数数组
    recent_days_list = [7, 30]
    
    # 为每个最近天数生成数据
    results = []
    for recent_days in recent_days_list:
        start_date = F.date_add(F.lit(dt), -recent_days + 1)
        
        # 读取n日数据
        n_day_data = spark.table("py_tms_dws.dws_trans_org_sort_1d").filter(
            F.col("dt") >= start_date
        ).select(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "province_id",
            "province_name",
            "sort_count",
            "dt"
        )
        
        # 聚合处理
        agg_result = n_day_data.groupBy(
            "org_id",
            "org_name",
            "city_id",
            "city_name",
            "province_id",
            "province_name"
        ).agg(
            F.lit(recent_days).alias("recent_days"),
            F.sum("sort_count").alias("sort_count")
        ).withColumn("dt", F.lit(dt))
        
        results.append(agg_result)
    
    # 合并所有结果
    if results:
        final_result = results[0]
        for i in range(1, len(results)):
            final_result = final_result.union(results[i])
    else:
        # 如果没有数据，创建空的DataFrame
        final_result = spark.createDataFrame([], spark.table("py_tms_dws.dws_trans_org_sort_1d").schema)

    # 验证数据量
    print_data_count(final_result, "dws_trans_org_sort_nd")

    # 写入数据
    final_result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_org_sort_nd")

    # 修复分区
    repair_hive_table("dws_trans_org_sort_nd")
    print("物流域机构粒度分拣 n 日汇总表 dws_trans_org_sort_nd 处理完成\n")

# ====================== 13. 物流域发单历史至今汇总表 ======================
def process_dws_trans_dispatch_td():
    print("开始处理物流域发单历史至今汇总表 dws_trans_dispatch_td")
    create_hive_table("dws_trans_dispatch_td")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_dispatch_td")

    # 读取1日汇总表数据
    detail_1d = spark.table("py_tms_dws.dws_trans_dispatch_1d").select(
        "order_count",
        "order_amount"
    )

    # 聚合处理
    result = detail_1d.agg(
        F.sum("order_count").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    ).withColumn("dt", F.lit(dt))

    # 验证数据量
    print_data_count(result, "dws_trans_dispatch_td")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_dispatch_td")

    # 修复分区
    repair_hive_table("dws_trans_dispatch_td")
    print("物流域发单历史至今汇总表 dws_trans_dispatch_td 处理完成\n")

# ====================== 14. 物流域转运完成历史至今汇总表 ======================
def process_dws_trans_bound_finish_td():
    print("开始处理物流域转运完成历史至今汇总表 dws_trans_bound_finish_td")
    create_hive_table("dws_trans_bound_finish_td")
    create_hdfs_dir("/warehouse/py_tms/dws/dws_trans_bound_finish_td")

    # 读取dwd层数据
    detail = spark.table("py_tms_dwd.dwd_trans_bound_finish_detail_inc").select(
        "order_id",
        "amount"
    )

    # 去重处理
    distinct_info = detail.groupBy("order_id").agg(
        F.max("amount").alias("order_amount")
    )

    # 聚合处理
    result = distinct_info.agg(
        F.count("order_id").alias("order_count"),
        F.sum("order_amount").alias("order_amount")
    ).withColumn("dt", F.lit(dt))

    # 验证数据量
    print_data_count(result, "dws_trans_bound_finish_td")

    # 写入数据
    result.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dws/dws_trans_bound_finish_td")

    # 修复分区
    repair_hive_table("dws_trans_bound_finish_td")
    print("物流域转运完成历史至今汇总表 dws_trans_bound_finish_td 处理完成\n")

# ====================== 主函数 ======================
if __name__ == "__main__":
    print("开始执行 TMS DWS 层汇总表处理任务")

    # 执行各个汇总表的处理
    process_dws_trade_org_cargo_type_order_1d()           # 1. 交易域机构货物类型粒度下单 1 日汇总表
    process_dws_trans_org_receive_1d()                    # 2. 物流域转运站粒度揽收 1 日汇总表
    process_dws_trans_dispatch_1d()                       # 3. 物流域发单 1 日汇总表
    process_dws_trans_org_truck_model_type_trans_finish_1d()  # 4. 物流域机构卡车类别粒度运输最近 1 日汇总表
    process_dws_trans_org_deliver_suc_1d()                # 5. 物流域转运站粒度派送成功 1 日汇总表
    process_dws_trans_org_sort_1d()                       # 6. 物流域机构粒度分拣 1 日汇总表
    process_dws_trade_org_cargo_type_order_nd()           # 7. 交易域机构货物类型粒度下单 n 日汇总表
    process_dws_trans_org_receive_nd()                    # 8. 物流域转运站粒度揽收 n 日汇总表
    process_dws_trans_dispatch_nd()                       # 9. 物流域发单 n 日汇总表
    process_dws_trans_shift_trans_finish_nd()             # 10. 物流域班次粒度转运完成最近 n 日汇总表
    process_dws_trans_org_deliver_suc_nd()                # 11. 物流域转运站粒度派送成功 n 日汇总表
    process_dws_trans_org_sort_nd()                       # 12. 物流域机构粒度分拣 n 日汇总表
    process_dws_trans_dispatch_td()                       # 13. 物流域发单历史至今汇总表
    process_dws_trans_bound_finish_td()                   # 14. 物流域转运完成历史至今汇总表

    print("所有 TMS DWS 层汇总表处理任务执行完成")

    # 关闭 SparkSession
    spark.stop()
