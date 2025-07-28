from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("py_tms") \
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
    spark.sql(f"MSCK REPAIR TABLE py_tms_dwd.{table_name}")
    print(f"修复分区完成：py_tms_dwd.{table_name}")


# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count


# # 9.1 交易域下单事务事实表
# # 1. 创建HDFS目录
# create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trade_order_detail_inc")
#
# # 2. 删除旧表
# spark.sql("DROP TABLE IF EXISTS py_tms_dwd.dwd_trade_order_detail_inc")
#
# # 3. 创建外部表
# spark.sql("""
# CREATE EXTERNAL TABLE py_tms_dwd.dwd_trade_order_detail_inc (
#     `id`                   bigint comment '运单明细ID',
#     `order_id`             string COMMENT '运单ID',
#     `cargo_type`           string COMMENT '货物类型ID',
#     `cargo_type_name`      string COMMENT '货物类型名称',
#     `volumn_length`        bigint COMMENT '长cm',
#     `volumn_width`         bigint COMMENT '宽cm',
#     `volumn_height`        bigint COMMENT '高cm',
#     `weight`               decimal(16, 2) COMMENT '重量 kg',
#     `order_time`           string COMMENT '下单时间',
#     `order_no`             string COMMENT '运单号',
#     `status`               string COMMENT '运单状态',
#     `status_name`          string COMMENT '运单状态名称',
#     `collect_type`         string COMMENT '取件类型，1为网点自寄，2为上门取件',
#     `collect_type_name`    string COMMENT '取件类型名称',
#     `user_id`              bigint COMMENT '用户ID',
#     `receiver_complex_id`  bigint COMMENT '收件人小区id',
#     `receiver_province_id` string COMMENT '收件人省份id',
#     `receiver_city_id`     string COMMENT '收件人城市id',
#     `receiver_district_id` string COMMENT '收件人区县id',
#     `receiver_name`        string COMMENT '收件人姓名',
#     `sender_complex_id`    bigint COMMENT '发件人小区id',
#     `sender_province_id`   string COMMENT '发件人省份id',
#     `sender_city_id`       string COMMENT '发件人城市id',
#     `sender_district_id`   string COMMENT '发件人区县id',
#     `sender_name`          string COMMENT '发件人姓名',
#     `cargo_num`            bigint COMMENT '货物个数',
#     `amount`               decimal(16, 2) COMMENT '金额',
#     `estimate_arrive_time` string COMMENT '预计到达时间',
#     `distance`             decimal(16, 2) COMMENT '距离，单位：公里'
# ) comment '交易域订单明细事务事实表'
# PARTITIONED BY (`dt` string comment '统计日期')
# STORED AS ORC
# LOCATION '/warehouse/tms/dwd/dwd_trade_order_detail_inc'
# TBLPROPERTIES ('orc.compress' = 'snappy');
# """)
#
# # 4. 数据处理
# # 4.1 读取货物信息
# cargo_df = spark.table("tms.ods_order_cargo").filter(
#     (F.col("dt") == "20250718") & (F.col("is_deleted") == "0")
# ).select(
#     F.col("id"),
#     F.col("order_id"),
#     F.col("cargo_type"),
#     F.col("volume_length"),
#     F.col("volume_width"),
#     F.col("volume_height"),
#     F.col("weight"),
#     # 处理下单时间格式
#     F.concat(
#         F.substring(F.col("create_time"), 1, 10),
#         F.lit(" "),
#         F.substring(F.col("create_time"), 12, 8)
#     ).alias("order_time")
# )
#
# # 4.2 读取订单信息
# info_df = spark.table("tms.ods_order_info").filter(
#     (F.col("dt") == "20250718") & (F.col("is_deleted") == "0")
# ).select(
#     F.col("id"),
#     F.col("order_no"),
#     F.col("status"),
#     F.col("collect_type"),
#     F.col("user_id"),
#     F.col("receiver_complex_id"),
#     F.col("receiver_province_id"),
#     F.col("receiver_city_id"),
#     F.col("receiver_district_id"),
#     # 收件人姓名脱敏（保留首字符）
#     F.concat(F.substring(F.col("receiver_name"), 1, 1), F.lit("*")).alias("receiver_name"),
#     F.col("sender_complex_id"),
#     F.col("sender_province_id"),
#     F.col("sender_city_id"),
#     F.col("sender_district_id"),
#     # 发件人姓名脱敏
#     F.concat(F.substring(F.col("sender_name"), 1, 1), F.lit("*")).alias("sender_name"),
#     F.col("cargo_num"),
#     F.col("amount"),
#     # 处理预计到达时间
#     F.date_format(
#         F.from_utc_timestamp(F.col("estimate_arrive_time").cast(T.LongType()).cast(T.TimestampType()), "UTC"),
#         "yyyy-MM-dd HH:mm:ss"
#     ).alias("estimate_arrive_time"),
#     F.col("distance")
# )
#
# # 4.3 读取字典表（货物类型）
# cargo_type_dic = spark.table("tms.ods_base_dic").filter(
#     (F.col("dt") == "20250718") & (F.col("is_deleted") == "0")
# ).select(
#     F.col("id"),
#     F.col("name")
# )
#
# # 4.4 读取字典表（订单状态）
# status_dic = spark.table("tms.ods_base_dic").filter(
#     (F.col("dt") == "20250718") & (F.col("is_deleted") == "0")
# ).select(
#     F.col("id"),
#     F.col("name")
# )
#
# # 4.5 读取字典表（取件类型）
# collect_type_dic = spark.table("tms.ods_base_dic").filter(
#     (F.col("dt") == "20250718") & (F.col("is_deleted") == "0")
# ).select(
#     F.col("id"),
#     F.col("name")
# )
#
# # 5. 多表关联（修改dt生成逻辑）
# joined_df = cargo_df.alias("c") \
#     .join(info_df.alias("i"), F.col("c.order_id") == F.col("i.id"), "inner") \
#     .join(cargo_type_dic.alias("ct"), F.col("c.cargo_type") == F.col("ct.id").cast(T.StringType()), "left") \
#     .join(status_dic.alias("s"), F.col("i.status") == F.col("s.id").cast(T.StringType()), "left") \
#     .join(collect_type_dic.alias("colt"), F.col("i.collect_type") == F.col("colt.id").cast(T.StringType()), "left") \
#     .select(
#     F.col("c.id"),
#     F.col("c.order_id"),
#     F.col("c.cargo_type"),
#     F.coalesce(F.col("ct.name"), F.lit("")).alias("cargo_type_name"),
#     F.col("c.volume_length"),
#     F.col("c.volume_width"),
#     F.col("c.volume_height"),
#     F.col("c.weight"),
#     F.col("c.order_time"),
#     F.col("i.order_no"),
#     F.col("i.status"),
#     F.coalesce(F.col("s.name"), F.lit("")).alias("status_name"),
#     F.col("i.collect_type"),
#     F.coalesce(F.col("colt.name"), F.lit("")).alias("collect_type_name"),
#     F.col("i.user_id"),
#     F.col("i.receiver_complex_id"),
#     F.col("i.receiver_province_id"),
#     F.col("i.receiver_city_id"),
#     F.col("i.receiver_district_id"),
#     F.col("i.receiver_name"),
#     F.col("i.sender_complex_id"),
#     F.col("i.sender_province_id"),
#     F.col("i.sender_city_id"),
#     F.col("i.sender_district_id"),
#     F.col("i.sender_name"),
#     F.col("i.cargo_num"),
#     F.col("i.amount"),
#     F.col("i.estimate_arrive_time"),
#     F.col("i.distance"),
#     # 使用当前日期作为dt分区字段（格式：yyyy-MM-dd）
#     F.date_format(F.current_date(), "yyyy-MM-dd").alias("dt")
# )
#
# # 6. 验证数据量
# print_data_count(joined_df, "dwd_trade_order_detail_inc")
#
# # 7. 写入数据
# joined_df.write.mode("overwrite") \
#     .partitionBy("dt") \
#     .orc("/warehouse/py_tms/dwd/dwd_trade_order_detail_inc")
#
# # 8. 修复分区
# repair_hive_table("dwd_trade_order_detail_inc")


# 9.2 交易域支付成功事务事实表





spark.stop()
