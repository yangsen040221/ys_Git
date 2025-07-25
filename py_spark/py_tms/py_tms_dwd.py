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

#导入 Java 类并获取 HDFS 文件系统实例
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.Path")
java_import(spark.sparkContext._gateway.jvm, "org.apache.hadoop.fs.FileSystem")

# 获取HDFS文件系统实例，用于后续HDFS操作
fs = spark.sparkContext._jvm.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())


# 使用数据库
spark.sql("use tms")

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
    spark.sql(f"MSCK REPAIR TABLE py_tms_dim.{table_name}")
    print(f"修复分区完成：py_tms_dim.{table_name}")

# 新增：打印数据量的函数（验证数据是否存在）
def print_data_count(df, table_name):
    count = df.count()
    print(f"{table_name} 处理后的数据量：{count} 行")
    return count














spark.stop()