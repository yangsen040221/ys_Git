from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from py4j.java_gateway import java_import

# 初始化SparkSession
spark = SparkSession.builder \
    .appName("py_tms_dwd") \
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
    spark.sql(f"MSCK REPAIR TABLE py_tms_dwd.{table_name}")
    print(f"修复分区完成：py_tms_dwd.{table_name}")

# 新增：创建Hive表的函数
def create_hive_table(table_name):
    table_schemas = {
        "dwd_trade_order_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trade_order_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `order_time` string COMMENT '下单时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '交易域订单明细事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trade_order_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trade_pay_suc_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trade_pay_suc_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `payment_time` string COMMENT '支付时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '交易域支付成功事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trade_pay_suc_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trade_order_cancel_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trade_order_cancel_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `cancel_time` string COMMENT '取消时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '交易域取消运单事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trade_order_cancel_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trans_receive_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trans_receive_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `receive_time` string COMMENT '揽收时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '物流域揽收事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trans_receive_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trans_dispatch_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trans_dispatch_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `dispatch_time` string COMMENT '发单时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '物流域发单事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trans_dispatch_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trans_bound_finish_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trans_bound_finish_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `bound_finish_time` string COMMENT '转运完成时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '物流域转运完成事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trans_bound_finish_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trans_deliver_suc_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trans_deliver_suc_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `deliver_suc_time` string COMMENT '派送成功时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '物流域派送成功事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trans_deliver_suc_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trans_sign_detail_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trans_sign_detail_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `sign_time` string COMMENT '签收时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里'
            ) COMMENT '物流域签收事务事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trans_sign_detail_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trade_order_process_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trade_order_process_inc(
                `id` bigint COMMENT '运单明细ID',
                `order_id` string COMMENT '运单ID',
                `cargo_type` string COMMENT '货物类型ID',
                `cargo_type_name` string COMMENT '货物类型名称',
                `volumn_length` bigint COMMENT '长cm',
                `volumn_width` bigint COMMENT '宽cm',
                `volumn_height` bigint COMMENT '高cm',
                `weight` decimal(16,2) COMMENT '重量 kg',
                `order_time` string COMMENT '下单时间',
                `order_no` string COMMENT '运单号',
                `status` string COMMENT '运单状态',
                `status_name` string COMMENT '运单状态名称',
                `collect_type` string COMMENT '取件类型，1为网点自寄，2为上门取件',
                `collect_type_name` string COMMENT '取件类型名称',
                `user_id` bigint COMMENT '用户ID',
                `receiver_complex_id` bigint COMMENT '收件人小区id',
                `receiver_province_id` string COMMENT '收件人省份id',
                `receiver_city_id` string COMMENT '收件人城市id',
                `receiver_district_id` string COMMENT '收件人区县id',
                `receiver_name` string COMMENT '收件人姓名',
                `sender_complex_id` bigint COMMENT '发件人小区id',
                `sender_province_id` string COMMENT '发件人省份id',
                `sender_city_id` string COMMENT '发件人城市id',
                `sender_district_id` string COMMENT '发件人区县id',
                `sender_name` string COMMENT '发件人姓名',
                `payment_type` string COMMENT '支付类型',
                `payment_type_name` string COMMENT '支付类型名称',
                `cargo_num` bigint COMMENT '货物个数',
                `amount` decimal(16,2) COMMENT '金额',
                `estimate_arrive_time` string COMMENT '预计到达时间',
                `distance` decimal(16,2) COMMENT '距离，单位：公里',
                `start_date` string COMMENT '开始日期',
                `end_date` string COMMENT '结束日期'
            ) COMMENT '交易域运单累积快照事实表'
            PARTITIONED BY (`dt_partition` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_order_process'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_trans_trans_finish_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_trans_trans_finish_inc(
                `id` bigint COMMENT '运输任务ID',
                `shift_id` bigint COMMENT '班次ID',
                `line_id` bigint COMMENT '线路ID',
                `start_org_id` bigint COMMENT '起始机构ID',
                `start_org_name` string COMMENT '起始机构名称',
                `end_org_id` bigint COMMENT '目标机构ID',
                `end_org_name` string COMMENT '目标机构名称',
                `order_num` bigint COMMENT '运单数量',
                `driver1_emp_id` bigint COMMENT '司机1ID',
                `driver1_name` string COMMENT '司机1名称',
                `driver2_emp_id` bigint COMMENT '司机2ID',
                `driver2_name` string COMMENT '司机2名称',
                `truck_id` bigint COMMENT '卡车ID',
                `truck_no` string COMMENT '卡车号',
                `actual_start_time` string COMMENT '实际起始时间',
                `actual_end_time` string COMMENT '实际结束时间',
                `estimate_end_time` string COMMENT '预计结束时间',
                `actual_distance` decimal(16,2) COMMENT '实际距离',
                `finish_dur_sec` bigint COMMENT '运输时长，单位：秒'
            ) COMMENT '物流域运输完成事务事实表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_trans_trans_finish_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_bound_inbound_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_bound_inbound_inc(
                `id` bigint COMMENT '中转记录ID',
                `order_id` bigint COMMENT '运单ID',
                `org_id` bigint COMMENT '机构ID',
                `inbound_time` string COMMENT '入库时间',
                `inbound_emp_id` bigint COMMENT '入库人员'
            ) COMMENT '中转域入库事务事实表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_bound_inbound_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_bound_sort_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_bound_sort_inc(
                `id` bigint COMMENT '中转记录ID',
                `order_id` bigint COMMENT '订单ID',
                `org_id` bigint COMMENT '机构ID',
                `sort_time` string COMMENT '分拣时间',
                `sorter_emp_id` bigint COMMENT '分拣人员'
            ) COMMENT '中转域分拣事务事实表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_bound_sort_inc'
            TBLPROPERTIES('orc.compress' = 'snappy')
        """,
        "dwd_bound_outbound_inc": """
            CREATE EXTERNAL TABLE IF NOT EXISTS py_tms_dwd.dwd_bound_outbound_inc(
                `id` bigint COMMENT '中转记录ID',
                `order_id` bigint COMMENT '订单ID',
                `org_id` bigint COMMENT '机构ID',
                `outbound_time` string COMMENT '出库时间',
                `outbound_emp_id` bigint COMMENT '出库人员'
            ) COMMENT '中转域出库事务事实表'
            PARTITIONED BY (`dt` string COMMENT '统计日期')
            STORED AS ORC
            LOCATION '/warehouse/py_tms/dwd/dwd_bound_outbound_inc'
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

# ====================== 1. 交易域下单事务事实表 dwd_trade_order_detail_inc ======================
def process_dwd_trade_order_detail_inc():
    print("开始处理交易域下单事务事实表 dwd_trade_order_detail_inc")
    create_hive_table("dwd_trade_order_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trade_order_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        F.concat(F.substring("create_time", 1, 10), F.lit(" "), F.substring("create_time", 12, 8)).alias("order_time"),
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance"
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "left") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            cargo.order_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("order_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trade_order_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trade_order_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trade_order_detail_inc")
    print("交易域下单事务事实表 dwd_trade_order_detail_inc 处理完成\n")

# ====================== 2. 交易域支付成功事务事实表 dwd_trade_pay_suc_detail_inc ======================
def process_dwd_trade_pay_suc_detail_inc():
    print("开始处理交易域支付成功事务事实表 dwd_trade_pay_suc_detail_inc")
    create_hive_table("dwd_trade_pay_suc_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trade_pay_suc_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) &
        (F.col("is_deleted") == "0") &
        (F.col("status") != "60010") &
        (F.col("status") != "60999")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("payment_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.payment_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("payment_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trade_pay_suc_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trade_pay_suc_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trade_pay_suc_detail_inc")
    print("交易域支付成功事务事实表 dwd_trade_pay_suc_detail_inc 处理完成\n")

# ====================== 3. 交易域取消运单事务事实表 dwd_trade_order_cancel_detail_inc ======================
def process_dwd_trade_order_cancel_detail_inc():
    print("开始处理交易域取消运单事务事实表 dwd_trade_order_cancel_detail_inc")
    create_hive_table("dwd_trade_order_cancel_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trade_order_cancel_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("cancel_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.cancel_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("cancel_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trade_order_cancel_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trade_order_cancel_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trade_order_cancel_detail_inc")
    print("交易域取消运单事务事实表 dwd_trade_order_cancel_detail_inc 处理完成\n")

# ====================== 4. 物流域揽收事务事实表 dwd_trans_receive_detail_inc ======================
def process_dwd_trans_receive_detail_inc():
    print("开始处理物流域揽收事务事实表 dwd_trans_receive_detail_inc")
    create_hive_table("dwd_trans_receive_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trans_receive_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) &
        (F.col("is_deleted") == "0") &
        (F.col("status") != "60010") &
        (F.col("status") != "60020") &
        (F.col("status") != "60999")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("receive_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.receive_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("receive_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trans_receive_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trans_receive_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trans_receive_detail_inc")
    print("物流域揽收事务事实表 dwd_trans_receive_detail_inc 处理完成\n")

# ====================== 5. 物流域发单事务事实表 dwd_trans_dispatch_detail_inc ======================
def process_dwd_trans_dispatch_detail_inc():
    print("开始处理物流域发单事务事实表 dwd_trans_dispatch_detail_inc")
    create_hive_table("dwd_trans_dispatch_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trans_dispatch_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) &
        (F.col("is_deleted") == "0") &
        (F.col("status") != "60010") &
        (F.col("status") != "60020") &
        (F.col("status") != "60030") &
        (F.col("status") != "60040") &
        (F.col("status") != "60999")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("dispatch_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.dispatch_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("dispatch_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trans_dispatch_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trans_dispatch_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trans_dispatch_detail_inc")
    print("物流域发单事务事实表 dwd_trans_dispatch_detail_inc 处理完成\n")

# ====================== 6. 物流域转运完成事务事实表 dwd_trans_bound_finish_detail_inc ======================
def process_dwd_trans_bound_finish_detail_inc():
    print("开始处理物流域转运完成事务事实表 dwd_trans_bound_finish_detail_inc")
    create_hive_table("dwd_trans_bound_finish_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trans_bound_finish_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("bound_finish_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.bound_finish_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("bound_finish_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trans_bound_finish_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trans_bound_finish_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trans_bound_finish_detail_inc")
    print("物流域转运完成事务事实表 dwd_trans_bound_finish_detail_inc 处理完成\n")

# ====================== 7. 物流域派送成功事务事实表 dwd_trans_deliver_suc_detail_inc ======================
def process_dwd_trans_deliver_suc_detail_inc():
    print("开始处理物流域派送成功事务事实表 dwd_trans_deliver_suc_detail_inc")
    create_hive_table("dwd_trans_deliver_suc_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trans_deliver_suc_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("deliver_suc_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.deliver_suc_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("deliver_suc_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trans_deliver_suc_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trans_deliver_suc_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trans_deliver_suc_detail_inc")
    print("物流域派送成功事务事实表 dwd_trans_deliver_suc_detail_inc 处理完成\n")

# ====================== 8. 物流域签收事务事实表 dwd_trans_sign_detail_inc ======================
def process_dwd_trans_sign_detail_inc():
    print("开始处理物流域签收事务事实表 dwd_trans_sign_detail_inc")
    create_hive_table("dwd_trans_sign_detail_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trans_sign_detail_inc")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.concat(F.substring("update_time", 1, 10), F.lit(" "), F.substring("update_time", 12, 8)).alias("sign_time")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            info.sign_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("sign_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trans_sign_detail_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_trans_sign_detail_inc")

    # 修复分区
    repair_hive_table("dwd_trans_sign_detail_inc")
    print("物流域签收事务事实表 dwd_trans_sign_detail_inc 处理完成\n")

# ====================== 9. 交易域运单累积快照事实表 dwd_trade_order_process_inc ======================
def process_dwd_trade_order_process_inc():
    print("开始处理交易域运单累积快照事实表 dwd_trade_order_process_inc")
    create_hive_table("dwd_trade_order_process_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_order_process")

    # 读取货物信息
    cargo = spark.table("tms.ods_order_cargo").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_id",
        "cargo_type",
        "volume_length",
        "volume_width",
        "volume_height",
        "weight",
        F.concat(F.substring("create_time", 1, 10), F.lit(" "), F.substring("create_time", 12, 8)).alias("order_time"),
        "dt"
    )

    # 读取订单信息
    info = spark.table("tms.ods_order_info").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        "id",
        "order_no",
        "status",
        "collect_type",
        "user_id",
        "receiver_complex_id",
        "receiver_province_id",
        "receiver_city_id",
        "receiver_district_id",
        F.concat(F.substring("receiver_name", 1, 1), F.lit("*")).alias("receiver_name"),
        "sender_complex_id",
        "sender_province_id",
        "sender_city_id",
        "sender_district_id",
        F.concat(F.substring("sender_name", 1, 1), F.lit("*")).alias("sender_name"),
        "payment_type",
        "cargo_num",
        "amount",
        F.from_unixtime(F.col("estimate_arrive_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("estimate_arrive_time"),
        "distance",
        F.when(
            (F.col("status") == "60080") | (F.col("status") == "60999"),
            F.substring("update_time", 1, 10)
        ).otherwise("9999-12-31").alias("end_date")
    )

    # 读取字典信息
    dic_for_cargo_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "cargo_type_name")

    dic_for_status = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "status_name")

    dic_for_collect_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "collect_type_name")

    dic_for_payment_type = spark.table("tms.ods_base_dic").filter(
        (F.col("dt") == dt) & (F.col("is_deleted") == "0")
    ).select(
        F.col("id").cast(T.StringType()).alias("id"),
        "name"
    ).withColumnRenamed("name", "payment_type_name")

    # 关联处理
    joined = cargo \
        .join(info, cargo.order_id == info.id, "inner") \
        .join(dic_for_cargo_type, cargo.cargo_type == dic_for_cargo_type.id, "left") \
        .join(dic_for_status, info.status == dic_for_status.id, "left") \
        .join(dic_for_collect_type, info.collect_type == dic_for_collect_type.id, "left") \
        .join(dic_for_payment_type, info.payment_type == dic_for_payment_type.id, "left") \
        .select(
            cargo.id,
            cargo.order_id,
            cargo.cargo_type,
            F.col("cargo_type_name"),
            cargo.volume_length,
            cargo.volume_width,
            cargo.volume_height,
            cargo.weight,
            cargo.order_time,
            info.order_no,
            info.status,
            F.col("status_name"),
            info.collect_type,
            F.col("collect_type_name"),
            info.user_id,
            info.receiver_complex_id,
            info.receiver_province_id,
            info.receiver_city_id,
            info.receiver_district_id,
            info.receiver_name,
            info.sender_complex_id,
            info.sender_province_id,
            info.sender_city_id,
            info.sender_district_id,
            info.sender_name,
            info.payment_type,
            F.col("payment_type_name"),
            info.cargo_num,
            info.amount,
            info.estimate_arrive_time,
            info.distance,
            cargo.dt,
            F.date_format("order_time", "yyyy-MM-dd").alias("start_date"),
            F.col("end_date"),
            F.date_format("order_time", "yyyy-MM-dd").alias("dt_partition")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trade_order_process_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt_partition") \
        .orc("/warehouse/py_tms/dwd/dwd_order_process")

    # 修复分区
    repair_hive_table("dwd_trade_order_process_inc")
    print("交易域运单累积快照事实表 dwd_trade_order_process_inc 处理完成\n")

# ====================== 10. 物流域运输完成事务事实表 dwd_trans_trans_finish_inc ======================
def process_dwd_trans_trans_finish_inc():
    print("开始处理物流域运输完成事务事实表 dwd_trans_trans_finish_inc")
    create_hive_table("dwd_trans_trans_finish_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_trans_trans_finish_inc")

    # 读取运输任务信息
    info = spark.table("tms.ods_transport_task").filter(
        (F.col("dt") == dt) &
        (F.col("is_deleted") == "0") &
        (F.col("actual_end_time").isNotNull())
    ).select(
        "id",
        "shift_id",
        "line_id",
        "start_org_id",
        "start_org_name",
        "end_org_id",
        "end_org_name",
        "order_num",
        "driver1_emp_id",
        F.concat(F.substring("driver1_name", 1, 1), F.lit("*")).alias("driver1_name"),
        "driver2_emp_id",
        F.concat(F.substring("driver2_name", 1, 1), F.lit("*")).alias("driver2_name"),
        "truck_id",
        F.md5("truck_no").alias("truck_no"),
        F.from_unixtime(F.col("actual_start_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("actual_start_time"),
        F.from_unixtime(F.col("actual_end_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("actual_end_time"),
        F.col("actual_distance"),
        ((F.col("actual_end_time").cast(T.LongType()) - F.col("actual_start_time").cast(T.LongType())) / 1000).alias("finish_dur_sec"),
        F.from_unixtime(F.col("actual_end_time") / 1000, 'yyyy-MM-dd').alias("dt1")
    )

    # 读取班次维度表
    dim_tb = spark.table("tms_dim.dim_shift_full").filter(
        F.col("dt") == dt
    ).select(
        "id",
        "estimated_time"
    )

    # 关联处理
    joined = info \
        .join(dim_tb, info.shift_id == dim_tb.id, "left") \
        .select(
            info.id,
            info.shift_id,
            info.line_id,
            info.start_org_id,
            info.start_org_name,
            info.end_org_id,
            info.end_org_name,
            info.order_num,
            info.driver1_emp_id,
            info.driver1_name,
            info.driver2_emp_id,
            info.driver2_name,
            info.truck_id,
            info.truck_no,
            info.actual_start_time,
            info.actual_end_time,
            F.col("estimated_time").alias("estimate_end_time"),
            info.actual_distance,
            info.finish_dur_sec,
            F.lit(dt).alias("dt")
        )

    # 验证数据量
    print_data_count(joined, "dwd_trans_trans_finish_inc")

    # 写入数据
    joined.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dwd/dwd_trans_trans_finish_inc")

    # 修复分区
    repair_hive_table("dwd_trans_trans_finish_inc")
    print("物流域运输完成事务事实表 dwd_trans_trans_finish_inc 处理完成\n")

# ====================== 11. 中转域入库事务事实表 dwd_bound_inbound_inc ======================
def process_dwd_bound_inbound_inc():
    print("开始处理中转域入库事务事实表 dwd_bound_inbound_inc")
    create_hive_table("dwd_bound_inbound_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_bound_inbound_inc")

    # 读取中转记录信息
    inbound_df = spark.table("tms.ods_order_org_bound").filter(
        F.col("dt") == dt
    ).select(
        "id",
        "order_id",
        "org_id",
        F.from_unixtime(F.col("inbound_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("inbound_time"),
        "inbound_emp_id",
        "dt"
    )

    # 验证数据量
    print_data_count(inbound_df, "dwd_bound_inbound_inc")

    # 写入数据
    inbound_df.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dwd/dwd_bound_inbound_inc")

    # 修复分区
    repair_hive_table("dwd_bound_inbound_inc")
    print("中转域入库事务事实表 dwd_bound_inbound_inc 处理完成\n")

# ====================== 12. 中转域分拣事务事实表 dwd_bound_sort_inc ======================
def process_dwd_bound_sort_inc():
    print("开始处理中转域分拣事务事实表 dwd_bound_sort_inc")
    create_hive_table("dwd_bound_sort_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_bound_sort_inc")

    # 读取中转记录信息
    sort_df = spark.table("tms.ods_order_org_bound").filter(
        (F.col("dt") == dt) &
        (F.col("sort_time").isNotNull())
    ).select(
        "id",
        "order_id",
        "org_id",
        F.from_unixtime(F.col("sort_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("sort_time"),
        "sorter_emp_id",
        "dt"
    )

    # 验证数据量
    print_data_count(sort_df, "dwd_bound_sort_inc")

    # 写入数据
    sort_df.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dwd/dwd_bound_sort_inc")

    # 修复分区
    repair_hive_table("dwd_bound_sort_inc")
    print("中转域分拣事务事实表 dwd_bound_sort_inc 处理完成\n")

# ====================== 13. 中转域出库事务事实表 dwd_bound_outbound_inc ======================
def process_dwd_bound_outbound_inc():
    print("开始处理中转域出库事务事实表 dwd_bound_outbound_inc")
    create_hive_table("dwd_bound_outbound_inc")
    create_hdfs_dir("/warehouse/py_tms/dwd/dwd_bound_outbound_inc")

    # 读取中转记录信息
    outbound_df = spark.table("tms.ods_order_org_bound").filter(
        (F.col("dt") == dt) &
        (F.col("outbound_time").isNotNull())
    ).select(
        "id",
        "order_id",
        "org_id",
        F.from_unixtime(F.col("outbound_time") / 1000, 'yyyy-MM-dd HH:mm:ss').alias("outbound_time"),
        "outbound_emp_id",
        "dt"
    )

    # 验证数据量
    print_data_count(outbound_df, "dwd_bound_outbound_inc")

    # 写入数据
    outbound_df.write.mode("overwrite") \
        .partitionBy("dt") \
        .orc("/warehouse/py_tms/dwd/dwd_bound_outbound_inc")

    # 修复分区
    repair_hive_table("dwd_bound_outbound_inc")
    print("中转域出库事务事实表 dwd_bound_outbound_inc 处理完成\n")

# ====================== 主函数 ======================
if __name__ == "__main__":
    print("开始执行 TMS DWD 层事实表处理任务")

    # 执行各个事实表的处理
    process_dwd_trade_order_detail_inc()           # 1. 交易域下单事务事实表
    process_dwd_trade_pay_suc_detail_inc()         # 2. 交易域支付成功事务事实表
    process_dwd_trade_order_cancel_detail_inc()    # 3. 交易域取消运单事务事实表
    process_dwd_trans_receive_detail_inc()         # 4. 物流域揽收事务事实表
    process_dwd_trans_dispatch_detail_inc()        # 5. 物流域发单事务事实表
    process_dwd_trans_bound_finish_detail_inc()    # 6. 物流域转运完成事务事实表
    process_dwd_trans_deliver_suc_detail_inc()     # 7. 物流域派送成功事务事实表
    process_dwd_trans_sign_detail_inc()            # 8. 物流域签收事务事实表
    process_dwd_trade_order_process_inc()          # 9. 交易域运单累积快照事实表
    process_dwd_trans_trans_finish_inc()           # 10. 物流域运输完成事务事实表
    process_dwd_bound_inbound_inc()                # 11. 中转域入库事务事实表
    process_dwd_bound_sort_inc()                   # 12. 中转域分拣事务事实表
    process_dwd_bound_outbound_inc()               # 13. 中转域出库事务事实表

    print("所有 TMS DWD 层事实表处理任务执行完成")

    # 关闭 SparkSession
    spark.stop()