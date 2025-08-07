import random
from faker import Faker
from datetime import datetime, timedelta
import pymysql
from pymysql.cursors import DictCursor
from tqdm import tqdm
import time

# 初始化Faker（中文数据生成）
fake = Faker('zh_CN')
Faker.seed(42)  # 固定种子，保证数据可复现
random.seed(42)

# MySQL 5.7连接配置（请根据实际环境修改）
MYSQL_CONFIG = {
    'host': 'cdh03',
    'port': 3306,
    'user': 'root',
    'password': 'root',  # 替换为实际密码
    'database': 'gd',
    'charset': 'utf8mb4'
}

# 批量插入批次大小
BATCH_SIZE = 1000

# 数据时间范围：最近一个月
END_DATE = datetime.now()
START_DATE = END_DATE - timedelta(days=30)


def get_db_connection():
    """获取数据库连接"""
    try:
        conn = pymysql.connect(
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=MYSQL_CONFIG['database'],
            charset=MYSQL_CONFIG['charset'],
            cursorclass=DictCursor
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        raise


def create_tables_if_not_exists():
    """创建ODS层所需表（仅ods_product_info和ods_user_behavior）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 创建商品基础信息表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `ods_product_info` (
              `product_id` INT NOT NULL COMMENT '商品ID',
              `product_name` VARCHAR(200) NOT NULL COMMENT '商品名称',
              `category_id` INT NOT NULL COMMENT '品类ID',
              `create_time` DATETIME NOT NULL COMMENT '创建时间',
              `is_valid` TINYINT DEFAULT 1 COMMENT '是否有效（1-有效 0-无效）',
              `etl_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL时间',
              PRIMARY KEY (`product_id`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '商品基础信息原始表';
            """)

            # 创建用户行为表
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `ods_user_behavior` (
              `id` BIGINT NOT NULL AUTO_INCREMENT COMMENT '记录ID',
              `user_id` INT NOT NULL COMMENT '用户ID',
              `main_product_id` INT NOT NULL COMMENT '主访问商品ID',
              `related_product_id` INT COMMENT '关联商品ID（同时操作的商品）',
              `behavior_type` TINYINT NOT NULL COMMENT '行为类型（1-访问 2-收藏 3-加购 4-支付）',
              `behavior_time` DATETIME NOT NULL COMMENT '行为时间',
              `is_guide_from_detail` TINYINT DEFAULT 0 COMMENT '是否从详情页引导（1-是 0-否）',
              `etl_time` DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'ETL时间',
              PRIMARY KEY (`id`),
              KEY `idx_main_product` (`main_product_id`),
              KEY `idx_behavior_time` (`behavior_time`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT '用户商品行为原始表';
            """)
            print("表结构创建/验证完成")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"表创建失败: {str(e)}")
        raise
    finally:
        conn.close()


def truncate_tables():
    """清空表数据（重新生成前初始化）"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            for table in ['ods_product_info', 'ods_user_behavior']:
                cursor.execute(f"TRUNCATE TABLE {table}")
                print(f"已清空表: {table}")
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"清空表失败: {str(e)}")
        raise
    finally:
        conn.close()


def batch_insert(table_name, data, columns):
    """批量插入数据到指定表"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            # 构建插入SQL
            placeholders = ', '.join(['%s'] * len(columns))
            sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            # 分批插入并显示进度
            total = len(data)
            for i in tqdm(range(0, total, BATCH_SIZE), desc=f"插入{table_name}"):
                batch = data[i:i+BATCH_SIZE]
                cursor.executemany(sql, batch)
                conn.commit()
                time.sleep(0.05)  # 控制插入速度
        print(f"成功插入 {total} 条数据到 {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"插入失败: {str(e)}")
        raise
    finally:
        conn.close()


def generate_ods_product_info(num_products=5000):
    """生成商品基础数据并插入ods_product_info"""
    products = []
    # 商品品类定义（ID与名称对应，可扩展）
    categories = {
        1: "电子产品", 2: "服装鞋帽", 3: "家居用品",
        4: "食品饮料", 5: "美妆个护", 6: "图书文具",
        7: "运动户外", 8: "母婴玩具"
    }
    category_ids = list(categories.keys())

    for i in range(num_products):
        product_id = 10000 + i  # 商品ID起始值（确保唯一）
        # 生成有业务意义的商品名称（品类+随机特征词）
        cid = random.choice(category_ids)
        product_name = f"{categories[cid]}-{fake.word()}{random.choice(['款', '系列', '版', '型'])}"

        # 生成创建时间（80%为近一个月新商品，20%为历史商品）
        if random.random() < 0.8:
            create_time = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        else:
            create_time = fake.date_time_between(start_date='-2y', end_date=START_DATE)

        # 商品有效性（95%有效，5%无效，模拟下架场景）
        is_valid = 1 if random.random() < 0.95 else 0

        products.append((
            product_id,
            product_name,
            cid,
            create_time.strftime('%Y-%m-%d %H:%M:%S'),
            is_valid
        ))

    # 插入数据（etl_time由数据库自动生成）
    columns = ['product_id', 'product_name', 'category_id', 'create_time', 'is_valid']
    batch_insert('ods_product_info', products, columns)
    return [p[0] for p in products]  # 返回商品ID列表，用于关联行为数据


def generate_ods_user_behavior(product_ids, num_records=100000):
    """生成用户行为数据并插入ods_user_behavior（无需用户表，直接生成合理user_id）"""
    behaviors = []
    # 行为类型分布（符合业务逻辑：访问>收藏≈加购>支付）
    behavior_types = [1, 2, 3, 4]
    behavior_probs = [0.6, 0.15, 0.15, 0.1]

    # 生成独立用户ID范围（10000个用户，ID从100000开始）
    user_ids = [100000 + i for i in range(10000)]

    for _ in range(num_records):
        user_id = random.choice(user_ids)  # 随机选择用户ID
        main_product_id = random.choice(product_ids)  # 关联商品ID

        # 关联商品ID（30%概率存在，模拟商品连带场景）
        related_product_id = random.choice(product_ids) if random.random() < 0.3 else None

        # 按概率分布选择行为类型
        behavior_type = random.choices(behavior_types, behavior_probs)[0]

        # 行为时间限定在最近一个月
        behavior_time = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)

        # 是否从详情页引导（20%概率）
        is_guide = 1 if random.random() < 0.2 else 0

        behaviors.append((
            user_id,
            main_product_id,
            related_product_id,
            behavior_type,
            behavior_time.strftime('%Y-%m-%d %H:%M:%S'),
            is_guide
        ))

    # 插入数据（id自增，etl_time自动生成）
    columns = [
        'user_id', 'main_product_id', 'related_product_id',
        'behavior_type', 'behavior_time', 'is_guide_from_detail'
    ]
    batch_insert('ods_user_behavior', behaviors, columns)


def main():
    print("开始生成ODS层数据（仅商品和行为表，最近一个月）...")

    # 1. 确保表结构存在
    create_tables_if_not_exists()

    # 2. 清空历史数据
    truncate_tables()

    # 3. 生成商品数据（5000个商品）
    product_ids = generate_ods_product_info(num_products=5000)

    # 4. 生成用户行为数据（10万条记录）
    generate_ods_user_behavior(product_ids, num_records=100000)

    print("ODS层数据生成完成！")


if __name__ == "__main__":
    # 依赖安装：pip install faker pymysql tqdm
    main()