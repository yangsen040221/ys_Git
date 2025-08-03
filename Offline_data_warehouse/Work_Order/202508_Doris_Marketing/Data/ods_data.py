import pandas as pd
import numpy as np
import random
from faker import Faker
from datetime import datetime, timedelta
import pymysql
from pymysql.cursors import DictCursor
from tqdm import tqdm
import time

# 初始化Faker
fake = Faker('zh_CN')
Faker.seed(42)
random.seed(42)

# Doris数据库连接配置 - 请根据实际环境修改
DORIS_CONFIG = {
    'host': 'cdh01',
    'port': 9031,  # 默认查询端口
    'user': 'root',
    'password': '',
    'database': 'gd',  # ODS层数据库
    'charset': 'utf8mb4'
}

# 批量插入的批次大小
BATCH_SIZE = 1000

def get_db_connection():
    """获取数据库连接"""
    try:
        conn = pymysql.connect(
            host=DORIS_CONFIG['host'],
            port=DORIS_CONFIG['port'],
            user=DORIS_CONFIG['user'],
            password=DORIS_CONFIG['password'],
            database=DORIS_CONFIG['database'],
            charset=DORIS_CONFIG['charset'],
            cursorclass=DictCursor
        )
        return conn
    except Exception as e:
        print(f"数据库连接失败: {str(e)}")
        raise

def truncate_tables():
    """清空ODS层表，为新数据做准备"""
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            tables = [
                'ods_user',
                'ods_marketing_activity',
                'ods_user_behavior',
                'ods_marketing_effect'
            ]
            for table in tables:
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
            columns_str = ', '.join(columns)
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

            # 分批插入
            total = len(data)
            for i in tqdm(range(0, total, BATCH_SIZE), desc=f"插入{table_name}"):
                batch = data[i:i+BATCH_SIZE]
                cursor.executemany(sql, batch)
                conn.commit()
                # 避免插入过快
                time.sleep(0.1)

        print(f"成功插入 {total} 条数据到 {table_name}")
    except Exception as e:
        conn.rollback()
        print(f"插入数据失败: {str(e)}")
        raise
    finally:
        conn.close()

# 生成用户数据并插入
def generate_and_insert_users(num=10000):
    users = []
    channels = ['app', 'website', 'wechat', 'alipay', 'other']
    cities = ['北京', '上海', '广州', '深圳', '杭州', '成都', '武汉', '南京', '重庆', '西安']

    for i in range(num):
        user_id = 100000 + i
        register_time = fake.date_time_between(start_date='-3y', end_date='now')

        user = (
            user_id,
            fake.name(),
            random.choice([1, 2, 0]),
            random.randint(18, 65),
            random.choice(cities),
            register_time.strftime('%Y-%m-%d %H:%M:%S'),
            random.choice(channels),
            fake.phone_number(),
            fake.email()
        )
        users.append(user)

    # 插入到ODS表
    columns = ['user_id', 'user_name', 'gender', 'age', 'city',
               'register_time', 'channel', 'mobile', 'email']
    batch_insert('ods_user', users, columns)

    # 返回用户ID列表供其他表使用
    return [user[0] for user in users]

# 生成营销活动数据并插入
def generate_and_insert_activities(num=50):
    activities = []
    activity_types = ['discount', 'gift', 'lottery', 'membership', 'new_product']
    channels = ['app', 'website', 'wechat', 'alipay', 'offline']

    for i in range(num):
        activity_id = 200000 + i
        start_time = fake.date_time_between(start_date='-1y', end_date='now')
        end_time = start_time + timedelta(days=random.randint(3, 30))

        # 确定活动状态
        now = datetime.now()
        if end_time < now:
            status = 2  # 已结束
        elif start_time > now:
            status = 0  # 未开始
        else:
            status = 1  # 进行中

        activity = (
            activity_id,
            f'活动{i+1}:{fake.word()}{random.choice(["促销", "优惠", "活动", "盛典"])}',
            random.choice(activity_types),
            start_time.strftime('%Y-%m-%d %H:%M:%S'),
            end_time.strftime('%Y-%m-%d %H:%M:%S'),
            status,
            random.choice(channels),
            round(random.uniform(10000, 500000), 2)
        )
        activities.append(activity)

    # 插入到ODS表
    columns = ['activity_id', 'activity_name', 'activity_type', 'start_time',
               'end_time', 'status', 'channel', 'budget']
    batch_insert('ods_marketing_activity', activities, columns)

    # 返回活动ID和时间信息供其他表使用
    return [(a[0], a[3], a[4]) for a in activities]

# 生成用户行为数据并插入
def generate_and_insert_user_behavior(user_ids, activities, num=50000):
    behaviors = []
    behavior_types = ['click', 'view', 'share', 'participate']
    pages = ['home', 'detail', 'cart', 'checkout', 'activity', 'profile']
    devices = ['android', 'ios', 'pc', 'mac', 'other']

    # 转换活动时间为datetime对象以便计算
    activity_info = []
    for act in activities:
        activity_id, start_str, end_str = act
        start_time = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
        activity_info.append((activity_id, start_time, end_time))

    for i in range(num):
        behavior_id = 3000000 + i
        user_id = random.choice(user_ids)
        activity_id, start_time, end_time = random.choice(activity_info)

        # 确保行为时间在活动期间内或结束后不久
        time_diff = (end_time - start_time).total_seconds()
        if time_diff > 0:
            random_seconds = random.randint(0, int(time_diff * 1.2))  # 允许20%的时间在活动结束后
            behavior_time = start_time + timedelta(seconds=random_seconds)
        else:
            behavior_time = start_time

        behavior = (
            behavior_id,
            user_id,
            activity_id,
            random.choice(behavior_types),
            behavior_time.strftime('%Y-%m-%d %H:%M:%S'),
            random.choice(pages),
            random.choice(devices),
            fake.ipv4()
        )
        behaviors.append(behavior)

    # 插入到ODS表
    columns = ['behavior_id', 'user_id', 'activity_id', 'behavior_type',
               'behavior_time', 'page', 'device', 'ip']
    batch_insert('ods_user_behavior', behaviors, columns)

# 生成营销效果数据并插入
def generate_and_insert_marketing_effect(user_ids, activities, num=10000):
    effects = []
    conversion_types = ['purchase', 'register', 'download', 'subscribe', 'upgrade']

    # 转换活动时间为datetime对象以便计算
    activity_info = []
    for act in activities:
        activity_id, start_str, end_str = act
        start_time = datetime.strptime(start_str, '%Y-%m-%d %H:%M:%S')
        end_time = datetime.strptime(end_str, '%Y-%m-%d %H:%M:%S')
        activity_info.append((activity_id, start_time, end_time))

    for i in range(num):
        effect_id = 4000000 + i
        user_id = random.choice(user_ids)
        activity_id, start_time, end_time = random.choice(activity_info)

        # 确保转化时间在活动期间内
        time_diff = (end_time - start_time).total_seconds()
        if time_diff > 0:
            random_seconds = random.randint(0, int(time_diff))
            conversion_time = start_time + timedelta(seconds=random_seconds)
        else:
            conversion_time = start_time

        # 只有购买类型有金额
        conversion_type = random.choice(conversion_types)
        amount = round(random.uniform(10, 5000), 2) if conversion_type == 'purchase' else 0

        effect = (
            effect_id,
            activity_id,
            user_id,
            conversion_time.strftime('%Y-%m-%d %H:%M:%S'),
            conversion_type,
            amount
        )
        effects.append(effect)

    # 插入到ODS表
    columns = ['effect_id', 'activity_id', 'user_id', 'conversion_time',
               'conversion_type', 'amount']
    batch_insert('ods_marketing_effect', effects, columns)

# 主函数
def main():
    print("开始准备Doris ODS层数据生成...")

    # 首先始化 - 清空表
    print("清空现有ODS层表数据...")
    truncate_tables()

    # 生成并插入用户数据
    print("开始生成并插入用户数据...")
    user_ids = generate_and_insert_users(10000)

    # 生成并插入营销活动数据
    print("开始生成并插入营销活动数据...")
    activities = generate_and_insert_activities(50)

    # 生成并插入用户行为数据
    print("开始生成并插入用户行为数据...")
    generate_and_insert_user_behavior(user_ids, activities, 50000)

    # 生成并插入营销效果数据
    print("开始生成并插入营销效果数据...")
    generate_and_insert_marketing_effect(user_ids, activities, 10000)

    print("所有数据已成功成生成并写入Doris ODS层表中！")

if __name__ == "__main__":
    # 需要安装的依赖：pip install pandas faker pymysql tqdm
    main()
