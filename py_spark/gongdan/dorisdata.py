import random
import datetime
import csv
from faker import Faker

# 初始化Faker
fake = Faker('zh_CN')

# 配置参数
START_DATE = datetime.date(2025, 7, 1)
END_DATE = datetime.date(2025, 7, 10)
USER_COUNT = 1000
CAMPAIGN_COUNT = 10
PRODUCT_COUNT = 50
CHANNELS = [1, 2, 3, 4, 5]  # 渠道ID列表
BEHAVIOR_TYPES = ['view', 'click', 'add_cart', 'purchase']
DEVICE_TYPES = ['mobile', 'pc', 'tablet']

def generate_user_info():
    """生成用户信息数据"""
    users = []
    for user_id in range(1, USER_COUNT + 1):
        register_time = fake.date_time_between(start_date='-1y', end_date='now')
        user = {
            'user_id': user_id,
            'user_name': fake.user_name(),
            'gender': random.choice(['男', '女', '未知']),
            'age': random.randint(18, 65),
            'city': fake.city(),
            'register_time': register_time.strftime('%Y-%m-%d %H:%M:%S'),
            'register_channel': random.choice(CHANNELS),
            'user_level': random.randint(1, 5),
            'status': '正常',
            'update_time': fake.date_time_between(start_date=register_time).strftime('%Y-%m-%d %H:%M:%S'),
            'dt': datetime.date.today().strftime('%Y-%m-%d')
        }
        users.append(user)
    return users

def generate_campaign_info():
    """生成营销活动数据"""
    campaigns = []
    for campaign_id in range(1, CAMPAIGN_COUNT + 1):
        start_time = fake.date_time_between(start_date=START_DATE, end_date=END_DATE)
        end_time = fake.date_time_between(start_date=start_time, end_date=END_DATE + datetime.timedelta(days=7))
        campaign = {
            'campaign_id': campaign_id,
            'campaign_name': f'营销活动{campaign_id}',
            'campaign_type': random.choice(['限时折扣', '满减活动', '新品推广', '节日促销']),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
            'budget': round(random.uniform(10000, 100000), 2),
            'status': '进行中',
            'create_time': fake.date_time_between(start_date=start_time - datetime.timedelta(days=30),
                                                  end_date=start_time).strftime('%Y-%m-%d %H:%M:%S'),
            'update_time': fake.date_time_between(start_date=start_time).strftime('%Y-%m-%d %H:%M:%S'),
            'dt': datetime.date.today().strftime('%Y-%m-%d')
        }
        campaigns.append(campaign)
    return campaigns

def generate_user_behavior_log():
    """生成用户行为日志数据"""
    behaviors = []
    current_date = START_DATE

    while current_date <= END_DATE:
        # 每天生成10000条行为记录
        for _ in range(10000):
            user_id = random.randint(1, USER_COUNT)
            behavior_type = random.choices(
                BEHAVIOR_TYPES,
                weights=[0.5, 0.3, 0.15, 0.05]  # 权重偏向浏览和点击
            )[0]

            # 随机生成事件时间
            event_hour = random.randint(8, 22)  # 主要活动时间在8:00-22:00
            event_minute = random.randint(0, 59)
            event_second = random.randint(0, 59)

            event_time = datetime.datetime(
                current_date.year, current_date.month, current_date.day,
                event_hour, event_minute, event_second
            )

            # 50%的行为关联营销活动
            campaign_id = random.randint(1, CAMPAIGN_COUNT) if random.random() < 0.5 else None

            behavior = {
                'user_id': user_id,
                'behavior_type': behavior_type,
                'page_id': f'page_{random.randint(1, 20)}',
                'product_id': random.randint(1, PRODUCT_COUNT),
                'campaign_id': campaign_id,
                'channel_id': random.choice(CHANNELS),
                'device_type': random.choice(DEVICE_TYPES),
                'ip_address': fake.ipv4(),
                'user_agent': fake.user_agent(),
                'event_time': event_time.strftime('%Y-%m-%d %H:%M:%S'),
                'dt': current_date.strftime('%Y-%m-%d')
            }
            behaviors.append(behavior)

        current_date += datetime.timedelta(days=1)

    return behaviors

def generate_order_info():
    """生成订单数据"""
    orders = []
    order_id = 1
    current_date = START_DATE

    while current_date <= END_DATE:
        # 每天生成500-1000个订单
        order_count = random.randint(500, 1000)

        for _ in range(order_count):
            user_id = random.randint(1, USER_COUNT)
            product_id = random.randint(1, PRODUCT_COUNT)
            order_amount = round(random.uniform(50, 2000), 2)
            discount_rate = random.uniform(0.8, 1.0)  # 折扣率
            pay_amount = round(order_amount * discount_rate, 2)
            discount_amount = round(order_amount - pay_amount, 2)

            # 30%的订单关联营销活动
            campaign_id = random.randint(1, CAMPAIGN_COUNT) if random.random() < 0.3 else None

            create_time = fake.date_time_between(
                start_date=datetime.datetime(current_date.year, current_date.month, current_date.day, 8, 0, 0),
                end_date=datetime.datetime(current_date.year, current_date.month, current_date.day, 22, 0, 0)
            )

            # 支付时间在创建时间之后，90%的订单会支付
            if random.random() < 0.75:
                pay_time = fake.date_time_between(
                    start_date=create_time,
                    end_date=create_time + datetime.timedelta(hours=2)
                ).strftime('%Y-%m-%d %H:%M:%S')
                order_status = '已支付'
            else:
                pay_time = None
                order_status = '未支付'

            order = {
                'order_id': order_id,
                'user_id': user_id,
                'product_id': product_id,
                'product_name': f'商品{product_id}',
                'order_amount': order_amount,
                'pay_amount': pay_amount if order_status == '已支付' else 0,
                'discount_amount': discount_amount,
                'campaign_id': campaign_id,
                'channel_id': random.choice(CHANNELS),
                'order_status': order_status,
                'create_time': create_time.strftime('%Y-%m-%d %H:%M:%S'),
                'pay_time': pay_time,
                'dt': current_date.strftime('%Y-%m-%d')
            }

            orders.append(order)
            order_id += 1

        current_date += datetime.timedelta(days=1)

    return orders

def save_to_csv(data, filename):
    """将数据保存为CSV文件"""
    if not data:
        return

    with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in data:
            writer.writerow(row)
    print(f"已生成 {filename}，共 {len(data)} 条记录")

if __name__ == "__main__":
    # 生成各类测试数据
    print("开始生成测试数据...")

    user_info = generate_user_info()
    save_to_csv(user_info, 'ods_user_info.csv')

    campaign_info = generate_campaign_info()
    save_to_csv(campaign_info, 'ods_campaign_info.csv')

    user_behavior = generate_user_behavior_log()
    save_to_csv(user_behavior, 'ods_user_behavior_log.csv')

    order_info = generate_order_info()
    save_to_csv(order_info, 'ods_order_info.csv')

    print("测试数据生成完成！")
