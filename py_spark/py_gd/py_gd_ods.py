from kafka import KafkaProducer
import json
import time
import random
from faker import Faker
from datetime import datetime, timedelta

# 初始化 Kafka 生产者（替换为你的 Kafka 地址）
producer = KafkaProducer(
    bootstrap_servers=['cdh01:9092'],
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
    batch_size=16384,
    linger_ms=500
)

# 初始化伪造数据工具
fake = Faker(locale='zh_CN')

# 公共数据池
USER_ID_POOL = [fake.random_int(min=100000, max=999999) for _ in range(500)]  # 用户ID池
GOODS_ID_POOL = [f"goods_{i:08d}" for i in range(1, 300)]  # 商品ID池
SHOP_ID_POOL = [f"shop_{i:06d}" for i in range(1, 50)]  # 店铺ID池
DATA_SOURCE = ["日志采集", "接口同步"]  # 数据来源

# 各表特有配置
# 店铺访问行为表配置
VISIT_BEHAVIORS = ["直播间观看", "短视频观看", "图文浏览", "微详情浏览", "详情页访问", "其他页面访问"]
VISIT_PLATFORMS = ["APP", "H5", "小程序"]

# 商品加购行为表配置
CART_STATUSES = ["normal", "cancel"]

# 订单支付行为表配置
ORDER_TYPES = ["normal", "presale", "try_first"]
PAYMENT_STATUSES = ["success", "fail"]


def generate_shop_visit_behavior():
    """生成店铺访问行为数据"""
    # 修改时间范围为当前时间前30分钟到现在
    visit_time = fake.date_time_between(start_date="-30m", end_date="now")
    behavior = random.choice(VISIT_BEHAVIORS)

    # 非商品页面访问时goods_id为None
    goods_id = random.choice(GOODS_ID_POOL) if behavior not in ["其他页面访问", "直播间观看"] else None

    return {
        "user_id": str(random.choice(USER_ID_POOL)),
        "visit_time": visit_time.strftime("%Y-%m-%d %H:%M:%S"),
        "visit_behavior": behavior,
        "goods_id": goods_id,
        "shop_id": random.choice(SHOP_ID_POOL),
        "visit_platform": random.choice(VISIT_PLATFORMS),
        "data_source": random.choice(DATA_SOURCE),
        "dt": visit_time.strftime("%Y%m%d")
    }


def generate_goods_cart_behavior():
    """生成商品加购行为数据"""
    # 修改时间范围为当前时间前30分钟到现在
    cart_time = fake.date_time_between(start_date="-30m", end_date="now")
    return {
        "user_id": str(random.choice(USER_ID_POOL)),
        "cart_time": cart_time.strftime("%Y-%m-%d %H:%M:%S"),
        "goods_id": random.choice(GOODS_ID_POOL),
        "cart_quantity": random.randint(1, 10),
        "shop_id": random.choice(SHOP_ID_POOL),
        "cart_status": random.choice(CART_STATUSES),
        "data_source": random.choice(DATA_SOURCE),
        "dt": cart_time.strftime("%Y%m%d")
    }


def generate_order_payment_behavior():
    """生成订单支付行为数据"""
    # 修改时间范围为当前时间前30分钟到现在
    payment_time = fake.date_time_between(start_date="-30m", end_date="now")
    payment_amount = round(random.uniform(19.9, 4999.9), 2)

    # 退款金额不超过支付金额
    refund_amount = round(random.uniform(0, payment_amount * 0.3), 2) if random.random() < 0.2 else 0
    freight_amount = round(random.uniform(0, 20), 2)
    red_packet_amount = round(random.uniform(0, 50), 2)

    return {
        "order_id": f"order_{fake.random_int(min=10000000, max=99999999)}",
        "user_id": str(random.choice(USER_ID_POOL)),
        "payment_time": payment_time.strftime("%Y-%m-%d %H:%M:%S"),
        "goods_id": random.choice(GOODS_ID_POOL),
        "shop_id": random.choice(SHOP_ID_POOL),
        "payment_amount": payment_amount,
        "refund_amount": refund_amount,
        "freight_amount": freight_amount,
        "red_packet_amount": red_packet_amount,
        "order_type": random.choice(ORDER_TYPES),
        "payment_status": random.choice(PAYMENT_STATUSES),
        "data_source": random.choice(DATA_SOURCE),
        "dt": payment_time.strftime("%Y%m%d")
    }


if __name__ == "__main__":
    print("三张表流数据模拟脚本启动，持续发送数据到Kafka...")
    try:
        while True:
            # 随机生成各表数据（模拟不同行为的发生频率）
            # 店铺访问行为数据（发生频率最高）
            for _ in range(random.randint(3, 8)):
                visit_data = generate_shop_visit_behavior()
                producer.send(
                    topic="ods_shop_visit_behavior",
                    key=visit_data["shop_id"].encode('utf-8'),
                    value=visit_data
                )
                print(f"发送店铺访问数据：{json.dumps(visit_data, ensure_ascii=False)}")

            # 商品加购行为数据（发生频率中等）
            for _ in range(random.randint(1, 3)):
                cart_data = generate_goods_cart_behavior()
                producer.send(
                    topic="ods_goods_cart_behavior",
                    key=cart_data["goods_id"].encode('utf-8'),
                    value=cart_data
                )
                print(f"发送加购行为数据：{json.dumps(cart_data, ensure_ascii=False)}")

            # 订单支付行为数据（发生频率最低）
            for _ in range(random.randint(0, 2)):
                payment_data = generate_order_payment_behavior()
                producer.send(
                    topic="ods_order_payment_behavior",
                    key=payment_data["order_id"].encode('utf-8'),
                    value=payment_data
                )
                print(f"发送支付行为数据：{json.dumps(payment_data, ensure_ascii=False)}")

            # 控制发送频率（每1.0-2.0秒发送一批）
            time.sleep(random.uniform(1.0, 2.0))
    except KeyboardInterrupt:
        print("脚本手动停止")
    finally:
        producer.close()
        print("Kafka生产者关闭")
