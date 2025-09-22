from confluent_kafka import Producer
import json, random, time
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

#Load environment variables
load_dotenv()

# -------- Delivery Callback -----------
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message successfully delivered to topic:{msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")

# --------- Kafka Configuration -----------
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISM'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

# ----------- Order generator ----------------
def generate_order(order_id: str) -> dict:
    return {
        "order_id": order_id,
        "order_date": (datetime.now() - timedelta(minutes=random.randint(0,30))).isoformat(),
        "created_at": datetime.now().isoformat(),
        "customer_id": f"customer_{random.randint(1,100)}",
        "amount": random.randint(100,1000)
    }

# ----------- Producer --------------
producer = Producer(**kafka_config)
topic = os.getenv('KAFKA_ORDER_TOPIC')

try:
    order_id_counter = 1
    for _ in range(10):
        order_id = f"order_{order_id_counter}"
        order = generate_order(order_id)
        payload = json.dumps(order)

        # Send primary event
        producer.produce(topic,key=order_id, value=payload,callback=delivery_report)
        producer.poll(0)
        print("Sent order ->", payload)

        # Random duplicate
        if random.choice([True, False]):
            producer.produce(topic,key=order_id, value=payload,callback=delivery_report)
            producer.poll(0)
            print("Sent duplicate order ->", payload)

        order_id_counter+=1
        time.sleep(6)
finally:
    # Ensure all messages delivery before exit
    producer.flush()