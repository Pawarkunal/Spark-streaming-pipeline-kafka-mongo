from confluent_kafka import Producer
import json, random, uuid
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

#Load environment variables
load_dotenv()

# ------------- Delivery Callback ----------------
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message successfully delivered to topic: {msg.topic()} [partition {msg.partition()}] @ offset {msg.offset()}")

# --------- Kafka Configuration -----------------
kafka_config = {
    'bootstrap.servers': os.getenv('KAFKA_BOOTSTRAP'),
    'sasl.mechanisms': os.getenv('KAFKA_SASL_MECHANISM'),
    'security.protocol': os.getenv('KAFKA_SECURITY_PROTOCOL'),
    'sasl.username': os.getenv('KAFKA_SASL_USERNAME'),
    'sasl.password': os.getenv('KAFKA_SASL_PASSWORD')
}

# ----------- Payment Gateway -------------------
def generate_payment(order_id, payment_id):
    return {
        "payment_id": payment_id,
        "order_id": order_id,
        "payment_date": (datetime.now() - timedelta(minutes=random.randint(0,30))).isoformat(),
        "created_at": datetime.now().isoformat(),
        "amount": random.randint(50,500)
    }

# --------------- Producer ---------------------
producer = Producer(**kafka_config)
topic = os.getenv('KAFKA_PAYMENT_TOPIC')

# Example order_id and unique payment id 
order_id = f"order_{random.randint(1,10)}"
payment_id = str(uuid.uuid4())

try:
    payment = generate_payment(order_id, payment_id)
    payload = json.dumps(payment)
    producer.produce(topic, key=order_id, value=payload,callback=delivery_report)
    producer.poll(0)
    print("Sent Payment -> ", payload)

finally:
    producer.flush()