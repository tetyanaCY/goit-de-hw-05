import json
import time
import random
from kafka import KafkaProducer
from configs import kafka_config, topics_configurations
import uuid


def produce_alerts(producer, sensor_id, topic_name):
    print(f"Producer started for Sensor ID: {sensor_id}, Topic: {topic_name}")
    try:
        while True:
            data = {
                "sensor_id": sensor_id,
                "timestamp": time.time(),
                "temperature": round(random.uniform(25, 45), 2),
                "humidity": round(random.uniform(15, 85), 2),
            }
            producer.send(topic_name, key=str(sensor_id).encode("utf-8"), value=data)
            print(f"Data sent: {data}")
            time.sleep(2)
    except KeyboardInterrupt:
        print("Producer stopped.")
    finally:
        producer.close()


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    topic_name = topics_configurations["topic_sensors"]["name"]
    sensor_id = str(uuid.uuid4())
    produce_alerts(producer, sensor_id, topic_name)
