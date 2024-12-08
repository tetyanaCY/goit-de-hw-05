import json
from kafka import KafkaConsumer
from configs import kafka_config, topics_configurations


def listen_alerts(consumer):
    print("Listening for alerts...")
    try:
        for message in consumer:
            print(f"Received: Topic={message.topic}, Data={message.value}")
    except KeyboardInterrupt:
        print("Alert listener stopped.")
    finally:
        consumer.close()


if __name__ == "__main__":
    consumer = KafkaConsumer(
        topics_configurations["topic_temperature"]["name"],
        topics_configurations["topic_humidity"]["name"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="alert_listener",
        auto_offset_reset="earliest",
    )

    listen_alerts(consumer)
