import json
from kafka import KafkaConsumer, KafkaProducer
from configs import kafka_config, topics_configurations


def process_message(producer, message, temperature_topic, humidity_topic):
    sensor_id = message.get("sensor_id")
    temperature = message.get("temperature")
    humidity = message.get("humidity")
    timestamp = message.get("timestamp")

    if temperature > 40:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "temperature": temperature,
            "message": "Temperature exceeds threshold (40°C)",
        }
        producer.send(temperature_topic, value=alert)
        print(f"Temperature alert sent: {alert}")

    if humidity > 80 or humidity < 20:
        alert = {
            "sensor_id": sensor_id,
            "timestamp": timestamp,
            "humidity": humidity,
            "message": "Humidity out of range (20-80%)",
        }
        producer.send(humidity_topic, value=alert)
        print(f"Humidity alert sent: {alert}")


def filter_alerts(producer, consumer, temperature_topic, humidity_topic):
    print("Alert filtering started...")
    try:
        for message in consumer:
            process_message(producer, message.value, temperature_topic, humidity_topic)
    except KeyboardInterrupt:
        print("Filtering stopped.")
    finally:
        producer.close()
        consumer.close()


if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    consumer = KafkaConsumer(
        topics_configurations["topic_sensors"]["name"],
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id="alert_processor",
        auto_offset_reset="earliest",
    )

    filter_alerts(
        producer,
        consumer,
        topics_configurations["topic_temperature"]["name"],
        topics_configurations["topic_humidity"]["name"],
    )
