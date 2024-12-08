from kafka.admin import KafkaAdminClient, NewTopic
from configs import kafka_config, topics_configurations


def create_topics(admin_client, topic_configs):
    existing_topics = admin_client.list_topics()  # Get the list of existing topics
    for topic_id, config in topic_configs.items():
        topic_name = config["name"]
        num_partitions = config["num_partitions"]
        replication_factor = config["replication_factor"]

        if topic_name in existing_topics:
            print(f"Topic '{topic_name}' already exists. Skipping creation.")
            continue

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        try:
            admin_client.create_topics(new_topics=[new_topic], validate_only=False)
            print(f"Topic '{topic_name}' created successfully.")
        except Exception as e:
            print(f"Error creating topic '{topic_name}': {e}")



if __name__ == "__main__":
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        security_protocol=kafka_config["security_protocol"],
        sasl_mechanism=kafka_config["sasl_mechanism"],
        sasl_plain_username=kafka_config["username"],
        sasl_plain_password=kafka_config["password"],
    )
    create_topics(admin_client, topics_configurations)
    admin_client.close()
