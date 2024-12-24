from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092", 
    client_id="my_admin_client"
)

topic_list = []
topic_list.append(NewTopic(name="my_topic", num_partitions=1, replication_factor=1))

admin_client.create_topics(new_topics=topic_list, validate_only=False)
