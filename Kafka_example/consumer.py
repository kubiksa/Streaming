from kafka import KafkaConsumer
import json

# Define Kafka consumer
consumer = KafkaConsumer(
    'test-topic-bash',  # The topic to consume messages from
    bootstrap_servers=['localhost:9092'],  # List of Kafka brokers to connect to
    auto_offset_reset='earliest',  # Where to start reading messages when no offset is stored ('earliest' to read from the beginning)
    enable_auto_commit=True,  # Automatically commit offsets after consuming messages
    value_deserializer=lambda x: x.decode('utf-8') if x else None  # Deserialize message values from bytes to UTF-8 strings
)

# Consume messages with error handling for non-JSON messages
for message in consumer:
    print(message.value)

