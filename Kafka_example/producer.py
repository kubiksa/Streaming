from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import json

# Define Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Produce a message
future = producer.send('test-topic', {'key': 'akash'})
try:
    record_metadata = future.get(timeout=10)
except KafkaError as e:
    print(f"Error sending message: {e}")
finally:
    producer.close()


df = pd.read_csv("data/indexProcessed.csv")
df.head()
while True:
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    producer.send('demo_test', value=dict_stock)
    sleep(1)
producer.flush() #clear data from kafka server
