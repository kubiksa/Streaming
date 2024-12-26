#Go to directory that holds docker-compose file
docker-compose up
#This starts the containers with the yml cofigs

#find the container id
docker ps

#Access Kafka container:
docker exec -it <kafka_container_id> /bin/bash

# Go to kafka binaries (this is at a kafka prompt
cd /opt/kafka/bin

# create topic 
./kafka-topics.sh --create --topic test-topic-bash --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3

# Run Kafka Producer -- after running, can start inserting data, it should go to each partition in round robin style
./kafka-console-producer.sh --topic test-topic-bash --bootstrap-server localhost:9092

# Run Kafka Consumers in separaate windows
docker exec -it <kafka_container_id> /bin/bash
cd /opt/kafka/bin
./kafka-console-consumer.sh --topic test-topic-bash --bootstrap-server localhost:9092 --partition 1 --from-beginning
./kafka-console-consumer.sh --topic test-topic-bash --bootstrap-server localhost:9092 --partition 2 --from-beginning
./kafka-console-consumer.sh --topic test-topic-bash --bootstrap-server localhost:9092 --partition 3 --from-beginning

#to see what processes are running open a new window and command
top
