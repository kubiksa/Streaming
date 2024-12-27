FROM apache/flink:latest

# Install additional dependencies if needed
RUN apt-get update && apt-get install -y python3-pip

# Install PyFlink and any other Python dependencies
RUN pip3 install apache-flink==1.18.0

# Copy resources
WORKDIR /opt/flink/usrlib
COPY wait-for-it.sh /wait-for-it.sh
COPY weather.py .

# Download PostgreSQL JDBC driver
RUN wget https://jdbc.postgresql.org/download/postgresql-42.2.18.jar -O /opt/flink/lib/postgresql-42.2.18.jar

# Wait for Zookeeper and Kafka to be available and run PyFlink application
CMD /wait-for-it.sh -s -t 30 $ZOOKEEPER_SERVER -- /wait-for-it.sh -s -t 30 $KAFKA_SERVER -- flink run -py /opt/flink/usrlib/your_pyflink_script.py
