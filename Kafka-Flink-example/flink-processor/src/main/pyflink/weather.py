from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes
from pyflink.table.expressions import col
from pyflink.table.window import Tumble


#create the execution and table environments

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

#define the Kafka source

t_env.execute_sql("""
    CREATE TABLE weather_source (
        city STRING,
        temperature DOUBLE,
        event_time TIMESTAMP(3),
        WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
    ) WITH (
        'connector' = 'kafka',
        'topic' = 'weather',
        'properties.bootstrap.servers' = 'kafka:9092',
        'properties.group.id' = 'groupId-919292',
        'scan.startup.mode' = 'earliest-offset',
        'format' = 'json'
    )
""")

#create a table from the Kafka source and perform an aggregation

result_table = t_env.from_path('weather_source') \
    .window(Tumble.over("1.minutes").on(col("event_time")).alias("w")) \
    .group_by(col("city"), col("w")) \
    .select(col("city"), col("temperature").avg.alias("avg_temperature"), col("w").start.alias("window_start"))

#define the sink table in postgres

t_env.execute_sql("""
    CREATE TABLE weather_sink (
        city STRING,
        average_temperature DOUBLE,
        window_start TIMESTAMP(3)
    ) WITH (
        'connector' = 'jdbc',
        'url' = 'jdbc:postgresql://docker.for.mac.host.internal:5438/postgres',
        'table-name' = 'weather',
        'driver' = 'org.postgresql.Driver',
        'username' = 'postgres',
        'password' = 'postgres'
    )
""")

#insert the results into the sink table

result_table.execute_insert('weather_sink').wait()

#execute the python job
env.execute("Kafka-flink-postgres")

