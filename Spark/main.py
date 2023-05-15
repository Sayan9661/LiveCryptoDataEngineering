from typing import Dict, Any
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import os

def return_cassandra_session() -> Cluster:
    userdb = os.environ.get('userdb')
    dbpass = os.environ.get('dbpass')
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-crypto-db.zip'
    }
    auth_provider = PlainTextAuthProvider(
        userdb, dbpass)
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    keyspace = 'bigdata'
    session = cluster.connect(keyspace)
    print(session)
    return session


def run_spark_example(consumer, spark, cass_session) -> None:

    # Poll for new data from the topic
    for message in consumer:
        data_js = (message.value)
        print(data_js["market_data"]["current_price"]["usd"])
    consumer.close()
    print("done")


def return_kafka_consumer() -> KafkaConsumer:
    """
    Helper to create a Kafka consumer with the right configuration
    params. See below comments for details:
        |_
    """
    # Create an instance of the Kafka consumer
    consumer = KafkaConsumer("bitcoin", bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id='consumer-group-a',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    print(consumer)
    # Print the topic name
    print(consumer.topics())

    return consumer


def get_spark_context(app_name: str) -> SparkSession:
    """
    Helper to manage the `SparkContext` and keep all of our
    configuration params in one place. See below comments for details:
        |_ https://github.com/bitnami/bitnami-docker-spark/issues/18#issuecomment-700628676
        |_ https://github.com/leriel/pyspark-easy-start/blob/master/read_file.py
    """

    conf = SparkConf()
    conf.setAppName(app_name)

    return SparkSession.builder.config(conf=conf).getOrCreate()


if __name__ == "__main__":

    consumer = return_kafka_consumer()
    # Regular Spark job executed on a Docker container
    spark = get_spark_context("transform-data")
    cass_session = return_cassandra_session()
    print(spark, cass_session)
    while True:
        run_spark_example(consumer, spark, cass_session)
    spark.stop()
