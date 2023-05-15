from typing import Dict, Any
from pyspark.conf import SparkConf
from pyspark.sql import SparkSession
from kafka import KafkaConsumer
import json
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider


def return_cassandra_session() -> Cluster:
    cloud_config = {
        'secure_connect_bundle': 'secure-connect-crypto-db.zip'
    }
    auth_provider = PlainTextAuthProvider(
        'GBZEexiNcuZkgXuZZnjblXLq', '+bFNyYq65Pp+e-G6j7D-XzNDNDqbmEDOW_n,NpDlbGx9j0RMnPyNh+_sG_tnqWZ8zGa5ZZoX0wFMiM7crP7-Ki3mdc+JtF8BHRRzr4NxhDjh6myTuFEAaBZQ1ZyPC5w-')
    cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
    keyspace = 'bigdata'
    session = cluster.connect(keyspace)
    print(session)
    return session


def run_spark_example(consumer, spark, cass_session) -> None:

    # Poll for new data from the topic
    for message in consumer:
        data_js = (message.value)
        insert_q = """INSERT INTO bigdata.crypto_info (id, name,
            symbol,
            time_last_updated,
            description,
            current_price_usd,
            block_time_in_minutes,
            total_volume_usd,
            market_cap_rank,
            high_24h_usd,
            low_24h_usd,
            price_change_percentage_24h,
            price_change_percentage_200d_usd,
            price_change_percentage_7d_usd,
            price_change_percentage_30d_usd,
            price_change_percentage_60d_usd,
            price_change_percentage_1y_usd,
            ticker_base,
            ticker_target,
            market_name,
            ticker_last,
            ticker_volume,
            converted_last_eth,
            converted_volume_eth,
            trust_score)
            VALUES (uuid(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

        cass_session.execute(
            insert_q,
            (
                data_js['name'],
                data_js['symbol'],
                data_js["market_data"]["last_updated"],
                data_js['description']['en'],
                data_js['market_data']['current_price']['usd'],
                data_js['block_time_in_minutes'],
                data_js['market_data']['total_volume']['usd'],
                data_js['market_data']['market_cap_rank'],
                data_js['market_data']['high_24h']['usd'],
                data_js['market_data']['low_24h']['usd'],
                data_js['market_data']['price_change_percentage_24h'],
                data_js['market_data']['price_change_percentage_200d_in_currency']['usd'],
                data_js['market_data']['price_change_percentage_7d_in_currency']['usd'],
                data_js['market_data']['price_change_percentage_30d_in_currency']['usd'],
                data_js['market_data']['price_change_percentage_60d_in_currency']['usd'],
                data_js['market_data']['price_change_percentage_1y_in_currency']['usd'],
                data_js['tickers'][0]['base'],
                data_js['tickers'][0]['target'],
                data_js['tickers'][0]['market']['name'],
                data_js['tickers'][0]['last'],
                data_js['tickers'][0]['volume'],
                data_js['tickers'][0]['converted_last']['eth'],
                data_js['tickers'][0]['converted_volume']['eth'],
                data_js['tickers'][0]['trust_score']
            )
        )
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
    consumer = KafkaConsumer("dogecoin", bootstrap_servers=['localhost:9092'],
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
