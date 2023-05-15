from kafka import KafkaProducer
import json
import time
import random
from urllib import request
import json

# from data_ingestion import return_data


def return_data():
    url = 'https://api.coingecko.com/api/v3/coins/solana?localization=false&tickers=true&market_data=true&community_data=false&developer_data=false&sparkline=false'
    response = request.urlopen(url)
    data = response.read()
    data_js = json.loads(data)
    # print((data_js))
    return data_js


# Create an instance of the Kafka producer
producer = KafkaProducer(bootstrap_servers=[
                         'localhost:9092'], value_serializer=lambda x: json.dumps(x).ende('utf-8'))

if __name__ == '__main__':
    # Call the producer.send method with a producer-record
    while True:
        data = return_data()
        producer.send("solana", value=data)
        print(f"Sent: {data}")
        time.sleep(20)
