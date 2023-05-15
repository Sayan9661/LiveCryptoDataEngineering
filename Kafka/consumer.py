# this is code to test the kafka cluster


from kafka import KafkaConsumer
import json


if __name__ == "__main__":
    # Create an instance of the Kafka consumer
    consumer = KafkaConsumer("bitcoin", bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='earliest', group_id='consumer-group-a',
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    print(consumer)
    # Print the topic name
    print(consumer.topics())

    # Poll for new data from the topic
    for message in consumer:
        print(message.value)
