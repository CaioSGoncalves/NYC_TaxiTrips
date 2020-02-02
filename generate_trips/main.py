import random
from kafka import KafkaProducer

from flask import Flask
app = Flask(__name__)


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['10.158.0.57:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

def get_trips():
    with open("stream_trips.json") as fp:
        lines = fp.readlines()
        return [random.choice(lines)[:-1] for _ in range(2)]

@app.route("/push-trips")
def push_trips():
    kafka_producer = connect_kafka_producer()

    for message in get_trips():
        publish_message(kafka_producer, 'nyc_trips', "", str(message))

    if kafka_producer is not None:
        kafka_producer.close()    
    return "Ok", 200

@app.route("/ping")
def ping():
    return "pong", 200


if __name__ == "__main__":
    app.run('0.0.0.0', 5000)



