# Import KafkaConsumer from Kafka library
from kafka import KafkaConsumer

# Import json module to serialize data
import json

consumer = KafkaConsumer ('output_topic',bootstrap_servers = ['localhost:9092'],
value_deserializer=lambda m: json.loads(m.decode('utf-8')),consumer_timeout_ms=1000,auto_offset_reset='earliest')

for message in consumer:
    print("message:")
    print(message)

print('down')