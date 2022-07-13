import os
import json
import itertools
from kafka import KafkaConsumer
from operator import itemgetter
from kafka import KafkaProducer




def main():
    #define input and oupt topics 
    input_topic = 'input_topic'
    output_topic = 'output_topic'
    kafka_data =[]
    #initialize 
    producer = KafkaProducer(bootstrap_servers =
    ['localhost:9092'],value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    consumer = KafkaConsumer (input_topic,bootstrap_servers = ['localhost:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),consumer_timeout_ms=1000,auto_offset_reset='earliest')

    for message in consumer:
        kafka_data.append(message.value)

    sorted_data = sorted(kafka_data,key=itemgetter('userId'))

    #aggragate the data, and send the result to output
    for key, group in itertools.groupby(sorted_data, key=lambda x:x['userId']):
    
        arr = list(group)
        big = -1
        small = float('inf')
        for obj in arr :
            sent_at = obj['metadata']['sentAt']
            receive_at = obj['metadata']['receivedAt']
            time = sent_at+receive_at
            #find the max and min value of the timestamp that the system received the message
            if time < small:
                small = time
            if time > big :
                big = time
        dict = {'userId':key,'firstSeen': small,'lastSeen':big}
        producer.send(output_topic, value = dict, key =json.dumps(key).encode('utf-8'))





if __name__ == "__main__":
    main()