# KafkaDemo

Produce message and publish to Kafka topic,Consume (read) messages from topic, aggregate the messages by userId and produce a summary item to another kafka topic

# File description
meta_data ----------  folder that contains json files

clear_topics.py -------- clear input_topic and output_topic if those topics are already existed

producer.py ------------ kafka producer, read json files from metadata, produce messages and publish to kafka topic (input_topic)

consumer_normal.py ----- Consume (read) messages from topic (input_topic), aggregate the messages by userId and produce a summary item to another kafka topic (output_topic) without spark

consumer_spark.py ------ Consume (read) messages from topic (input_topic), aggregate the messages by userId and produce a summary item to another kafka topic (output_topic) with spark

get_output.py -------  Consume (read) messages from output_topic, check the result

kafka-spark-test.ipynb ------ jupyter notebook of consumer_spark.py, show step by step

kafka-test.ipynb ------ jupyter notebook of consumer_normal.py, show step by step

# Depolyment 

## prerequisites
Please install kafka and launch kafka service, check http://localhost:9021 is up,  and if you want run consumer_spark.py, please ensure you have installed spark on your enviroment.
Before run the files, please ensure you have installed python-kafka

$pip install python-kafka

## How to run

if you have already created input_topic and output_topic, you can delete the topics by yourself or you can run clear_topics.py

python clear_topics.py

run producer.py to publish kafka topics(input_topic)

python producer.py 

and then, run consumer_normal.py to read messages from input_topic, aggregate the messages by userId and produce a summary item to output_topic

python consumer_normal.py

if you want to run consumer with spark 
python consumer_spark.py

After that, you can get the ouput topic in kafka.

you can check the output topic by either 

cd<#kafka directory#>
  
$ bin/kafka-console-consumer --topic output_topic  --from-beginning --bootstrap-server localhost:9092

or you can run 

python get_output.py to check the output topic


