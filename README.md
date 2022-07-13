# KafkaDemo

Produce message and publish to Kafka topic,Consume (read) messages from topic, aggregate the messages by userId and produce a summary item to another kafka topic

# File description
meta_data ----------  folder that contains json files

clear_topics.py -------- clear input_topic and output_topic if those topics are already existed

producer.py ------------ kafka producer, read json files from metadata, produce messages and publush to kafka topic (input_topic)

consumer_normal.py ----- Consume (read) messages from topic (input_topic), aggregate the messages by userId and produce a summary item to another kafka topic (output_topic) without spark

consumer_spark.py ------ Consume (read) messages from topic (input_topic), aggregate the messages by userId and produce a summary item to another kafka topic (output_topic) with spark

get_output.py -------  Consume (read) messages from output_topic, check the result

kafka-spark-test.ipynb ------ jupyter notebook of consumer_spark.py, show step by step

kafka-test.ipynb ------ jupyter notebook of consumer_normal.py, show step by step

