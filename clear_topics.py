from kafka.admin import KafkaAdminClient


topic_names = ['input_topic','output_topic']

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])

try:
    admin_client.delete_topics(topics=topic_names)
    print("Topic Deleted Successfully")
except Exception as e:
    print(e)