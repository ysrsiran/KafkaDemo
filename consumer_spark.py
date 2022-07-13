import os
from kafka import KafkaConsumer
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import SparkSession

def main():

    # set up enviroment
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
    #start spark 
    sc = SparkSession.builder.appName('Pyspark_kafka_read_write').getOrCreate()

    df = sc \
        .read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "input_topic") \
        .option("startingOffsets", "earliest") \
        .option("endingOffsets", "latest") \
        .load() \
        .select("value") \
        .selectExpr("CAST(value AS STRING) as json")

    schema = StructType([ \
        StructField("userId",StringType(),True), \
        StructField("type",StringType(),True), \
        StructField("metadata",StructType([\
                        StructField('messageId', StringType(), True), \
                        StructField('sentAt', LongType(), True), \
                        StructField('timestamp', LongType(), True), \
                        StructField('receivedAt', LongType(), True), \
                        StructField('apiKey', StringType(), True), \
                        StructField('spaceId', StringType(), True), \
                        StructField('version', StringType(), True), \
                        
                        ])),\
        StructField("event", StringType(), True), \
        StructField("eventData", StructType([
                StructField('MovieID', StringType(), True)\
    ])) \
    ])

    # Parsing and selecting the right column data
    df = df.withColumn("jsonData", from_json(col("json"), schema)) \
                    .select("jsonData.*")
    #filter data 
    user_info = df.select('userId','metadata.sentAt','metadata.receivedAt')
    #calculate the timestamp, assuing sentAt + recivedAt = the time that the system recive the message
    user_info = user_info.withColumn("seenTime",col('sentAt')+col('receivedAt'))
    #aggreate te data, get the last seen and first seen time
    result = user_info.groupby("userId").agg(min('seenTime').alias('firstSeen'),max('seenTime').alias('lastSeen'))

    #putting data back to kafka 
    result.selectExpr("CAST(userId AS STRING) AS key", "to_json(struct(*)) AS value")\
                    .write \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", "localhost:9092") \
                    .option("topic", "output_topic") \
                    .option("checkpointLocation", "./check") \
                    .save()
if __name__ == "__main__":
    main()