from pprint import pprint
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json

# import properties

# spark-sql-kafka-0-10_2.11
spark = SparkSession.builder.appName("SparkStreaming").config("spark.jars.packages",
                                                              "org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4").master(
    "local[*]").getOrCreate()
sc = spark.sparkContext

ssc = StreamingContext(sc, 10)
ssc.checkpoint("checkpoint")
lines = KafkaUtils.createStream(ssc, zkQuorum="localhost:2181", groupId="spark",
                                kafkaParams={"metadata.broker.list": "localhost:9092"},
                                topics={"twitter": 2})

name_count = lines.map(lambda x: json.loads(x[1])).map(lambda x: (x['user']['screen_name'], 1)).reduceByKey(
    lambda x, y: x + y).checkpoint(10000)
# id_name = temp.map(str)
# count_tweet = temp.count()
# print(id_name, ", ", count_tweet)
name_count.pprint()
#
#
# def func1(x):
#     for i in ['riot', 'protest', 'violence', 'angry', 'bloody', 'hell', 'damn', 'morning', 'http']:
#         if i in x:
#             return True
#     return False
#
#
# temp = lines.map(lambda x: json.loads(x[1])).map(
#     lambda x: (x['user']['location'], x['text'])).filter(
#     lambda x: func1(x[1]))
# temp.pprint()
ssc.start()
ssc.awaitTermination()
# lines = spark.readStream.format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("subscribe", "twitter") \
#     .load()
# name_count = lines.head()
# name_count.writeStream.format("console").outputMode("append").trigger(processingTime='6 seconds').start()
# pprint(name_count)
