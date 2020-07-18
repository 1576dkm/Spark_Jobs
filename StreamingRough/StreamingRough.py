from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

spark = SparkSession.builder.appName("FlightDelayRDD").master("local[*]").getOrCreate()
sc = spark.sparkContext

ssc = StreamingContext(sc, 1)
lines = ssc.socketTextStream()
