from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Test_Parquet").master("local[*]").getOrCreate()

sc = spark.sparkContext

spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.shuffle.service.enabled", "true")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "6")
spark.conf.set("spark.dynamicAllocation.minExecutors", "6")
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s")
sc.setSystemProperty("spark.speculation", "true")

csvDF = spark.read.csv("data.csv", header="true", inferSchema="true")
# csvDF = spark.read.csv("data.csv") // if we want to take header also as an rdd then we need to filter the header
# from the rdd as given in the comment section below. and will have to create dataFrame using createDtaFrame.
csvDF.write.mode("overwrite").parquet("Parquet")
rdd = spark.read.parquet("Parquet").rdd.map(list)  # here map(list) is not needed but to make it readable i have
# putted it
# header = rdd.first()
# rdd.filter(lambda line: header != line).map(
#     lambda x: (x[0], map(lambda t: int(t), x[1].strip().split(";")))).reduceByKey(
#     lambda a, b: list(map(lambda a, b: a + b, a, b))).mapValues(lambda x: ";".join(map(lambda y: str(y), x)))
rdd1 = rdd.map(
    lambda x: (x[0], map(lambda t: int(t), x[1].strip().split(";")))).reduceByKey(
    lambda a, b: list(map(lambda a, b: a + b, a, b))).mapValues(lambda x: ";".join(map(lambda y: str(y), x)))
# spark.createDataFrame(rdd1, schema=header).write.csv("final", mode="overwrite", header="true")
# rdd1.foreach(print)
df = rdd1.toDF()
# df.withColumnRenamed("_1", "Country").withColumnRenamed("_2", "Values").write.csv("final", mode="overwrite",
#                                                                                   header="true")
df.select(col("_1").alias("Country"), col("_2").alias("Values")).write.csv("final", mode="overwrite",
                                                                           header="true")

