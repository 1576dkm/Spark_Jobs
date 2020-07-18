# from dataclasses import dataclass
from pyspark.sql import SparkSession


# @dataclass(frozen=True)
# class CountryAgg:
#     Country: str
#     Values: str


def returnKeyValue(str1):
    arr = str1.lstrip("\\[").rstrip("\\]").replace("'", "").split(",")
    return arr[0], arr[1]


spark = SparkSession.builder.appName("Test_Parquet").master("local[*]").getOrCreate()

parquetDF = spark.read.csv("flower-classification-with-tpus.csv")

parquetDF.coalesce(1).write.mode("overwrite").parquet("Parquet")
# rdd=spark.read.format("parquet").option("header","True").load("Parquet")
rdd = spark.read.parquet("Parquet").rdd.map(list)
header = rdd.first()
print(header)
rdd1 = rdd.filter(lambda line: header != line).map(lambda x: str(x))
rdd1.foreach(print)
# resultRDD = rdd1.map(lambda r: returnKeyValue(r)).reduceByKey(
#     lambda a, b: a.split(";").zip(b.split(";")).map(lambda x, y: int(x) + int(y)).mkString(";"))
#
# dataSet = resultRDD.toDF()
# dataSet.coalesce(1).write.option("header", "true").mode("overwrite").csv("final")
