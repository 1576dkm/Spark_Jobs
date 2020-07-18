from pyspark import SparkContext
from dataclasses import dataclass

sc = SparkContext("local[*]", "example", 20)
rdd = sc.textFile("flower-classification-with-tpus.csv")
header = sc.parallelize(rdd.take(1))
# print(type(header))
print(header)
rdd2 = (header + (
    rdd.map(lambda x: x.split(',')).map(lambda x: (x[0], x[1].split(';'))).filter(
        lambda line: "Country" not in line).map(
        lambda z: (z[0], list(map(lambda y: int(y), z[1])))).reduceByKey(
        lambda a, b: list(map(lambda a, b: a + b, a, b)))).mapValues(lambda x: ";".join(map(lambda y: str(y), x))).map(
    lambda x: x[0] + ',' + x[1]))

rdd2.saveAsTextFile("Results")


# print(rdd1.take(10))


# @dataclass(frozen=True)
# class CountryAgg:
#     x: str
#     y: str























































# from csv import DictReader
# from io import StringIO
#
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("FlightDelayRDD").master("local[*]").getOrCreate()
# sc = spark.sparkContext
#
#
# def parseLine(line, fieldnames):
#     si = StringIO(line)
#     return next(DictReader(si, fieldnames=fieldnames))
#
#
# rdd = sc.textFile("airline-delays.csv")
# header = rdd.first()
# fieldnames = filter(lambda field: len(field) > 0,
#                     map(lambda field: field.strip('"'), header.split(',')))
# flights = rdd.filter(lambda line: line != header).map(lambda line: parseLine(line, fieldnames))
# flightsByCarrier = flights.filter(lambda flight: flight['OriginCityName'] == "Boston, MA").map(
#     lambda flight: flight['Carrier']).countByValue()
# print(flightsByCarrier)
# t = sorted(flightsByCarrier.items(), key=lambda temp: -temp[1])[0]
# print(t)
