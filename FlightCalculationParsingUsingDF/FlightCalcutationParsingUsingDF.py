from pyspark.sql import SparkSession
import os
spark = SparkSession.builder.appName("FlightDelayRDD").master("local[*]").getOrCreate()
sc = spark.sparkContext
# sc.setSystemProperty("spark.executor.memory","256m")
# sc.setSystemProperty("spark.executor.instances","4")

sc.setSystemProperty("spark.dynamicAllocation.enabled", "true")
sc.setSystemProperty("spark.dynamicAllocation.initialExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.minExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s")
sc.setSystemProperty("spark.speculation", "true")
flights = spark.read.csv("airline-delays.csv", header="true", inferSchema="true")


# temp = list(flights.columns)
# print(temp)
# temp.remove("_c109")
# print(temp)
# flights = flights.select(temp).rdd
flights = flights.drop("_c109").rdd

# Which airline has the most flights departing from Boston, MA
flightsByCarrier = flights.filter(lambda flight: flight['OriginCityName'] == "Boston, MA").map(
    lambda flight: flight['Carrier']).countByValue()
# print(flightsByCarrier)
# print(flightsByCarrier.items())

Carrier = sorted(flightsByCarrier.items(), key=lambda position: -position[1])[0]
print(Carrier)

# the farthest 10 destinations from Chicago, IL that we could fly to
flightsFarthest = flights.filter(lambda f: f['OriginCityName'] == "Chicago, IL") \
    .map(lambda flight: (flight['DestCityName'], float(flight['Distance']))) \
    .distinct().sortBy(lambda distance: distance[1], ascending=False).take(10)
print(flightsFarthest)

# contemplating direct flights from New York, NY to San Francisco, CA In terms of arrival delay, which airline has
# the best record on that route


bestRouteNY = flights.filter(
    lambda flight: flight['OriginCityName'] == "New York, NY" and flight['DestCityName'] == "San Francisco, CA" and
                   flight['ArrDelay'] != '').map(
    lambda flight: (flight['Carrier'], flight['ArrDelay'])).reduceByKey(lambda a, b: float(a or 0) + float(b or 0)).min(
    lambda delay: delay[1])

print(bestRouteNY)
# San Jose, CA, and there don't seem to be many direct flights taking you to Boston, MA. Of all the 1-stop flights,
# which would be the best option in terms of average arrival delay
SanJoseCAToOthers = flights.filter(lambda flight: flight['OriginCityName'] == "San Jose, CA").map(
    lambda flight: (
        flight['DestCityName'], flight['ArrDelay'] if flight['ArrDelay'] != '' else 0))
OthersToBostonMA = flights.filter(lambda flight: flight['DestCityName'] == 'Boston, MA').map(
    lambda flight: (
        flight['OriginCityName'], flight['ArrDelay'] if flight['ArrDelay'] != '' else 0))
finalDF = SanJoseCAToOthers.join(OthersToBostonMA)
# print(finalDF.take(10))
KeyValue = finalDF  # .map(lambda x: (x[0], (x[1][0] + x[1][1])))
AverageKeyValue = KeyValue.mapValues(lambda x: (float(x[0] or 0) + float(x[1] or 0), 1)).reduceByKey(
    lambda x, y: (x[0] + y[0], x[1] + y[1])).mapValues(
    lambda x: x[0] / x[1]).min(lambda x: x[1])

print(AverageKeyValue)
