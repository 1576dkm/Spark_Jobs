from pyspark.sql.functions import when
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Flights_SparkSql").master("local[*]").getOrCreate()

sc = spark.sparkContext

sc.setSystemProperty("spark.dynamicAllocation.enabled", "true")
sc.setSystemProperty("spark.dynamicAllocation.initialExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.minExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s")
sc.setSystemProperty("spark.speculation", "true")

df = spark.read.csv("airline-delays.csv", header="true", inferSchema="true")
# df = df.toDF(*[c.upper() for c in df.columns])  # to convert column name to upper case.
# print(df.columns)
df.createTempView("t")
# Which airline has the most flights departing from Boston, MA
bestCarrierFromBostonMA = spark.sql(
    "select Carrier, count(Carrier) as fc from t where OriginCityName='Boston, MA' group by Carrier order by fc desc "
    "limit 1")
bestCarrierFromBostonMA.show()

# the farthest 10 destinations from Chicago, IL that we could fly to

flightsFarthestFromChicagoIL = spark.sql("select DestCityName, max(Distance) as MaxDistance from t where "
                                         "OriginCityName='Chicago, IL' group by DestCityName order by "
                                         "MaxDistance desc limit 10")
flightsFarthestFromChicagoIL.show()

# contemplating direct flights from New York, NY to San Francisco, CA In terms of arrival delay, which airline has
# the best record on that route
bestRouteNewYorkNYToSanFrancisco = spark.sql(
    "select Carrier, sum(ArrDelay) as MinArrDelay from t where OriginCityName='New York, NY' and DestCityName='San "
    "Francisco, CA' and ArrDelay is not null group by Carrier order by MinArrDelay limit 1")
bestRouteNewYorkNYToSanFrancisco.show()

# San Jose, CA, and there don't seem to be many direct flights taking you to Boston, MA. Of all the 1-stop flights,
# which would be the best option in terms of average arrival delay
# df = df.withColumn('ArrDelay', when(df['ArrDelay'].isNull(), 0).otherwise(df['ArrDelay']))
# df.withColumn('ArrDelay', df['ArrDelay'].cast(DoubleType())).createOrReplaceTempView("t")
SanJoseCAToOthers = spark.sql("select DestCityName as key, if(ArrDelay is not null,ArrDelay,0.0) as delay1 from t "
                              "where "
                              "OriginCityName='San Jose, CA'")
OthersToBostonMA = spark.sql(
    "select OriginCityName as key, if(ArrDelay is not null,ArrDelay,0.0) as delay2 from t where DestCityName='Boston, "
    "MA'")
SanJoseCAToOthers.join(broadcast(OthersToBostonMA), on=["key"], how='inner').createOrReplaceTempView("t")
res = spark.sql(
    "select key as cityName,avg(delay1+delay2) as avgDelay from t group by cityName order by avgDelay limit 1")

res.write.csv("final", mode="overwrite", header="true")
# res.write.parquet("final", mode="overwrite")
# t = spark.read.parquet("final").first()
# print(t)
