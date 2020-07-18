from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast, when, lit

spark = SparkSession.builder.appName("Test_Parquet").master("local[*]").getOrCreate()

sc = spark.sparkContext

sc.setSystemProperty("spark.dynamicAllocation.enabled", "true")
sc.setSystemProperty("spark.dynamicAllocation.initialExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.minExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s")
sc.setSystemProperty("spark.speculation", "true")

csvDF = spark.read.csv("flower-classification-with-tpus.csv", header="true", inferSchema="true")
csvDF = csvDF.withColumn("Country", when(csvDF.Country.isNull(), lit("USA")).otherwise(csvDF.Country))

csvDF.write.parquet("Parquet", mode="overwrite")
spark.read.parquet("Parquet").createTempView("table")

temp = spark.sql("select Country, concat( cast (sum (split(Values,';') [0]) as int),';', cast (sum (split("
                 "Values,';')[1]) as int),';',cast(sum(split(Values,';')[2]) as int),';', cast (sum (split("
                 "Values,';')[3]) as int),';', cast(sum (split(Values,';')[4]) as int)) as Values from "
                 "table group by Country order by Country")
# totalValues.show(20)
totalValues = spark.sql("""with t1 as (select Country, cast(split(Values, ";") as array<int>) as V from table ) 
select Country, concat_ws(";", sum(V[0]), sum(V[1]), sum(V[2]), sum(V[3]), sum(V[4])) as Values from t1 group by 
Country order by Country""")
temp.show(20)
totalValues.write.csv("final", header="true", mode="overwrite")
