
# Create Spark Session
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import sum, avg, max

spark = SparkSession.builder.appName("Union_Example").master("local[*]").getOrCreate()

# Create Sample Dataframe
simpleData = spark.createDataFrame([("James","Sales","NY",90000,34,10000),
    ("Michael","Sales","NY",86000,56,20000),
    ("Robert","Sales","CA",81000,30,23000),
    ("Maria","Finance","CA",90000,24,23000)]).toDF("employee_name","department","state","salary","age","bonus")


simpleData.show(30)

simpleData1 = spark.createDataFrame([("James","Sales","NY",90000,34,10000),
    ("Maria","Finance","CA",90000,24,23000),
    ("Jen","Finance","NY",79000,53,15000),
    ("Jeff","Marketing","CA",80000,25,18000),
    ("Kumar","Marketing","NY",91000,50,21000)]).toDF("name","department","state","salary","age","bonus")

simpleData1.show(30)

df3 = simpleData.union(simpleData1)
df3.show(30)

df5 = simpleData.union(simpleData1).distinct()
df5.show(30)

df5.groupBy("department").count().show(50)

df5.groupBy("department","state").sum("salary","bonus").withColumnRenamed("sum(salary)","SumSalary").show(50)

df5.groupBy("department").agg(
      sum("salary").alias("sum_salary"),avg("salary").alias("avg_salary"),
      sum("bonus").alias("sum_bonus"),
      max("bonus").alias("max_bonus")).show(50)

