from pyspark.sql.functions import when, col, sum
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

import os

spark = SparkSession.builder.appName("FillNullWithAvg").master("local[*]").getOrCreate()

empDf = spark.read.csv("Dataset.csv",header="true",inferSchema="true")
# empDf.select("*").orderBy("Emp_id","month").show(50)
partitionWindow = Window.partitionBy(col("Emp_id")).orderBy(col("Emp_id"),col("month")).rowsBetween(-1,1)
sumOfNeighbour = sum(empDf.Salary).over(partitionWindow)

final = empDf.select("*",sumOfNeighbour.alias("sumOfNeighbour"))
final.show(50)
final.withColumn('Salary', when(final['Salary'].isNotNull(), final['Salary']).otherwise(final['sumOfNeighbour']/2)).show(50)

temp = final.orderBy("Emp_id","month").select("Salary").rdd.flatMap(lambda x: x).collect()
print(temp)
