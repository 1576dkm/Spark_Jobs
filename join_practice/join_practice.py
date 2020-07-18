from pyspark.sql.functions import when
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Flights_SparkSql").master("local[*]").getOrCreate()

sc = spark.sparkContext

left = spark.createDataFrame([(3, "A3"), (4, "A4"), (4, "A4_1"), (5, "A5"), (6, "A6")]).toDF("id", "value")
right = spark.createDataFrame([(1, "A1"), (2, "A2"), (3, "A3"), (4, "A4")]).toDF("id1", "value")


print("LEFT")
left.orderBy("id").show()

print("RIGHT")
right.orderBy("id1").show()

joinTypes = ["inner", "outer", "full", "full_outer", "left", "left_outer", "right", "right_outer", "left_semi",
             "left_anti"]

for i in joinTypes:
    print("{} join!!!!!".format(i))
    left.join(right, left["id"] == right["id1"], how=i).show(50)

# print("cross join!!!!!")
# left.join(right, how="cross")
print("left_anti join!!!!!")
left.join(right, left["id"] == right["id1"], how="left").where("id1 is null").show(50)

left_anti = left.join(right, left["id"] == right["id1"], how="left_anti")
left_anti.union(right).show(50)
