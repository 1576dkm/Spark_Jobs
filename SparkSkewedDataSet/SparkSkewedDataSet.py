from math import exp
from random import randint
from datetime import datetime
from pyspark.sql import functions as f

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Flights_SparkSql").master("local[*]").getOrCreate()
sc = spark.sparkContext
# spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)


# Creating the 2 dataframes:
def count_elements(splitIndex, iterator):
    n = sum(1 for _ in iterator)
    yield (splitIndex, n)


def get_part_index(splitIndex, iterator):
    for it in iterator:
        yield (splitIndex, it)


num_parts = 18
# create the large skewed rdd
skewed_large_rdd = sc.parallelize(range(0, num_parts), num_parts).flatMap(lambda x: range(0, int(exp(x))))
skewed_large_rdd = skewed_large_rdd.mapPartitionsWithIndex(lambda ind, x: get_part_index(ind, x))

skewed_large_df = spark.createDataFrame(skewed_large_rdd, ['x', 'y'])
small_rdd = sc.parallelize(range(0, num_parts), num_parts).map(lambda x: (x, x))

small_df = spark.createDataFrame(small_rdd, ['a', 'b'])

t0 = datetime.now()
result1 = small_df.join(skewed_large_df, skewed_large_df['x'] == small_df['a'])
# result1.count()
print("The direct join takes %s" % (str(datetime.now() - t0)))

# Dividing the flower-classification-with-tpus into 100 bins for large df and replicating the small df 100 times
salt_bins = 100

skewed_transformed_df = skewed_large_df.withColumn('salt', (f.rand() * salt_bins).cast('int')).cache()
skewed_transformed_df.show(10)
small_transformed_df = small_df.withColumn('replicate', f.array([f.lit(i) for i in range(salt_bins)]))

small_transformed_df = small_transformed_df.select('*', f.explode('replicate').alias('salt')).drop('replicate').cache()
small_transformed_df.show(10)
# Finally the join avoiding the skew

t0 = datetime.now()
result2 = skewed_transformed_df.join(small_transformed_df, (skewed_transformed_df['x'] == small_transformed_df['a']) & (
        skewed_transformed_df['salt'] == small_transformed_df['salt']))
result2.explain()
# result2.count()
print("The Without Skewness of flower-classification-with-tpus join takes %s" % (str(datetime.now() - t0)))

dat = spark.read.csv("person_data.csv", header="true", inferSchema="true")

dat.repartition(8).write.partitionBy("person_country").csv("final", mode="overwrite", header="true")
