
import os

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").getOrCreate()

    spark.conf.set(
        "spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true"
    )
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    df = spark.range(1, 16000, 1, 16).select(
        f.col("id").alias("key"), f.rand(12).alias("value")
    )

    df.write.saveAsTable("unbucketed", format="parquet", mode="overwrite")

    df.write.bucketBy(16, "key").sortBy("value").saveAsTable(
        "bucketed", format="parquet", mode="overwrite")

    t1 = spark.table("unbucketed")
    t2 = spark.table("bucketed")
    t3 = spark.table("bucketed")

    # Unbucketed - bucketed join. Both sides need to be repartitioned.
    print("Unbucketed - bucketed join. Both sides need to be repartitioned")
    t1.join(t2, "key").explain()

    # Unbucketed - bucketed join. Unbucketed side is correctly repartitioned, and only one shuffle is needed.
    print("Unbucketed - bucketed join. Unbucketed side is correctly repartitioned, and only one shuffle is needed.")
    t1.repartition(16, "key").join(t2, "key").explain()

    # Unbucketed - bucketed join. Unbucketed side is incorrectly repartitioned, and two shuffles are needed
    print("Unbucketed - bucketed join. Unbucketed side is incorrectly repartitioned, and two shuffles are needed")
    t1.repartition("key").join(t2, "key").explain()

    # Bucketed - bucketed join. Both sides have the same bucketing, and no shuffles are needed.
    print("Bucketed - bucketed join. Both sides have the same bucketing, and no shuffles are needed.")
    t3.join(t2, "key").explain()
