import urllib.request as urllib2
from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, rank, lag, lead, row_number, dense_rank, percent_rank, ntile, cume_dist, \
    first, last, regexp_extract, regexp_replace, explode, explode_outer, posexplode, posexplode_outer, flatten
from pyspark.sql.session import SparkSession
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import broadcast

# Create Spark Session
spark = SparkSession.builder.appName("Flights_SparkSql").master("local[*]").getOrCreate()

# Create Sample Dataframe
empDF = spark.createDataFrame([
    (7369, "SMITH", "CLERK", 7902, "17-Dec-80", 800, 20, 10),
    (7499, "ALLEN", "SALESMAN", 7698, "20-Feb-81", 1600, 300, 30),
    (7521, "WARD", "SALESMAN", 7698, "22-Feb-81", 1250, 500, 30),
    (7566, "JONES", "MANAGER", 7839, "2-Apr-81", 2975, 0, 20),
    (7654, "MARTIN", "SALESMAN", 7698, "28-Sep-81", 1250, 1400, 30),
    (7698, "BLAKE", "MANAGER", 7839, "1-May-81", 2850, 0, 30),
    (7782, "CLARK", "MANAGER", 7839, "9-Jun-81", 2450, 0, 10),
    (7788, "SCOTT", "ANALYST", 7566, "19-Apr-87", 3000, 0, 20),
    (7839, "KING", "PRESIDENT", 0, "17-Nov-81", 5000, 0, 10),
    (7844, "TURNER", "SALESMAN", 7698, "8-Sep-81", 1500, 0, 30),
    (7844, "DIWAKAR", "SALESMAN", 7698, "9-Sep-81", 1000, 0, 30),
    (7876, "ADAMS", "CLERK", 7788, "23-May-87", 1100, 0, 20)
]).toDF("empno", "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno")

# partitionWindow = Window.partitionBy(col("deptno")).orderBy(col("sal").desc())
dfs = empDF.where("mgr in (7698, 7788)")
dfs.show(50)

arrayArrayData = [
    ("James", [["Java", "Scala", "C++"], ["Spark", "Java"]]),
    ("Michael", [["Spark", "Java", "C++"], ["Spark", "Java"]]),
    ("Robert", [["CSharp", "VB"], ["Spark", "Python"]])
]

df = spark.createDataFrame(data=arrayArrayData, schema=['name', 'subjects'])
df.printSchema()
df.show(truncate=False)
df.select(df.name, explode(df.subjects).alias("Exploded_ListSubject")).show(truncate=False)
df.select(df.name, flatten(df.subjects).alias("Flatten_Subjects")).show()


arrayData = [
    ('James', ['Java', 'Scala'], {'hair': 'black', 'eye': 'brown'}),
    ('Michael', ['Spark', 'Java', None], {'hair': 'brown', 'eye': None}),
    ('Robert', ['CSharp', ''], {'hair': 'red', 'eye': ''}),
    ('Washington', None, None),
    ('Jefferson', ['1', '2'], {})]

df = spark.createDataFrame(data=arrayData, schema=['name', 'knownLanguages', 'properties'])
df.printSchema()
df.show()

df2 = df.select(df.name, explode(df.knownLanguages).alias("knownLanguages"))
df2.printSchema()
df2.show()

df3 = df.select(df.name, explode(df.properties))
df3.printSchema()
df3.show()

""" with list """
df.select(df.name, explode_outer(df.knownLanguages)).show()
""" with dict """
df.select(df.name, explode_outer(df.properties)).show()

""" with list """
df.select(df.name, posexplode(df.knownLanguages)).show()
""" with dict """
df.select(df.name, posexplode(df.properties)).show()

""" with list """
df.select(col("name"), posexplode_outer(col("knownLanguages"))).show()

""" with dict """
df.select(df.name, posexplode_outer(df.properties)).show()
