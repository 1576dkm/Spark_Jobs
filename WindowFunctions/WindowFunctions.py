from pyspark.sql.window import Window
from pyspark.sql.functions import when, col, rank, lag, lead, row_number, dense_rank, percent_rank, ntile, cume_dist, \
    first, last, regexp_extract, regexp_replace, lit, avg, sum
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
empDF.createOrReplaceTempView("emp")

partitionWindow = Window.partitionBy(col("deptno")).orderBy(col("sal").desc())

#  Ranking window function.

rankTest = spark.sql("SELECT *,RANK() OVER (partition by deptno "
                     "ORDER BY sal desc) as rank FROM emp")
rankTest.show(50)
rankTest1 = rank().over(partitionWindow)

empDF.select("*", rankTest1.alias("Ranks")).show(50)

denseRank = spark.sql("SELECT empno,ename,deptno,sal,DENSE_RANK() OVER (PARTITION BY deptno ORDER BY sal desc) as "
                      "dense_rank FROM emp")
denseRank.show(50)
denseRank1 = dense_rank().over(partitionWindow)
empDF.select("*", denseRank1.alias("DenseRanks")).show(50)

rowNumber = spark.sql("SELECT empno,ename,deptno,sal,ROW_NUMBER() OVER (PARTITION BY deptno ORDER BY sal desc) as "
                      "row_num FROM emp")
rowNumber.show(50)

rowNumber1 = row_number().over(partitionWindow)
empDF.select("empno", "ename", "deptno", "sal", rowNumber1.alias("row_number")).show(50)

percentRank = spark.sql("SELECT empno,ename,deptno,sal,PERCENT_RANK() OVER (PARTITION BY deptno ORDER BY sal desc) as "
                        "per_rank FROM emp")
percentRank.show(50)

percentRank1 = percent_rank().over(partitionWindow)
empDF.select("empno", "ename", "deptno", "sal", percentRank1.alias("Percent_Rank")).show(50)

ntiles = spark.sql("SELECT empno,ename,deptno,sal,NTILE(5) OVER (PARTITION BY deptno ORDER BY sal desc) as "
                   "ntile FROM emp")
ntiles.show(50)

ntiles1 = ntile(5).over(partitionWindow)
empDF.select("empno", "ename", "deptno", "sal", ntiles1.alias("Ntile")).show(50)

#  Analytic window Functions.

cumeDists = spark.sql("SELECT empno,ename,deptno,sal,CUME_DIST() OVER (PARTITION BY deptno ORDER BY sal desc) as "
                      "cumeDists FROM emp")
cumeDists.show(50)

cumeDists1 = cume_dist().over(partitionWindow)
empDF.select("empno", "ename", "deptno", "sal", cumeDists1.alias("CumeDists1")).show(50)

firstValues = spark.sql("SELECT empno,ename,deptno,sal,FIRST_VALUE(sal) OVER (PARTITION BY deptno ORDER BY sal desc) "
                        "as FirstValue FROM emp")
firstValues.show(50)

firstValue1 = first(col("sal")).over(partitionWindow)
empDF.select("empno", "ename", "deptno", "sal", firstValue1.alias("FirstValue1")).show(50)

lastValue = spark.sql("SELECT empno,ename,deptno,sal,LAST_VALUE(sal) OVER (PARTITION BY deptno ORDER BY sal desc "
                      "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as LastValue FROM emp")
lastValue.show(50)

# Define new window partition to operate on row frame
partitionWindowWithUnboundedFollowing = Window.partitionBy(col("deptno")).orderBy(col("sal").desc()) \
    .rowsBetween(Window.currentRow, Window.unboundedFollowing)

lastValue1 = last(col("sal")).over(partitionWindowWithUnboundedFollowing)
empDF.select("empno", "ename", "deptno", "sal", lastValue1.alias("LastValue1")).show(50)

leadTest = spark.sql("SELECT empno,deptno,sal,lead(sal) OVER (PARTITION BY deptno ORDER BY sal desc) as next_val FROM "
                     "emp")
leadTest.show(50)

leadTest1 = lead(col("sal"), 1, 0).over(partitionWindow)
empDF.select("empno", "deptno", "sal", leadTest1.alias("next_vals")).show(50)

lagTest = spark.sql("SELECT empno,deptno,sal,lag(sal) OVER (PARTITION BY deptno ORDER BY "
                    "sal desc) as pre_val FROM emp")
lagTest.show(50)

lagTest1 = lag(col("sal"), 1, 0).over(partitionWindow)
empDF.select("empno", "deptno", "sal", lagTest1.alias("pre_vals")).show(50)

# Aggregate functions

SumTest = spark.sql("SELECT empno,ename,deptno,sal,sum(sal) OVER (PARTITION BY deptno ORDER BY sal desc "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as SumOfPreviousAndCurrent FROM emp")
SumTest.show(50)

AvgTest = spark.sql("SELECT empno,ename,deptno,sal,avg(sal) OVER (PARTITION BY deptno ORDER BY sal desc "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as AvgOfPreviousAndCurrent FROM emp")
AvgTest.show(50)

MaxTest = spark.sql("SELECT empno,ename,deptno,sal,max(sal) OVER (PARTITION BY deptno ORDER BY sal desc "
                    "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as MaxOfPreviousAndCurrent FROM emp")
MaxTest.show(50)

MinTest = spark.sql("SELECT empno,ename,deptno,sal,min(sal) OVER (PARTITION BY deptno ORDER BY sal desc "
                    "ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) as MinOfCurrentAndTillLast FROM emp")
MinTest.show(50)

countEmp = spark.sql("SELECT empno,ename,deptno,sal,count(deptno) OVER (PARTITION BY deptno ORDER BY sal desc "
                     "ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as countOfEmployee FROM emp")
countEmp.show(50)

countEmp1 = spark.sql("select empno,ename,emp.deptno,sal,t.cnt from emp inner join (select deptno, count(deptno) cnt "
                      "from emp group by deptno) t on emp.deptno = t.deptno ")
countEmp1.show(50)

empDF = empDF.select("*", lit(1).alias("Counts"))
# empDF = empDF.withColumn("Counts", when(empDF.job.isNull(), lit("USA")).otherwise(1))
temp = empDF.count()
partitionWindow = Window.partitionBy(col("job")).orderBy(col("job").desc()).rowsBetween(Window.unboundedPreceding,
                                                                                        Window.unboundedFollowing)
sumtest = sum(empDF.Counts).over(partitionWindow)
empDF = empDF.select("*", sumtest.alias("Sums"))
empDF.select(sum(empDF.Counts).alias("TotalCounts")).show(50)

print(temp)
empDF.show(50)
empDF.select("*")
