from operator import add
from pyspark import SparkContext

sc = SparkContext("local[*]", "example")
rdd = sc.textFile("C:\\Users\janjanam.sudheer\Desktop\data.csv")
rdd1 = rdd.map(lambda x : x.split(',')).map(lambda x: (x[0],x[1].split(';'))).filter(lambda line: "Country" not in line).map(lambda z : (z[0],list(map(lambda y : int(y),z[1])))).reduceByKey(lambda a, b: list(map(add,a,b)))
print(rdd1.take(10))