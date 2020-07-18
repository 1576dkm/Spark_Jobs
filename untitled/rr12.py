from collections import Counter

from pyspark.sql.functions import when
from pyspark.sql.session import SparkSession
from itertools import groupby
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import broadcast

spark = SparkSession.builder.appName("Flights_SparkSql").master("local[*]").getOrCreate()
#
sc = spark.sparkContext
# schema = StructType([StructField('Name', StringType(), True),
#                      StructField('DateTime', TimestampType(), True),
#                      StructField('Age', IntegerType(), True)])


# rdd1 = sc.parallelize(["Roses are red", "Violets are blue"])
# # rdd1.foreach(print)
# rdd1.flatMap(lambda x: (len(x), x)).coalesce(1).saveAsTextFile("final")
# lst = sc.parallelize(["a", "b", "c"])
# x = lst.fold("", lambda a, b: a + b)
# print(x)
rdd1 = sc.textFile("rough333.txt")
rdd2 = sc.parallelize(["Let's have some fun fun have .",
                       "To have fun you don't need any plans to ."])

# count of the total words per line.
output = rdd1.map(lambda t: t.split(" ")).map(lambda lists: (lists, len(lists)))
output.foreach(print)

# count of the words per line.
output1 = rdd1.map(lambda t: t.split(" ")).map(lambda lists: dict(Counter(lists)))
output1.foreach(print)

numbers = [1, 5, 3, 4, 3]


result = map(lambda x: x + x, numbers)
print(list(result))

testList = [("Sita", 1), ("Ram", 3), ("Ram", 4), ("Shyam", 5), ("Sita", 2)]

testList1 = [("Sita_English", 1), ("Ram_Maths", 4), ("Shyam_English", 5), ("Ram_English", 3), ("Sita_Maths", 2)]

out1 = list(map(lambda v: (v[0], sum(map(lambda s: s[1], v[1]))),
                groupby(sorted(testList, key=lambda x: x[0]), key=lambda x: x[0])))
print(out1)

out2 = list(map(lambda v: (v[0], sum(map(lambda s: s[1], v[1]))),
                groupby(sorted(testList1, key=lambda x: x[0].split("_")[0]), key=lambda x: x[0].split('_')[0])))
rm = max(map(lambda mv: mv[1], out2))
print(out2)
print(rm)

r = list(map(lambda v: (v[0], sum(map(lambda s: s[1], v[1]))),
             groupby(sorted(testList1, key=lambda x: x[0].split("_")[1]), key=lambda x: x[0].split('_')[1])))
print(r)

# num = int(input("Enter a number:\n"))
# print(num % 5)
#
#
# class three:
#     def func(self, val):
#         self.val = val
#
#
# t = three()
# t.func(8)
# tp = t.val
# print(tp)
# t.func(6)  # Also lets us re-initialize attributes
# tps = t.val
# print(tps)
#
#
# class three1:
#     def __init__(self):
#         self.val = input("What value?")
#
#
# tv = three1()
# pv = tv.val
# print(pv)
#
#
# class Polygon:
#     def __init__(self, no_of_sides):
#         self.n = no_of_sides
#         self.sides = [0 for i in range(no_of_sides)]
#
#     def inputSides(self):
#         self.sides = [float(input("Enter side " + str(i + 1) + " : ")) for i in range(self.n)]
#
#     def dispSides(self):
#         for i in range(self.n):
#             print("Side", i + 1, "is", self.sides[i])
#
#
# class Triangle(Polygon):
#     def __init__(self):
#         super().__init__(3)  # Polygon.__init__(self,3)
#
#     def findArea(self):
#         a, b, c = self.sides
#         # calculate the semi-perimeter
#         s = (a + b + c) / 2
#         area = (s * (s - a) * (s - b) * (s - c)) ** 0.5
#         print('The area of the triangle is %0.2f' % area)
#
#
# t = Triangle()
# t.inputSides()
# t.dispSides()
# t.findArea()
#
#
# def let_me_in():
#     password = '@dc#431'
#     print("The password is", password)
#
#
# expr = input('Enter an expression as x')
#
# eval(expr)

# val ld = mapd.groupBy(_._1.split(",").head).map { case (k, v) => k -> v.map(_._2).sum }
pile = [7, 4, 8, 3, 9, 0, 1, 9]
pile = list(set(pile))
pile.sort(reverse=True)
print(pile)


