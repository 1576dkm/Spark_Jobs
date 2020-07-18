from mysql.connector import connect
from pyspark.sql import SparkSession

from pyspark.sql import DataFrameWriter

spark = SparkSession.builder.appName("Test_Parquet").master("local[*]").getOrCreate()

sc = spark.sparkContext

sc.setSystemProperty("spark.dynamicAllocation.enabled", "true")
sc.setSystemProperty("spark.dynamicAllocation.initialExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.minExecutors", "6")
sc.setSystemProperty("spark.dynamicAllocation.schedulerBacklogTimeout", "0.5s")
sc.setSystemProperty("spark.speculation", "true")
names = spark.read.csv("name.csv", header="true", inferSchema="true")
names.printSchema()

# names = spark.createDataFrame([(1, "", "x", " ",), (2, "", "b", " ",), (5, "", "c", " ",), (8, "", "d", " ",)],
#                               ("st", " ", "ani", " ",))
names.show()
# temp = list(names.columns)
# print(temp)
# temp.remove(" 1")
# temp.remove(" 3")
# temp.remove("_c5")
# print(temp)
# names = names.select(temp)
names = names.drop(" 1", " 3")
temp = names.filter(lambda x: x)
names = names.filter(names.Name != " ")
names.show()
f = open("name.csv", "rt")
print(f.read())
f1 = open("abc", "r")
# f1.write(" Mobile")
f2 = open("xyz.csv", "w")
for data in f:
    f2.write(data)

mydb = connect(host="localhost", user="root", passwd="Prabhu07@", database="Diwakar", auth_plugin='mysql_native_password')
mycursor = mydb.cursor()
# mycursor.execute("show databases")
mycursor.execute("select * from bst")
result = mycursor.fetchall()
for i in result:
    print(i)

pile = [7, 4, 8, 3, 9, 0, 1, 9]
pile = list(set(pile))
pile.sort(reverse=True)
print(pile)
pile.remove(pile[0])
print(pile[0])


# lst1 = []
# for i in range(len(lst)):
#     lst3 = []
#     for j in range(i + 1):
#         lst3.append(lst[j])
#     lst3.sort(reverse=True)
#     lst1.append(tuple(lst3))
# print(lst1)


class Car:
    _wheels = 4

    def __init__(self):
        self.mil = 10
        self.com = "BMW"

    def __myPrivateMethod(self):
        print("my")

    def pil(self):
        return self.com

    @classmethod
    def wheel(cls):
        return cls.wheels

    @staticmethod
    def add(x, y):
        return x + y

    def sum(self):
        return self.add(2, 6)


c1 = Car()
c2 = Car()

c1.mil = 8

Car.wheels = 5

print(c1.com, c1.mil, c1.wheels, c1.wheel(), c1.add(6, 4))
print(c2.com, c2.mil, c2.wheels, c2.sum())
print(dir(c1))
c1._Car__myPrivateMethod()
spark.read.csv("practice.csv", header="true", inferSchema="true").createTempView("t")
temp = spark.sql("select min(id) as id, name from t group by name order by id")
temp.show(10)
