from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("RandomForest").config("spark.executor.heartbeatInterval", "60s").getOrCreate()
sc = spark.sparkContext

x: object = sc.parallelize([("a", 1), ("b", 1), ("a", 1), ("a", 1),
                    ("b", 1), ("b", 1), ("b", 1), ("b", 1)], 3)

# Applying reduceByKey operation on x
y = x.reduceByKey(lambda v1, v2: v1 + v2)
print(y.collect())

a = int(input("Enter a number!!!\n"))
if a in {1, 2, 3}:
    print("%d is a prime number" % a)
    print(f"{a} is a prime number")

b = 'ferrets'
print("I love %s and %s" % (a, b))

from random import random

a, b = random(), random()
res = "a" if a > b else "b"
print(res)
a, b = 6, 3
print(a < b and a or b)


def sayhello(): print('Hello')


hi = sayhello()


def outer():
    x = 1

    def inner():
        nonlocal x
        x = 2
        print("inner:", x)

    inner()
    print("outer:", x)


outer()

b = 'ferrets'
print("I love %s %s " % (a, b))

a = 7
if a > 6:
    print("%s is good" % a)

if 1:
    print("yay")

a = 3
while (a > 0):
    print(a)
    a -= 1

a = 9
while (a > 0):
    print(a)
    a -= 1
else:
    print("Reached 0")

a = 3
while (a > 0):
    print(a)
    a -= 1
    if a == 1: break;
else:
    print("Reached 0")

for i in 'Python':  # First Example
    print('Current Letter :', i)

fruits = ['banana', 'apple', 'mango']
for i in fruits:  # Second Example
    print('Current fruit :', i)

print("Good bye!")

for i in range(len(fruits)):
    print('Current fruit :', fruits[i])
print("Good bye!!")

for i in range(len(fruits)):
    print('Current index :', i)

for num in range(10, 20):  # to iterate between 10 to 20
    for i in range(2, num):  # to iterate on the factors of the number
        if num % i == 0:  # to determine the first factor
            j = num / i  # to calculate the second factor
            print('%d equals %d * %d' % (num, i, j))
            break  # to move to the next number, the #first FOR
    else:  # else part of the loop
        print(num, 'is a prime number')

for a in [4, 7, 9]:
    print(a)
for num in range(1, 20):
    for i in range(2, num):  # to iterate on the factors of the number
        if num % i == 0:  # to determine the first factor

            break  # to move to the next number, the #first FOR
    else:  # else part of the loop
        print(num, 'is a prime number')

for i in range(1, 6):
    for j in range(i):
        print("*", end=' ')
    print()
for i in 'break':
    print(i)
    if i == 'a': break;  # the loop stops executing, and control shifts to the first statement outside it.
i = 0
while (i < 8):
    i += 1
    if (i == 6): continue  # it skips the statements after ‘continue’.
    print(i)  # It then shifts to the next item in the sequence and executes the block of code for it.
for i in 'selfhelp':
    pass  # The interpreter does not ignore it, but it performs a no-operation (NOP).
print(i)


def evens(n):
    print(0)
    x = 2
    for i in range(1, n):
        for j in range(i + 1):
            print(x, end="")
        x *= 2
        print()


n = 4
evens(n)


def week(i):
    switcher = {
        0: 'Sunday',
        1: 'Monday',
        2: 'Tuesday',
        3: 'Wednesday',
        4: 'Thursday',
        5: 'Friday',
        6: 'Saturday'
    }
    return switcher.get(i, "Invalid day of week")


print(week(4))


def switch_demo(argument):
    switcher = {
        1: "January",
        2: "February",
        3: "March",
        4: "April",
        5: "May",
        6: "June",
        7: "July",
        8: "August",
        9: "September",
        10: "October",
        11: "November",
        12: "December"
    }
    print(switcher.get(argument, "Invalid month"))


switch_demo(5)


def zero():
    return 'zero'


def one():
    return 'one'


def indirect(i):
    switcher = {
        0: zero,
        1: one,
        2: lambda: 'two'
    }
    func = switcher.get(i, lambda: 'Invalid')
    return func()


print(indirect(2))

downbytwo = lambda e: e - 2
print(downbytwo(0))

z = lambda a=2: print("Hello")
z()

o = lambda x=1, y=2, z=3: x + y + z  # Here the default value of x,y,z is given.
print(o(2, 3))  # here we gave the value of x and y.

a, b = 1, 2
y = lambda a, b: a + b  # Here we need to pass the argument
print(y(5, 9))

a, b = 1, 2
y = lambda: a + b  # Here we don't need to pass the argument
print(y())

(lambda: print("Hi"))()

numbers = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
print(list(filter(lambda x: x % 2 != 0,
                  numbers)))  # The filter() function takes two parameters- a function, and a list to operate on.
# Finally, we apply the list() function on this to return a list.

print(list(map(lambda x: x % 3 == 0, numbers)))  # The map() function, unlike the filter() function,
# returns values of the expression for each element in the list.

from functools import reduce

print(reduce(lambda x, y: y + x, numbers))  # the reduce() function takes two parameters- a function, and a list.


# It performs computation on sequential pairs in the list and returns one output.

def func():
    """
      This function prints out a greeting
    """
    print("Hi")


func()
print(func.__doc__)
print(sum.__doc__)


def hello1():
    pass


print(hello1())


def sum(a, b):
    print("a+b=", (a + b))


sum(2, 3)
sum(3.0, 2)


def function(a):
    if a % 2 == 0:
        return 0
    else:
        return 1


print(function(5))


def sum1(a, b):
    return a + b


print("a+b=", sum1(4, 7))


def func7():
    print("7")


func7()

myvar = lambda a, b: (a * b) + 2
print(myvar(3, 5))


def fact(x):
    if x == 1:
        return 1
    return x * fact(x - 1)


print(fact(5))


def remainder(a, b):
    return a % b


print(remainder(3, 2))


def sayhello(*names):
    for name in names:
        print("Hello", name)


sayhello('Ayushi', 'Leo', 'Megha')

print(all((2, 3, 9)))
print(abs(-7))
print(bool(8))
a = bytearray(4)
print(a)

exec(compile('a=5\nb=7\nprint(a+b)', '', 'exec'))
print(bytes('hello', 'utf-8'))


class Singleton:
    __instance = None

    @staticmethod
    def getInstance():
        """ Static access method. """
        if Singleton.__instance == None:
            Singleton()
        return Singleton.__instance

    def __init__(self):
        """ Virtually private constructor. """
        if Singleton.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            Singleton.__instance = self


s = Singleton()
print(s)

s = Singleton.getInstance()
print(s)

s = Singleton.getInstance()
print(s)
locals()


class person:
    def __init__(self):  # whenever an object calls its method, the object itself is passed as the first argument.
        # So, ob.func() translates into MyClass.func(ob).
        print("A person")


class student(person):
    def __init__(self):
        super().__init__()
        print("A student")


Avery = student()

print(set(zip([1, 2], [3, 4, 5])))


# declare our own string class
class String:

    # magic method to initiate object
    def __init__(self, string):
        self.string = string

        # print our string object

    def __repr__(self):
        return 'Object: {}'.format(self.string)

    # Driver Code


if __name__ == '__main__':
    # object creation
    string1 = String('Hello')

    # print object location
    print(string1)


# declare our own string class
class String:

    # magic method to initiate object
    def __init__(self, string):
        self.string = string

        # print our string object

    def __repr__(self):
        return 'Object: {}'.format(self.string)

    def __add__(self, other):
        return self.string + other

    # Driver Code


if __name__ == '__main__':
    # object creation
    string1 = String('Hello')

    # concatenate String object and a string
    print(string1 + ' Geeks')


class Car:
    def __init__(self, brand, model, color, fuel):
        self.brand = brand
        self.model = model
        self.color = color
        self.fuel = fuel

    def start(self):
        pass

    def halt(self):
        pass

    def drift(self):
        pass

    def speedup(self):
        pass

    def turn(self):
        pass


blackverna = Car('Hyundai', 'Verna', 'Black', 'Diesel')
print(blackverna)
f = blackverna.fuel
print(f)
d = blackverna.drift()
print(d)


class Try:
    def __init__(self):
        pass

    def printhello(self, name):
        print("Hello,", name)
        return name


obj = Try()
p = obj.printhello('Lisa')
print(p)


class Animal:
    def __init__(self, species, gender):
        self.species = species
        self.gender = gender


fluffy = Animal('Dog', 'Female')
g = fluffy.gender
print(g)


class Fruit:
    def printstate(self, state):
        print("The orange is", state)


orange = Fruit()
orange.printstate("ripe")


class Result:
    def __init__(self, phy, chem, math):
        self.phy = phy
        self.chem = chem
        self.math = math

    def printavg(self):
        print("Average=", (self.phy + self.chem + self.math) / 3)


rollone = Result(86, 95, 85)
pa = rollone.chem
print(pa)
rollone.printavg()


class LED:
    def __init__(self):
        self.lit = False


obj = LED()
pl = obj.lit
print(pl)


class Try3:
    def __init__(self, name):
        self.name = name


obj1 = Try3('Leo')
pn = obj1.name
print(pn)


def demofunc(a, b):
    """
  //This function is to demonstrate a few built-in functions in Python
    """
    print("Begin")
    print(max(a, b))
    print(abs(a), abs(b))
    print(float(a), b)
    print(callable(a))
    print(hash(a), hash(b))
    print("length", len('ab'))
    print(type(a))
    for i in range(2, 4): print(i)


demofunc(2, 3)


class vehicle:
    def __init__(self, color):
        self.color = color

    def start(self):
        print("Starting engine")

    def showcolor(self):
        print("I am", self.color)


car = vehicle('black')
car.start()
car.showcolor()


class three:
    value = 7


vp = three
sp = vp.value
print(sp)


class three:
    def func(self, val):
        self.val = val


t = three()
t.func(8)
tp = t.val
print(tp)
t.func(6)  # Also lets us re-initialize attributes
tps = t.val
print(tps)


class demo:
    def __new__(self):
        return 'dataflair'


d = demo()
print(d)
dp = type(d)
print(dp)


class citrus:
    def __init__(self):
        self.detoxifying = True

    def show(self):
        print("I detoxify") if self.detoxifying == True else print("I do not detoxify")


kt = citrus()
kt.show()
kt.color = 'orange'  # create a new attribute exclusively for this object and read it when defining values.
print("I am", kt.color)


class color:
    def show(self):
        print("You can see me")


orange = color()
orange.show()


class ComplexNumber:
    def __init__(self, r=0, i=0):
        self.real = r
        self.imag = i

    def getData(self):
        print("{0}+{1}j".format(self.real, self.imag))


# Create a new ComplexNumber object
c1 = ComplexNumber(2, 3)

# Call getData() function
# Output: 2+3j
c1.getData()

# Create another ComplexNumber object
# and create a new attribute 'attr'
c2 = ComplexNumber(5)
c2.attr = 10

# Output: (5, 0, 10)
print((c2.real, c2.imag, c2.attr))


# but c1 object doesn't have attribute 'attr'
# AttributeError: 'ComplexNumber' object has no attribute 'attr'
# c1.attr


# del c1.imag
# c1.getData()
# del ComplexNumber.getData
# c1.getData()
# c1 = ComplexNumber(1,3)
# del c1
# print(c1)


class Parrot:
    # class attribute
    species = "bird"

    # instance attribute
    def __init__(self, name, age):
        self.name = name
        self.age = age

    # instance method
    def sing(self, song):
        return "{} sings {}".format(self.name, song)

    def dance(self):
        return "{} is now dancing".format(self.name)


# instantiate the Parrot class
blu = Parrot("Blu", 10)
woo = Parrot("Woo", 15)

# access the class attributes
print("Blu is a {}".format(blu.__class__.species))  # we access the class attribute using __class __.species
print("Woo is also a {}".format(woo.__class__.species))

# access the instance attributes
print("{} is {} years old".format(blu.name, blu.age))  # we access the instance attributes using blu.name and blu.age
print("{} is {} years old".format(woo.name, woo.age))

# call our instance methods
print(blu.sing("'Happy'"))
print(blu.dance())


# parent class
class Bird:

    def __init__(self):
        print("Bird is ready")

    def whoisThis(self):
        print("Bird")

    def swim(self):
        print("Swim faster")


# child class
class Penguin(Bird):

    def __init__(self):
        # call super() function
        super().__init__()  # we use super() function before __init__() method.
        # This is because we want to pull the content of __init__() method from the parent class into the child class.
        print("Penguin is ready")

    def whoisThis(self):
        print("Penguin")

    def run(self):
        print("Run faster")


peggy = Penguin()
peggy.whoisThis()
peggy.swim()
peggy.run()


class Computer:

    def __init__(self):
        self.__maxprice = 900  # we can restrict access to methods and variables.
        # This prevent flower-classification-with-tpus from direct modification which is called encapsulation.
        # we denote private attribute using underscore as prefix i.e single “ _ “ or double “ __“.

    def sell(self):
        print("Selling Price: {}".format(self.__maxprice))

    def setMaxPrice(self, price):
        self.__maxprice = price


c = Computer()
c.sell()

# change the price
c.__maxprice = 1000  # we can’t change it because Python treats the __maxprice as private attributes
c.sell()

# using setter function
c.setMaxPrice(1000)  # Here we changed the value of maxprice using setmaxprice methon of Computer class
c.sell()


class Parrot1:

    def fly(self):
        print("Parrot can fly")

    def swim(self):
        print("Parrot can't swim")


class Penguin1:

    def fly(self):
        print("Penguin can't fly")

    def swim(self):
        print("Penguin can swim")


# common interface
def flying_test(
        bird):  # To allow polymorphism, we created common interface i.e flying_test() function that can take any object.
    bird.fly()


# instantiate objects
blu = Parrot1()
peggy = Penguin1()

# passing the object
flying_test(blu)
flying_test(peggy)

import datetime


class Logger(object):
    def log(self, message):
        print(message)


class TimestampLogger(Logger):
    def log(self, message):
        message = "{ts} {msg}".format(ts=datetime.datetime.now().isoformat(),
                                      msg=message)
        super(TimestampLogger, self).log(message)


l = Logger()
l.log("Hi!!!!!!")
t = TimestampLogger()
t.log('hi!')


class Point:
    def __init__(self, x=0, y=0):
        self.x = x
        self.y = y

    def __str__(self):
        return "({0},{1})".format(self.x, self.y)

    def __add__(self, other):
        x = self.x + other.x
        y = self.y + other.y
        return Point(x, y)


p1 = Point(2, 3)
p2 = Point(-1, 2)
print(p1 + p2)
print(p1.__add__(p2))
print(Point.__add__(p1, p2))


class Point:
    def __init__(self, x=0, y=0):
        self.x = x
        self.y = y

    def __str__(self):
        return "({0},{1})".format(self.x, self.y)

    def __lt__(self, other):
        self_mag = (self.x ** 2) + (self.y ** 2)
        other_mag = (other.x ** 2) + (other.y ** 2)
        return self_mag < other_mag


p1 = Point(1, 1)
p2 = Point(-2, -3)
cp = (p1 < p2)
print(cp)


def outerFunction():
    global a
    a = 20

    def innerFunction():
        global a
        a = 30
        print('a =', a)


a = 10
outerFunction()
print('a =', a)


def printHello():
    print("Hello")


a = printHello()

genre = ["pop", "music", "poop", "pkomre", "tomato"]
for i in genre:
    print("I like", i)

# Use of break statement inside loop

for val in "string":
    if val == "i":
        break
    print(val)

print("The end")

# Program to show the use of continue statement inside loops

for val in "string":
    if val == "i":
        continue
    print(val)

print("The end")

for char in 'PYTHON STRING':
    if char == ' ':
        break
    print(char, end='')

    if char == 'O':
        continue

import datetime

t = datetime.datetime.now()
print("", str(t))  # Readable
print(repr(t))

mydict = {1: 2, 2: 4, 3: 6}
print(mydict[2])

languages = [('English', 'Albanian'), 'Gujarati', 'Hindi', 'Romanian', 'Spanish']
print(languages[0][1])

even = [2 * i for i in range(1, 11) if i % 3 == 0]
print(even)

print((1, 2, 3) > (4, 5, 6))

from collections import Counter

c = Counter({'a': 3, 'b': 2,
             'c': 1})  # counter is a container that keeps count of the number of occurrences of any value in the container.
print(c)

c = Counter('Hello')
print(c)

c = Counter(a=3, b=2, c=1)
print(c)

c = Counter()  # To declare an empty counter in python, and then populate it, we use the update() method.
c.update('bfg')
print(c)
print(c['f'])  # To get a value’s count, we pass it as an index to the counter we defined.
print(c['h'])  # it gives an output wich is 0 because 'bfg' doesn't have h in it

for i in c.elements():
    print(i, ":", c[i])

c1 = Counter('hello')
c2 = Counter('help')
print(c1 + c2)
print(c1 & c2)

from collections import defaultdict

d = defaultdict(lambda: 35)
d['Ayushi'] = 95
d['Bree'] = 89
d['Leo'] = 90.5
d['Adam']
print(d.__missing__(
    'Adam'))  # we did not initialize ‘Adam’. So, it took 35, because that’s what our function returns to defaultdict().
print(d["Adam"])
print(d["Bree"])

# We can tell the interpreter what type of values we’re going to work with. We do this by passing it as an argument to defaultdict().
d = defaultdict(list)
for i, j in [('a', (1, 2)), ('b', (3, 4)), ('c', (5, 6))]:
    d[i].append(j)
    print(d)

print(d)

# Python OderDict, remembers the order in which the key-value pairs were added.
from collections import OrderedDict

o = OrderedDict()
o['a'] = 3
o['c'] = 1
o['b'] = 4
print(o)
o.move_to_end('c', last=False)  # to move key to the front.
print(o)
o.move_to_end('c', last=True)  # to move the key to last
print(o)

print(o.popitem())  # it will remove item from last.
print(o)
print(o.popitem(last=False))  # it will remove item from front.
print(o)

from collections import namedtuple  # This is a container that lets us access elements using names/labels.

colors = namedtuple('color', 'r g b')
red = colors(r=255, g=0, b=0)
print(red.r)
print(red[1])
print(getattr(red, 'r'))

print(red._asdict())  # To convert a namedtuple into Python dictionary.

print(colors._make(['1', '2', '3']))

print(colors(**{'r': 9, 'g': 4, 'b': 1}))

print(red._fields)  # to check What Fields Belong to the Tuple.

print(red._replace(g=3))  # Python namedtuples are immutable. But to change a value, we can use the _replace() method.

dict3 = {1: 'carrots', 'two': [1, 2, 3]}
print(dict3["two"])

print(dict(([1, 2], [2, 4], [3,
                             6])))  # it takes only one agrument and convert a compatible combination of constructs into a Python dictionary.

animals = {}
animals[1] = 'dog'
animals[2] = 'cat'
animals[3] = 'ferret'
print(animals)

# A shallow copy only copies contents, and the new construct points to the same memory location.
# But a deep copy copies contents, and the new construct points to a different location.
# The copy() method creates a shallow copy of the Python dictionary.
dict4 = {1: 2, 2: 4, 3: 6}
newdict = dict4.copy()
print(newdict)
print(newdict.popitem())
dict1 = {4: {1: 2, 2: 4}, 8: 16}
print(dict1[4][2])

from datetime import datetime

# It prints the current time.
t1 = datetime.now()
t2 = t1.__str__()
print(t2)
t3 = datetime.now().time()  # it prints time without date
print(t3)
print(t1.tzinfo)

import time

print(time.strftime("%d/%m/%Y"))  # it will print the Date without time like (09/08/2019)
print(time.strftime("%a, %d %b %Y %H:%M:%S"))  # It will print the Date like (Fri, 09 Aug 2019 11:25:44)
