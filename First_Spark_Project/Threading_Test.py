# import re
# from threading import Thread
# from time import sleep
# from typing import List
#
# from pyspark.sql import SparkSession
#
# spark = SparkSession.builder.appName("RandomForest").config("spark.executor.heartbeatInterval", "60s").getOrCreate()
# sc = spark.sparkContext
#
#
# def cube(num):
#     sleep(1)
#     print(f"Cube : {num ** 3}")
#
#
# def square(num):
#     print(f"Square : {num ** 2}")
#
#
# def main():
#     t1 = Thread(target=cube, args=(10,))
#     t2 = Thread(target=square, args=(10,))
#
#     t1.start()
#
#     t2.start()
#
#     print("Done")
#
#
# main()
#
#
# class myThread(Thread):
#     def run(self):
#         print("My Thread Executed")
#
#
# def run():
#     myThread.start()
#
#
# l = [1, ["p", ["2", 4.0]], ["q", ["r", ["s", [9.6]]]], "t"]  # [[1, 3.8], 7, 5, [[[9.5, 4]]],[[3,7]]]
#
# output = []
#
#
# def reemove_Nestings(l):
#     for i in l:
#         if type(i) == list:
#             reemove_Nestings(i)
#         else:
#             output.append(i)
#
#         # Driver code
#
#
#  print('The original list: ', l)
#  reemove_Nestings(l)
#  print('The list after removing nesting: ', output)
# t = str(l)
# e: List[str] = (t.replace("[", "").replace("]", "").replace(" ", "").split(","))
# print(e)
# p = re.compile(r"\'+")
# dict = {'string': 0, 'Int': 0, 'float': 0}
# for i in e:
#     if p.match(i):
#         dict['string'] = dict['string'] + 1
#     elif '.' in i:
#         dict['float'] = dict['float'] + 1
#     else:
#         dict['Int'] = dict['Int'] + 1
# flag = True
# for i in dict.values():
#     if i % 2 != 0:
#         flag = False
#         break
# if flag:
#     print("sym")
# else:
#     print("Non Sym")
#
#
#


# rows, cols, rotations = [int(x) for x in input().strip().split(" ")]
test_list = [6, 1, 8, 3, 4, 9, 2, 7]

# printing original list
print("Original list : " + str(test_list))


def rotate(l, r):
    test = l[len(l) - r:] + l[:len(l) - r]

    print(f"List after left rotate by {r} : " + str(test))
    return test


print(rotate(test_list, 4))




def larrysArray(a):
    count = 0

    def merge(left, right):
        nonlocal count
        result = []
        i, j = 0, 0
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                result.append(left[i])
                i += 1
            else:
                count += (len(left) - i)
                print(left)
                result.append(right[j])
                j += 1
        result += left[i:]
        result += right[j:]
        return result

    def merge_Sort(lst):
        if len(lst) <= 1:
            return lst
        mid = len(lst) // 2
        left = merge_Sort(lst[:mid])
        right = merge_Sort(lst[mid:])
        return merge(left, right)
    merge_Sort(A)
    return 'YES' if count % 2 == 0 else 'NO'


t = int(input())

for t_itr in range(t):
    n = int(input())

    A = list(map(int, input().rstrip().split()))

    result = larrysArray(A)
    print(result)
