# Enter your code here. Read input from STDIN. Print output to STDOUT
# from heapq import heappush, heappop
#
# N = int(input().strip())
#
# heap = []
# delset = set([])
#
# for _ in range(N):
#     q = [map(int, input().strip().split(" "))]
#
#     if q[0] == 1:
#         heappush(heap, q[1])
#         delset.discard(q[1])
#     elif q[0] == 2:
#         delset.add(q[1])
#     elif q[0] == 3:
#         while True:
#             item = heap[0]
#             if item not in delset:
#                 break
#             delset.discard(heappop(heap))
#         print(item)
import sys

arr = [6, -992, 99999, 4, 1, -99]


def max_min(arr):
    mins = 100
    maxs = -900
    for i in range(len(arr)):
        mins = arr[i] ^ ((mins ^ arr[i]) & -(mins < arr[i]))
        maxs = maxs ^ ((maxs ^ arr[i]) & -(maxs < arr[i]))
    print(mins)
    print(maxs)


max_min(arr)


def adding(x, y):
    while y != 0:
        carry = x & y
        x = x ^ y
        y = carry << 1
    print(x)


adding(5, 6)


def substracting(x, y):
    while y != 0:
        carry = ~x & y
        x = x ^ y
        y = carry << 1
    print(x)


substracting(5, 2)


def multiplication(x, y):
    pass


multiplication(3, 5)


# x = 6
# y = 9
# r = y ^ ((x ^ y) & -(x < y))  # min
# print(r)
# r = x ^ ((x ^ y) & -(x < y))  # max
# print(r)


def isPowerOf2(v):
    f = False
    f = (v and not (v & (v - 1)))
    print(f)


def isOfOppositeSign(x, y):
    f = False
    f = (x ^ y) < 0
    print(f)


isPowerOf2(31)
isOfOppositeSign(9, 3)


def absoluteValue(n):
    char_bit = 8
    size_int = sys.getsizeof(int())
    print(size_int)
    mask = n >> (size_int * char_bit - 1)
    print((n + mask) ^ mask)
    # return (n ^ mask) - mask # we can use this also


absoluteValue(-99)
s1 = "abcbce"
s2 = "bce"


def process(s1, s2):
    l1 = len(s1)
    l2 = len(s2)
    i = 0
    while i < l1:
        j = 0
        if s1[i] == s2[j]:
            j += 1
            while j < l2 and s1[i + j] == s2[j]:
                j += 1
            if j == l2:
                return True
        i += 1
    return False


print(process(s1, s2))
