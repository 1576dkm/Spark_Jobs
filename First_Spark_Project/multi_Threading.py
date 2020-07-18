# # Python program to illustrate the concept
# # of threading
# # importing the threading module
# import threading
#
#
# def print_cube(num):
#     """
#     function to print cube of given num
#     """
#     print(f"Cube: {num * num * num}")
#
#
# def print_square(num):
#     """
#     function to print square of given num
#     """
#     print(f"Square: {num * num}")
#
#
# if __name__ == "__main__":
#     # creating thread
#     t1 = threading.Thread(target=print_square, args=(10,))
#     t2 = threading.Thread(target=print_cube, args=(10,))
#
#     # starting thread 1
#     t1.start()
#     # starting thread 2
#     t2.start()
#
#     # wait until thread 1 is completely executed
#     t1.join()
#     # wait until thread 2 is completely executed
#     t2.join()
#
#     # both threads completely executed
#     print("Done!")
#
# from math import ceil
#
# s = input()
# col = int(ceil(len(s.strip()) ** .5))
# print(" ".join([s[i::col] for i in range(col)]))
#
# import math
#
# s = input().strip().replace(' ', '')
# cols = int(math.ceil(len(s) ** 0.5))
#
# for i in range(cols):
#     print(''.join([s[x] for x in range(i, len(s), cols)]), end=" ")
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
#
import bisect


# def bsearchSum(ll, first, last):
#     l=ll[0]
#     beg, end = 0, len(l)
#     first = bisect.bisect_left(l,first)
#     if first==end:
#         return 0
#     last = bisect.bisect_right(l,last,first,end)-1
#     if first>last:
#         return 0
#     return ll[1][last] if first==0 else ll[1][last]-ll[1][first-1]

# import sys
#
# if __name__ == '__main__':
#     n = int(sys.stdin.readline())
#
#     genes = sys.stdin.readline().split()
#
#     health = list(map(int, sys.stdin.readline().split()))
#
#     s = int(sys.stdin.readline())
#
#     mx, mn = 0, sys.maxsize
#
#     healthDict = {}
#     # test={}
#     sumDict = {}
#
#     root = [{}, None, set()]
#     root[1] = root
#
#     # Initialize tree and healthDict
#     for j in range(len(genes)):
#         gene = genes[j]
#         state = root
#         for c in gene:
#             if c not in state[0]:
#                 state[0][c] = [{}, root, ""]
#
#             state = state[0][c]
#
#         state[2] = gene
#
#         if gene not in healthDict:
#             healthDict[gene] = ([], [])
#             # test[gene]=[0 for i in range(n)]
#             sumDict[gene] = 0
#
#         sumDict[gene] += health[j]
#         healthDict[gene][0].append(j)
#         healthDict[gene][1].append(sumDict[gene])
#
#     queue = [root[0][char] for char in root[0]]
#     # Initialize tree fail pointer BFS
#     while queue:
#         newQueue = []
#         for state in queue:
#             for char in state[0]:
#                 child = state[0][char]
#
#                 fail = state[1]
#                 while True:
#                     if char in fail[0] or fail == root:
#                         break
#                     fail = fail[1]
#
#                 child[1] = fail[0].get(char, root)
#                 newQueue.append(child)
#
#         queue = newQueue
#     seen = {}
#     for s_itr in range(s):
#         firstLastd = sys.stdin.readline().split()
#
#         first = int(firstLastd[0])
#
#         last = int(firstLastd[1])
#
#         d = firstLastd[2]
#
#         totalHealth = 0
#
#         matchGene = {}
#         # matchGene[""] = 0
#
#         state = root
#         for c in d:
#             while c not in state[0] and state != root:
#                 state = state[1]
#             state = state[0].get(c, root)
#
#             tmp = state
#             while tmp != root:
#                 if tmp[2]:
#                     matchGene[tmp[2]] = matchGene.get(tmp[2], 0) + 1
#                 tmp = tmp[1]
#
#         for gene in matchGene:
#             ll1, ll2 = healthDict[gene][0], healthDict[gene][1]
#             first0 = bisect.bisect_left(ll1, first)
#             last0 = bisect.bisect_right(ll1, last, first0) - 1
#             if first0 > last0:
#                 continue
#             totalHealth += matchGene[gene] * (ll2[last0] - ll2[first0 - 1] if first0 else ll2[last0])
#
#         # if s_itr == 0:
#         #     mx = mn = totalHealth
#         # else:
#         mx = max(mx, totalHealth)
#         mn = min(mn, totalHealth)
#
#     print(mn, mx)


def initialize(s):
    n, z = len(s), ord('a')
    S, F, I = [[0] * L for _ in range(n + 1)], [1] * n, [1] * n
    for i, v in enumerate(s, 1):
        for j in range(L):  # 2D array of character counts
            S[i][j] = S[i - 1][j] + (j == ord(v) - z)
    for i in range(1, n):
        F[i] = F[i - 1] * i % M  # modular factorial
        I[i] = pow(F[i], M - 2, M)  # modular inverse of factorial
    return S, F, I


def answerQuery(l, r):
    c, s, d = 0, 0, 1
    for j in [S[r][i] - S[l - 1][i] for i in range(L)]:
        c += j % 2  # count of center characters
        s += j // 2  # count of side characters
        d *= I[j // 2]  # "denominators"
    return (c or 1) * F[s] * d % M


if __name__ == '__main__':

    L, M = 26, 1000000007
    S, F, I = initialize(input())
    for _ in range(int(input())):
        result = answerQuery(*map(int, input().split()))
        print(result)
