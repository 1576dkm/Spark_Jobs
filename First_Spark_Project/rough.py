# t = int(input())
# ans = []
#
# for _ in range(t):
#     n, k = map(int, input().split())
#     ans = [0] * n
#     for pos in range(1, n + 1):
#         if pos - k > 0 and ans[pos - k - 1] == 0:
#             ans[pos - k - 1] = pos
#         elif pos + k <= n and ans[pos + k - 1] == 0:
#             ans[pos + k - 1] = pos
#         else:
#             break
#     if 0 in ans:
#         print(-1)
#     else:
#         print(" ".join(map(str, ans)))


# for _ in range(int(input())):
#     n, k = list(map(int, input().split()))
#     found = False
#     arr = []
#     if k == 0:
#         print(*list(range(1, n + 1)))
#     elif (n / k) % 2 != 0:
#         print('-1')
#     else:
#         for i in range(1, n + 1):
#             if not found:
#                 arr.append(i + k)
#             else:
#                 arr.append(i - k)
#
#             if i % k == 0:
#                 if not found:
#                     found = True
#                 else:
#                     found = False
#         print(*arr, sep=' ')

# import re
#
# pat = r'[A-Z]'
# print(len(re.split(pat, input().strip())))
#
# n = int(input())
# password = input()
# count = 0
# cases = [r'[a-z]', r'[A-Z]', r'[\d]', r'[!@#$%^&*()\-+]']
# for case in cases:
#     if not re.search(case, password):
#         count += 1
#
# print(max(count, 6 - n))


# Enter your code here. Read input from STDIN. Print output to STDOUT

# def separate_Numbers(s):
#     for i in range(1, len(s) // 2 + 1):
#         candidate_string = ''
#         _init_num = s[0: i]
#         init_num = int(_init_num)
#         while len(candidate_string) < len(s):
#             candidate_string += str(init_num)
#             init_num += 1
#         if not (int(candidate_string) ^ int(s)):
#             print('YES ' + _init_num)
#             break
#     else:
#         print('NO')
#
#
# for _ in range(int(input().strip())):
#     separate_Numbers(input().strip())

# import sys
#
# input()
# k = list(input().split())
# v = list(map(int, input().strip().split()))
# cmax, cmin = 0, sys.maxsize
#
# for i in range(int(input().strip())):
#     sc = 0
#     seg = input().split()
#     s = int(seg[0])
#     e = int(seg[1])
#     g = [x for x in seg[2]]
#     # print(*g)
#     for j in range(s, e + 1):
#         for l in range(0, len(g)):
#             if k[j] == g[l]:
#                 sc += v[j]
#             elif len(k[j]) > len(g[l]):  # bcdyaa
#                 temp = ""
#                 x = 0
#                 while x < len(k[j]) and x + l < len(g):
#                     if k[j][x] == g[x + l]:
#                         temp += g[l + x]
#                         if k[j] == temp:
#                             sc += v[j]
#                     else:
#                         break
#                     x += 1
#
#     if cmax < sc:
#         cmax = sc
#     elif cmin > sc:
#         cmin = sc
#
# print(f"{cmin} {cmax}")


# import sys
#
# n = int(input())
# k = list(input().split())
# v = list(map(int, input().strip().split()))
# ri = []
# i = 0
# while i < n:
#     str1 = k[i]
#     temp = str1.replace(k[i][0], "")
#     if len(temp) == 0 and len(k[i]) > 1:
#         ri.append(i)
#     i += 1
# cmax, cmin = 0, sys.maxsize
# for t in range(int(input().strip())):
#     sc = 0
#     seg = input().split()
#     s = int(seg[0])
#     e = int(seg[1])
#     g = seg[2]
#
#     # print(*g)
#     for j in ri:
#         if s <= j <= e:
#             tcount = 0
#             str1 = g
#             try:
#                 i = str1.index(k[j][0])
#                 while k[j][0] == str1[i]:
#                     i += 1
#                     tcount += 1
#                     if k[j][0] != str1[i]:
#                         if tcount >= len(k[j]):
#                             sc += (tcount - len(k[j]) + 1) * v[j]
#                         tcount = 0
#                         str1 = str1[i:len(str1)]
#                         try:
#                             i = str1.index(k[j][0])
#                         except:
#                             break
#             except:
#                 pass
#
#     for j in range(s, e + 1):
#         temp = g
#
#         try:
#             ri.index(j)
#         except:
#             sc += ((len(g) - len(temp.replace(k[j], ""))) // len(k[j])) * v[j]
#
#     if cmax < sc:
#         cmax = sc
#     elif cmin > sc:
#         cmin = sc
#
# print(f"{cmin} {cmax}")

# def largest(s, n, k):
#     s = list(s)
#     palin = list(s)
#     l = 0
#     r = len(s) - 1
#     while (l < r):
#         if s[l] != s[r]:
#             palin[l] = palin[r] = max(s[l], s[r])
#             k -= 1
#         l += 1
#         r -= 1
#     if (k < 0):
#         return -1
#     l = 0
#     r = len(s) - 1
#     while l <= r:
#         if l == r:
#             if k > 0:
#                 palin[l] = '9'
#         if palin[l] < '9':
#             if k >= 2 and palin[l] == s[l] and palin[r] == s[r]:
#                 k -= 2
#                 palin[l] = palin[r] = '9'
#             elif k >= 1 and (palin[l] != s[l] or palin[r] != s[r]):
#                 k -= 1
#                 palin[l] = palin[r] = '9'
#         l += 1
#         r -= 1
#     return palin
#
#
# n, k = map(int, input().split())
# s = input()
# x = largest(s, n, k)
# if x == -1:
#     print("-1")
# else:
#     print("".join(x))


# def steadyGene(gene):
#     dic = {'A': 0, 'T': 0, 'C': 0, 'G': 0}
#     for i in gene:
#         dic[i] += 1
#     x = len(gene)
#     factor = x / 4
#
#     if dic['A'] == factor and dic['T'] == factor and dic['C'] == factor and dic['G'] == factor:
#         return 0
#
#     upper = 0
#     lower = 0
#     min_len = x
#     while upper < x and lower < x:
#         while (dic['A'] > factor or dic['C'] > factor or dic['T'] > factor or dic['G'] > factor) and upper < x:
#             dic[gene[upper]] -= 1
#             upper += 1
#         while dic['A'] <= factor and dic['C'] <= factor and dic['T'] <= factor and dic['G'] <= factor:
#             dic[gene[lower]] += 1
#             lower += 1
#         min_len = min(upper - lower + 1, min_len)
#     return min_len
#
#
# n = input()
# result = steadyGene(input())
# print(result)

# def common_Child(st_1, st_2):
#     m = len(st_1)
#     n = len(st_2)
#     len_Child = [[0] * (n + 1) for _ in range(m + 1)]
#     for i in range(m):
#         for j in range(n):
#             if st_1[i] == st_2[j]:
#                 len_Child[i + 1][j + 1] = len_Child[i][j] + 1
#             else:
#                 len_Child[i + 1][j + 1] = max(len_Child[i][j + 1], len_Child[i + 1][j])
#
#     return len_Child[m][n]
#
#
# st_1 = input()
# st_2 = input()
# print(common_Child(st_1, st_2))


# def valid_string(s):
#     d, v = {}, {}
#     for i in s:
#         d[i] = d.get(i, 0) + 1
#     for i in d.values():
#         v[i] = v.get(i, 0) + 1
#     n = len(v)
#     if n == 1:
#         return 'YES'
#     elif n == 2 and 1 in v.values():
#         K = list(v.keys())
#         if v.get(1, 0) == 1 or abs(K[0] - K[1]) == 1:
#             return 'YES'
#     return 'NO'
#
#
# print(valid_string(input()))

e = 2


def sub_string(s):
    sub = 0
    n = len(s)
    f = 0
    i = 0
    while i < n:
        if s[f:i + 1] in s[0:i]:
            sub += f
            i += 1
        elif i != f:
            f += 1
        else:
            f = i + 1
            sub += f
            i += 1
    return sub


n, q = map(int, input().strip().split())
s = input().strip()

for a0 in range(q):
    left, right = map(int, input().strip().split())
    print(sub_string(s[left:right + 1]))
