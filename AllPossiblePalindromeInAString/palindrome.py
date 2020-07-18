s = input()

from math import factorial

mod = 10 ** 9 + 7

F = [1]
for i in range(1, 10 ** 5 + 2):
    F.append(F[-1] * i % mod)

A = {}
for c in 'abcdefghijklmnopqrstuvwxyz':
    A[c] = [0]

for c in s:
    for a in A:
        A[a].append(A[a][-1])
    A[c][-1] += 1

q = int(input())
for a0 in range(q):
    l, r = map(int, input().split())

    even = []
    numodd = 0
    for a in A:
        count = A[a][r] - A[a][l - 1]
        if count <= 0: continue
        if count % 2:
            numodd += 1
            even.append(count - 1)
        else:
            even.append(count)
    # print(even,maxodd,maxoddcount)

    # if maxodd:
    #    even.remove(maxodd-1)

    # print(numodd,even)

    S = 0
    div = 1
    for e in even:
        e //= 2
        S += e
        div = (div * F[e]) % mod

    # print(S,div)
    # print(F[S],pow(div,mod-2,mod))
    res = (F[S] % mod) * pow(div, mod - 2, mod)
    if numodd: res *= numodd
    print(res % mod)
