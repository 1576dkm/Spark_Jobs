from typing import List


def merge(left, right):
    result = []
    i, j = 0, 0
    while i < len(left) and j < len(right):
        if left[i] <= right[j]:
            result.append(left[i])
            i += 1
        else:
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


arr = [1, 2, -1, 0, 9, 65, 7, 3, 4, 1, 2]
print(merge_Sort(arr))


def find_grid(R, C, G, r, c, P):
    for i in range(R - r + 1):
        for j in range(C - c + 1):
            found = True
            for k in range(r):
                if P[k] != G[i + k][j:j + c]:
                    found = False
                    break
            if found:
                return "YES"
    return "NO"


T = int(input())
for _ in range(T):
    R, C = (int(x) for x in input().strip().split())
    G: List[str] = [input().strip() for i in range(R)]
    print(G)
    r, c = (int(x) for x in input().strip().split())
    P = [input().strip() for i in range(r)]
    print(P)

    print(find_grid(R, C, G, r, c, P))
