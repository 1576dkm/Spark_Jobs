class MClass:
    def myPublicMethod(self):
        print("This is a public method!!!!!")

    def _myPrivateMethod(self):
        print("This is a private method!!!!")


obj = MClass()
obj.myPublicMethod()
# print(dir(obj))
obj._myPrivateMethod()


# Complete the surfaceArea function below.
# def surfaceArea(A):
#     surf = 0
#     for i in range(len(A)):  # for list in list.
#         for j in range(len(A[i])):  # for cell in list.
#             c = A[i][j]  # current cell val
#             s = (c * 4) + 2  # surface
#
#             if i > 0:  # left side limit.
#                 s -= min(c, A[i - 1][j]) * 2
#             if j > 0:  # upper limit.
#                 s -= min(c, A[i][j - 1]) * 2
#
#             surf += s
#
#     return surf
#
#
# HW = input().split()
# H = int(HW[0])
# W = int(HW[1])
# A = []
# for _ in range(H):
#     A.append(list(map(int, input().rstrip().split())))
# print(A)
# result = surfaceArea(A)
# print(result)

# !/bin/python3


# Complete the twoPluses function below.
def twoPluses(grid):
    ny = len(grid)
    nx = len(grid[0])
    d = []
    for i in range(1, ny - 1):
        for j in range(1, nx - 1):
            if grid[i][j] == 'G':
                mx = 0
                while True:
                    test = mx + 1
                    if i - test < 0 or grid[i - test][j] != 'G': break
                    if i + test >= ny or grid[i + test][j] != 'G': break
                    if j - test < 0 or grid[i][j - test] != 'G': break
                    if j + test >= nx or grid[i][j + test] != 'G': break
                    mx = test
                if mx != 0: d.append((i, j, mx))
    mx = 0
    for i, ii in enumerate(d):
        yi, xi, vi = ii
        for j, jj in enumerate(d[i + 1:]):
            yj, xj, vj = jj
            mi, mj = vi, vj
            while (mi * 4 + 1) * (mj * 4 + 1) > mx:
                dy = abs(yi - yj)
                dx = abs(xi - xj)
                hit = False
                if xi == xj and dy - 1 < mi + mj:
                    hit = True
                if xi != xj and dx <= max(mi, mj) and dy <= min(mi, mj):
                    hit = True
                if yi == yj and dx - 1 < mi + mj:
                    hit = True
                if yi != yj and dy <= max(mi, mj) and dx <= min(mi, mj):
                    hit = True
                if not hit: break
                if mi == 0: break
                if mi < mj:
                    mj -= 1
                else:
                    mi -= 1
            res = (mi * 4 + 1) * (mj * 4 + 1)
            if res > mx: mx = res
    if mx == 0:
        if len(d) == 0: return 1
        return max(x[2] for x in d) * 4 + 1
    return mx


if __name__ == '__main__':

    nm = input().split()

    n = int(nm[0])

    m = int(nm[1])

    grid = []

    for _ in range(n):
        grid_item = input()
        grid.append(grid_item)

    print(grid)
    result = twoPluses(grid)

    print(result)
