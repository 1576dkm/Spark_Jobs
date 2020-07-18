# # Import calc1 module
# import sys
#
# sys.path.append('D:\\Diw_Space')
# from My_Modules.Modules import calc1 as c
#
# # Call function
# c.world()
#
# # Print variable
# print(c.shark)
#
# # Call class
# jess = c.Octopus("Jesse", "orange")
# jess.tell_me_about_the_octopus()


# rows, cols, rotations = [int(x) for x in input().strip().split(" ")]
# num_layers = int(min(rows, cols) / 2)
# layers = [[] for x in range(num_layers)]
# print(layers)
# flower-classification-with-tpus = []
# for row in range(rows):
#     flower-classification-with-tpus += [[int(x) for x in input().strip().split(" ")]]
# print(flower-classification-with-tpus)
# rows -= 1
# cols -= 1
#
# for current_layer in range(num_layers):
#     i = j = current_layer
#     while i < rows - current_layer:
#         layers[current_layer].append(flower-classification-with-tpus[i][j])
#         i += 1
#     while j < cols - current_layer:
#         layers[current_layer].append(flower-classification-with-tpus[i][j])
#         j += 1
#     while i > current_layer:
#         layers[current_layer].append(flower-classification-with-tpus[i][j])
#         i -= 1
#     while j > current_layer:
#         layers[current_layer].append(flower-classification-with-tpus[i][j])
#         j -= 1
# print(layers)
# new_layers = []
# for layer in layers:
#     rots = rotations % len(layer)
#     new_layers += [(list(layer[-rots:] + layer[:-rots]))]
# print(new_layers)
#
# for current_layer in range(num_layers):
#     i = j = current_layer
#     while i < rows - current_layer:
#         flower-classification-with-tpus[i][j] = new_layers[current_layer].pop(0)
#         i += 1
#     while j < cols - current_layer:
#         flower-classification-with-tpus[i][j] = new_layers[current_layer].pop(0)
#         j += 1
#     while i > current_layer:
#         flower-classification-with-tpus[i][j] = new_layers[current_layer].pop(0)
#         i -= 1
#     while j > current_layer:
#         flower-classification-with-tpus[i][j] = new_layers[current_layer].pop(0)
#         j -= 1
# print(flower-classification-with-tpus)
# for row in range(rows + 1):
#     for col in range(cols + 1):
#         print(flower-classification-with-tpus[row][col], end=" ")
#     print()


# n, k = map(int, input().rstrip().split())
# s = list(map(int, input().rstrip().split()))
# r = [0] * k
#
# for item in s:
#     r[item % k] += 1
#
# i, j, st = 1, k - 1, 0
# if r[0] > 0:
#     st += 1
# print(r)
# while i < j:
#     st += max(r[i], r[j])
#     print(st)
#     i, j = i + 1, j - 1
# if i == j and r[i] > 0:
#     st += 1
# print(st)


# Enter your code here. Read input from STDIN. Print output to STDOUT
# n = int(input())
# l = list(map(int, input().split()))
# sl = sorted(l)
#
# diffcount = 0
# diff1 = -1
# diff2 = -1
#
# for i in range(n):
#     if sl[i] != l[i]:
#         diffcount += 1
#         if diff1 == -1:
#             diff1 = i
#         elif diffcount > 1:
#             diff2 = i
#
# if diffcount == 2:
#     l[diff1], l[diff2] = l[diff2], l[diff1]
#     if l == sl:
#         print("yes")
#         print(f"swap {diff1 + 1} {diff2 + 1}")
#     else:
#         print("no")
# elif diffcount > 2:
#     l = l[:diff1] + l[diff1:diff2 + 1][::-1] + l[diff2 + 1:]
#     if l == sl:
#         print("yes")
#         print(f"reverse {diff1 + 1} {diff2 + 1}")
#     else:
#         print("no")
# elif l == sl:
#     print("yes")

h = int(input().strip())
m = int(input().strip())

d = dict(enumerate(
    ['one', 'two', "three", "four", 'five', "six", "seven", "eight", "nine", "ten", "eleven", "twelve", 'thirteen',
     'fourteen'], 1))

if m == 0:
    print(f"{d[h]} o' clock")
elif m == 1:
    print(f"one minute past {d[h]}")
elif 1 < m < 15:
    print(f'{d[m]} minutes past {d[h]}')
elif m == 15:
    print(f'quarter past {d[h]}')
elif 15 < m < 20:
    print(f'{d[m - 10]}teen minutes past {d[h]}')
elif m == 20:
    print(f'twenty minutes past {d[h]}')
elif 20 < m < 30:
    print(f'twenty {d[m - 20]} minutes past {d[h]}')
elif m == 30:
    print(f'half past {d[h]}')
elif 30 < m < 40:
    print(f'twenty {d[40 - m]} minutes to {d[h + 1]}')
elif m == 40:
    print(f'twenty minutes to {d[h + 1]}')
elif 40 < m < 45:
    print(f'{d[60 - m]} minutes to {d[h + 1]}')
elif m == 45:
    print(f'quarter to {d[h + 1]}')
elif 45 < m < 59:
    print(f'{d[60 - m]} minutes to {d[h + 1]}')
elif m == 59:
    print(f"one minute to {d[h + 1]}")
