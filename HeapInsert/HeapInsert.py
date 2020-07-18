def heapify(arr, n, i):
    parent = (i - 1) // 2

    if parent >= 0:
        if arr[parent] < arr[i]:
            arr[i], arr[parent] = arr[parent], arr[i]
            heapify(arr, n, parent)


def insertNode(arr, key):
    arr.append(key)
    n = len(arr)
    heapify(arr, n, n - 1)
    return n


arr = [10, 5, 3, 2, 4]
n = len(arr)
for i in range(n):
    print(arr[i])
n = insertNode(arr, 15)
print("After Insertion!!!!")
for i in range(n):
    print(arr[i])
