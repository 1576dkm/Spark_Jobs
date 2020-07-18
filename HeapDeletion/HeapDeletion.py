def heapify(arr, n, i):
    largest = i
    l = 2 * i + 1  # left = 2*i + 1
    r = 2 * i + 2  # right = 2*i + 2

    # See if left child of root exists and is
    # greater than root
    if l < n and arr[i] < arr[l]:
        largest = l

        # See if right child of root exists and is
    # greater than root
    if r < n and arr[largest] < arr[r]:
        largest = r

        # Change root, if needed
    if largest != i:
        arr[i], arr[largest] = arr[largest], arr[i]  # swap

        # Heapify the root.
        heapify(arr, n, largest)


def deleteNode(arr, n):
    last = arr[n - 1]
    arr[0] = last
    n -= 1
    heapify(arr, n, 0)
    return n


arr = [10, 5, 3, 2, 4]
n = len(arr)
print("Before Deletion!!!!!")
for i in range(n):
    print(arr[i])
n = deleteNode(arr, n)
print("After Deletion!!!!!")
for i in range(n):
    print(arr[i])
