def countingSort(arr, max_item):
    counts = [0] * (max_item + 1)
    for item in arr:
        counts[item] += 1

    for i in range(1, max_item + 1):
        counts[i] += counts[i - 1]

    sortedList = [0] * len(arr)

    i = 0
    while i < len(arr):  # for sorting in decreasing order
        sortedList[len(arr) - counts[arr[i]]] = arr[i]
        counts[arr[i]] -= 1
        i += 1
    # i = len(arr) - 1
    # while i >= 0:  # for sorting in increasing order
    #     sortedList[counts[arr[i]] - 1] = arr[i]
    #     counts[arr[i]] -= 1
    #     i -= 1

    return sortedList


arr = [6, 2, 7, 5, 0, 3, 0, 0, 0]
arr = countingSort(arr, max(arr))
print("Sorted array in decreasing order is:", arr)
