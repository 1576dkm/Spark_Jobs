def findLongestSubstring(string):
    n = len(string)

    # starting point of current substring.
    st = 0

    # maximum length substring without
    # repeating characters.
    maxlen = 0

    # starting index of maximum
    # length substring.
    start = 0

    # Hash Map to store last occurrence
    # of each already visited character.
    pos = {}

    # Last occurrence of first
    # character is index 0
    pos[string[0]] = 0

    for i in range(1, n):

        # If this character is not present in hash,
        # then this is first occurrence of this
        # character, store this in hash.
        if string[i] not in pos:
            pos[string[i]] = i

        else:
            # If this character is present in hash then
            # this character has previous occurrence,
            # check if that occurrence is before or after
            # starting point of current substring.
            if pos[string[i]] >= st:

                # find length of current substring and
                # update maxlen and start accordingly.
                currlen = i - st
                if maxlen < currlen:
                    maxlen = currlen
                    start = st

                    # Next substring will start after the last
                # occurrence of current character to avoid
                # its repetition.
                st = pos[string[i]] + 1

            # Update last occurrence of
            # current character.
            pos[string[i]] = i

            # Compare length of last substring with maxlen
    # and update maxlen and start accordingly.
    if maxlen < i - st:
        maxlen = i - st
        start = st

        # The required longest substring without
    # repeating characters is from string[start]
    # to string[start+maxlen-1].
    return string[start: start + maxlen]


# Driver Code
if __name__ == "__main__":
    string = "GEEKSFORGEEKS"
    print(findLongestSubstring(string))

    # This code is contributed by Rituraj Jain

# Python program to find maximum contiguous subarray

# Function to find the maximum contiguous subarray
from sys import maxsize


def maxSubArraySum(a):
    # find maximum element present in given list
    maximum = max(a)

    # if list contains all negative values, return maximum element
    if maximum < 0:
        return maximum

    # stores maximum sum sublist found so far
    maxSoFar = 0

    # stores maximum sum of sublist ending at current position
    maxEndingHere = 0

    # do for each element of the given list
    for i in a:
        # update maximum sum of sublist "ending" at index i (by adding
        # current element to maximum sum ending at previous index i-1)
        maxEndingHere = maxEndingHere + i

        # if maximum sum is negative, set it to 0 (which represents
        # an empty sublist)
        maxEndingHere = max(maxEndingHere, 0)

        # update result if current sublist sum is found to be greater
        maxSoFar = max(maxSoFar, maxEndingHere)

    return maxSoFar


# Driver function to check the above function
a = [-13, -3, -25, -20, -3, -16, 23, -12, 5, 22, 15, -4, 7]
print("Maximum contiguous sum is", maxSubArraySum(a))

# This code is contributed by _Devesh Agrawal_



mat = [[0, 0, 0, 1],
       [0, 1, 1, 1],
       [1, 1, 1, 1],
       [0, 0, 0, 0]]


def rowWithMax1s(mat):
    for i in range(len(mat)):
        mat[i]=sum(mat[i])
    return (mat.index(max(mat)))


print ("Index of row with maximum 1s is",
      rowWithMax1s(mat))

