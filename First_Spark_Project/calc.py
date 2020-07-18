# Define a function
# def world():
#     print("Hello, World!")
#
#
# # Define a variable
# shark = "Sammy"
#
#
# # Define a class
# class Octopus:
#     def __init__(self, name, color):
#         self.color = color
#         self.name = name
#
#     def tell_me_about_the_octopus(self):
#         print("This octopus is " + self.color + ".")
#         print(self.name + " is the octopus's name.")


for _ in range(int(input())):
    w = input().strip()
    n = len(w) + 1
    for i in range(-2, -n, -1):
        if w[i] < w[i + 1]:
            print(w[:i], end='')
            v = w[:i:-1]
            for j in range(-i - 1):
                if w[i] < v[j]:
                    print(v[j] + v[:j] + w[i] + v[j + 1:])
                    break
            else:
                print(v + w[i])
            break
    else:
        print('no answer')
