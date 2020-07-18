class Node(object):
    def __init__(self, data=None):
        self.data = data
        self.next = None
        self.previous = None


class Stack:

    def __init__(self):
        self.length = 0
        self.head = None
        self.tail = None

    def push(self, item):
        newNode = Node(item)
        if self.tail is None:
            self.head = self.tail = newNode
        else:
            self.tail.next = newNode
            newNode.previous = self.tail
            self.tail = newNode
        self.length += 1

    def pop(self):
        if self.is_empty():
            return "Stack is empty!!!!"
        data = self.tail.data
        self.tail = self.tail.previous
        self.length -= 1
        if self.length == 0:
            self.head = None
        return data

    def top(self):
        if self.tail is not None:
            return self.tail.data
        return self.tail

    def length_stack(self):
        return self.length

    def is_empty(self):
        if self.head is None:
            return True
        return False


stack = Stack()
stack.push(1)  # push item 1
stack.push(2)  # push item 2
print('Pop the last item :', stack.pop())  # pop the top item
print('Current top item is :', stack.top())  # current top item
print('Pop the last item :', stack.pop())  # pop the top item
print('Pop the last item :', stack.pop())  # pop the top item
