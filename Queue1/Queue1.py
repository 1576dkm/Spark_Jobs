class Node(object):
    def __init__(self, data=None):
        self.data = data
        self.next = None
        # self.previous = None


class Queue(object):
    def __init__(self):
        self.length = 0
        self.head = None
        self.tail = None

    def enqueue(self, x):
        newNode = Node(x)
        if self.head is None:
            self.head = self.tail = newNode
        else:
            self.tail.next = newNode
            # newNode.previous = self.tail
            self.tail = newNode
        self.length += 1

    def dequeue(self):
        if self.is_empty():
            return "Queue1 is empty!!!!"
        data = self.head.data
        self.head = self.head.next
        self.length -= 1
        if self.length == 0:
            self.tail = None
        return data

    def top(self):
        if self.tail is not None:
            return self.tail.data
        return self.tail

    def length_queue(self):
        return self.length

    def is_empty(self):
        if self.head is None:
            return True
        return False


obj = Queue()
obj.enqueue(3)
obj.enqueue(9)
obj.enqueue(1)
print('Current top item is :', obj.top())
print("length of the queue is: ", obj.length_queue())
print(obj.dequeue())
print("length of the queue is: ", obj.length_queue())
print(obj.dequeue())
print("length of the queue is: ", obj.length_queue())
print(obj.dequeue())
print("length of the queue is: ", obj.length_queue())
print(obj.dequeue())
print('Current top item is :', obj.top())
