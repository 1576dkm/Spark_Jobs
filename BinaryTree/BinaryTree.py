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

    def enQueue(self, x):
        newNode = Node(x)
        if self.head is None:
            self.head = self.tail = newNode
        else:
            self.tail.next = newNode
            # newNode.previous = self.tail
            self.tail = newNode
        self.length += 1

    def deQueue(self):
        if self.isEmpty():
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

    def lengthOfQueue(self):
        return self.length

    def isEmpty(self):
        if self.head is None:
            return True
        return False

    def close(self):
        self.head = None
        self.tail = None


class TreeNode:
    def __init__(self, data=None):
        self.data = data
        self.right = None
        self.left = None


class BinaryTree:
    def __init__(self):
        self.root = None

    def insertNode(self, data):
        # Create a new node
        newNode = TreeNode(data)
        if self.root == None:
            self.root = newNode
        else:
            q = Queue()
            q.enQueue(self.root)
            node = self.root
            while not q.isEmpty():
                node = q.deQueue()
                if node.left != None:
                    q.enQueue(node.left)
                else:
                    q.close()
                if node.right != None:
                    q.enQueue(node.right)
                else:
                    q.close()
            if node.left == None:
                node.left = newNode
            elif node.right == None:
                node.right = newNode

    def preOrder(self, node):
        if node != None:
            print(str(node.data) + " ")
            self.preOrder(node.left)
            self.preOrder(node.right)
        elif self.root == None:
            print("Tree is Empty!!!!")

    def inOrder(self, node):
        if node != None:
            self.preOrder(node.left)
            print(str(node.data) + " ")
            self.preOrder(node.right)
        elif self.root == None:
            print("Tree is Empty!!!!")

    def postOrder(self, node):
        if node != None:
            self.postOrder(node.left)
            self.postOrder(node.right)
            print(str(node.data) + " ")
        elif self.root == None:
            print("Tree is Empty!!!!")

    def levelOrder(self, node):
        tempNode = node
        q = Queue()
        q.enQueue(tempNode)
        while not q.isEmpty():
            tempNode = q.deQueue()
            print(tempNode.data)
            if tempNode.left != None:
                q.enQueue(tempNode.left)
            if tempNode.right != None:
                q.enQueue(tempNode.right)
        q.close()

    def display(self):
        print("PreOrder Traversal....")
        self.preOrder(self.root)
        print("InOrder Traversal....")
        self.inOrder(self.root)
        print("PostOrder Traversal....")
        self.postOrder(self.root)
        print("LevelOrder Traversal....")
        self.levelOrder(self.root)


bt = BinaryTree()

bt.insertNode(1)
bt.insertNode(2)
bt.insertNode(3)
bt.insertNode(4)
bt.insertNode(5)
bt.insertNode(6)
bt.insertNode(7)
bt.insertNode(8)
bt.insertNode(9)
bt.insertNode(10)
bt.insertNode(11)
bt.display()
