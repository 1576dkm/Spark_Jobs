import copy
from collections import OrderedDict


class QueuePack:
    def __init__(self, level, tnode):
        self.level = level
        self.tnode = tnode


class Node(object):
    def __init__(self, data=None):
        self.data = data
        self.next = None
        self.previous = None


class Stack(object):

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
        if self.isEmpty():
            return "Stack is empty!!!!"
        data = self.tail.data
        self.tail = self.tail.previous
        self.length -= 1
        if self.length == 0:
            self.head = None
        return data

    def peek(self):
        if self.tail is not None:
            return self.tail.data
        return self.tail

    def length_stack(self):
        return self.length

    def isEmpty(self):
        if self.head is None:
            return True
        return False


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

    def size(self):
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
        self.left = None
        self.right = None
        self.data = data
        self.level = None  # level none defined


class BinarySearchTree(object):
    def __init__(self, root=None):
        self.root = root

    def insert(self, data):
        newNode = TreeNode(data)
        if self.root is None:
            self.root = newNode
            return

        else:
            temp = self.root
            ptemp = self.root
            while temp is not None:
                ptemp = temp
                if data <= temp.data:
                    temp = temp.left
                elif data > temp.data:
                    temp = temp.right
            if ptemp.data < data:
                ptemp.right = newNode
            elif ptemp.data >= data:
                ptemp.left = newNode

    def searchNode(self, num):
        curr = self.root
        parent = None
        while curr is not None:
            if curr.data == num:
                return parent
            parent = curr
            if num < curr.data:
                curr = curr.left
            elif num > curr.data:
                curr = curr.right
        return None

    def predecessor(self, tnode):
        node = tnode
        parent = node
        if node.left != None:
            parent = node
            node = node.left
            if node.right != None:
                while node.right != None:
                    parent = node
                    node = node.right
                if node.left != None:
                    parent.right = node.left
                else:
                    parent.right = None
            elif node.left != None and parent.left.data == node.data:
                parent.left = node.left
            elif node.left != None and parent.left.data != node.data:
                parent.right = node.left
            else:
                parent.left = None
            return node
        return None

    def successor(self, tempnode):
        node = tempnode
        parent = node
        if node.right != None:
            parent = node
            node = node.right
            if node.left != None:
                while node.left != None:
                    parent = node
                    node = node.left
                if node.right != None:
                    parent.left = node.right
                else:
                    parent.left = None
            elif node.right != None and parent.right.data == node.data:
                parent.right = node.right
            elif node.right != None and parent.right.data != node.data:
                parent.left = node.right
            else:
                parent.right = None
            return node
        return None

    def delete(self, num):
        parent = self.searchNode(num)
        if parent != None:
            if parent.left != None and parent.left.data == num:
                succ = self.successor(parent.left)
                if None == succ:
                    predec = self.predecessor(parent.left)
                    if None == predec:
                        parent.left = None
                    else:
                        parent.left.data = predec.data
                        predec = None
                else:
                    parent.left.data = succ.data
                    succ = None
            elif parent.right != None and parent.right.data == num:
                succ = self.successor(parent.right)
                if (None == succ):
                    predec = self.predecessor(parent.right)
                    if None == predec:
                        parent.right = None
                    else:
                        parent.right.data = predec.data
                        predec = None
                else:
                    parent.right.data = succ.data
                    succ = None
            else:
                print(str(num) + " not found!!!!!!")
        elif parent == None and self.root.data == num:
            succ = self.successor(self.root)
            if (None == succ):
                predec = self.predecessor(self.root)
                if None == predec:
                    root = None
                else:
                    self.root.data = predec.data
                    predec = None
            else:
                self.root.data = succ.data
                succ = None
        else:
            print(str(num) + " not found!!!!!!")

    def isLeaf(self, tnode):
        if tnode.right == None and tnode.left == None:
            return True
        else:
            return False

    def height(self, tnode):
        q = Queue()
        h = 0
        if tnode != None:
            q.enQueue(tnode)
            q.enQueue(None)  # use it as a level marker.
        while not q.isEmpty():
            n = q.deQueue()
            if n == None:
                if not q.isEmpty():
                    q.enQueue(None)  # use it as a level marker.
                h = h + 1
            else:
                if n.left != None:
                    q.enQueue(n.left)
                    if n.right != None:
                        q.enQueue(n.right)
        return h

    def preOrder(self, tnode):
        tpnode = tnode
        if tpnode != None:
            stc = Stack()
            while (tpnode != None) or (not stc.isEmpty()):
                while tpnode != None:
                    print(str(tpnode.data) + " ")
                    stc.push(tpnode)
                    tpnode = tpnode.left
                if not stc.isEmpty():
                    tpnode = stc.pop()
                    tpnode = tpnode.right

    def inOrder(self, tnode):
        tpnode = tnode
        if tpnode != None:
            stc = Stack()
            while (tpnode != None) or (not stc.isEmpty()):
                while tpnode != None:
                    stc.push(tpnode)
                    tpnode = tpnode.left
                if not stc.isEmpty():
                    tpnode = stc.pop()
                    print(str(tpnode.data) + " ")
                    tpnode = tpnode.right

    def postOrder(self, tnode):
        tcnode = copy.deepcopy(tnode)
        stc = Stack()
        if tcnode != None:
            stc.push(tcnode)
        else:
            print("Root is None!!!!")
        while not stc.isEmpty():
            curr = stc.peek()
            if self.isLeaf(curr):
                tpnode = stc.pop()
                print(str(tpnode.data) + " ")
            else:
                if curr.right != None:
                    stc.push(curr.right)
                    curr.right = None
                if curr.left != None:
                    stc.push(curr.left)
                    curr.left = None

    def postOrderWithoutDeletingTheTree(self, tnode):
        curr = tnode
        stc = Stack()
        while curr != None or stc.isEmpty() != None:
            if curr != None:
                stc.push(curr)
                curr = curr.left
            else:
                temp = stc.peek().right
                if temp == None:
                    temp = stc.pop()
                    print(str(temp.data) + " ")
                    while stc.isEmpty() != None and temp == stc.peek().right:
                        temp = stc.pop()
                        print(str(temp.data) + " ")

                else:
                    curr = temp

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

    def deepestNode(self, tnode):
        temp = None
        q = Queue()
        if tnode == None:
            return None
        else:
            q.enQueue(tnode)
            while not q.isEmpty():
                temp = q.deQueue()
                if temp.left != None:
                    q.enQueue(temp.left)
                if temp.right != None:
                    q.enQueue(temp.right)
            q.close()
            return temp

    def itrDiameter(self, tpnode):
        tnode = tpnode
        stc1 = Stack()
        stc2 = Stack()
        df = 0
        di = 0
        temp = None
        if tnode != None:
            stc1.push(tnode)
        while not stc1.isEmpty():
            tnode = stc1.pop()
            if tnode.left != None:
                stc1.push(tnode.left)
            if tnode.right != None:
                stc1.push(tnode.right)
            stc2.push(tnode)
        while not stc2.isEmpty():
            temp = stc2.pop()
            lh = self.height(temp.left)
            rh = self.height(temp.right)
            di = lh + rh + 1
            if df < di:
                df = di
        return df

    def rightView(self, tnode):
        if tnode == None:
            return None
        queue = Queue()
        queue.enQueue(tnode)
        curr = None
        while not queue.isEmpty():
            size = queue.size()
            i = 0
            while i < size:
                curr = queue.deQueue()
                i += 1
                if size == i:
                    print(curr.data)
                if curr.left != None:
                    queue.enQueue(curr.left)
                if curr.right != None:
                    queue.enQueue(curr.right)

    def leftView(self, tnode):
        if tnode == None:
            return None
        queue = Queue()
        queue.enQueue(tnode)
        curr = None
        while not queue.isEmpty():
            size = queue.size()
            i = 0
            while i < size:
                curr = queue.deQueue()
                i += 1
                if i == 1:
                    print(curr.data)
                if curr.left != None:
                    queue.enQueue(curr.left)
                if curr.right != None:
                    queue.enQueue(curr.right)

    def topView(self, tpnode, level):
        if tpnode == None:
            return None
        ht = {}
        queue = Queue()
        queue.enQueue(QueuePack(level, tpnode))
        while not queue.isEmpty():
            q = queue.deQueue()
            tnode = q.tnode
            lvl = q.level
            try:
                if ht[lvl]:
                    if tnode.left != None:
                        queue.enQueue(QueuePack(lvl - 1, tnode.left))
                    if tnode.right != None:
                        queue.enQueue(QueuePack(lvl + 1, tnode.right))
            except:
                ht[lvl] = tnode.data

                if tnode.left != None:
                    queue.enQueue(QueuePack(lvl - 1, tnode.left))
                if tnode.right != None:
                    queue.enQueue(QueuePack(lvl + 1, tnode.right))
        first = min(ht.keys())
        last = max(ht.keys())
        while first <= last:
            print(ht[first])
            first += 1

    def bottomView(self, tpnode, level):
        if tpnode == None:
            return None
        ht = {}
        queue = Queue()
        queue.enQueue(QueuePack(level, tpnode))
        while not queue.isEmpty():
            q = queue.deQueue()
            tnode = q.tnode
            lvl = q.level
            ht[lvl] = tnode.data
            if tnode.left != None:
                queue.enQueue(QueuePack(lvl - 1, tnode.left))
            if tnode.right != None:
                queue.enQueue(QueuePack(lvl + 1, tnode.right))
        first = min(ht.keys())
        last = max(ht.keys())
        while first <= last:
            print(ht[first])
            first += 1

    # def rightView(self, root):
    #
    #     if not root:
    #         return
    #
    #     q = [root]
    #
    #     while len(q):
    #
    #         # number of nodes at current level
    #         n = len(q)
    #
    #         # Traverse all nodes of current level
    #         for i in range(1, n + 1):
    #             temp = q[0]
    #             q.pop(0)
    #
    #             # Print the right most element
    #             # at the level
    #             if i == n:
    #                 print(temp.flower-classification-with-tpus, end=" ")
    #
    #                 # Add left node to queue
    #             if temp.left != None:
    #                 q.append(temp.left)
    #
    #                 # Add right node to queue
    #             if temp.right != None:
    #                 q.append(temp.right)

    def display(self):
        print("PreOrder Traversal-->> ")
        self.preOrder(self.root)

        print()
        print("InOrder Traversal-->> ")
        self.inOrder(self.root)

        print()
        print("PostOrder Traversal-->> ")
        self.postOrder(self.root)

        print()
        print("PostOrderWithoutDeletingTree Traversal-->> ")
        self.postOrderWithoutDeletingTheTree(self.root)

        # root = troot
        # troot = null

        print()
        print("LavelOrder Traversal-->> ")
        self.levelOrder(self.root)

        print()
        print("height of the tree!!!!")
        print(self.height(self.root))

        print()
        print("Deepest Node is: ")
        var = self.deepestNode(self.root).data
        print(var)

        print()
        self.searchNode(20)

        # println()
        # delete(20)

        print()
        print("Diameter of binary tree---> ")
        print(self.itrDiameter(self.root))
        print()

        print()
        print("Right View of the Tree!!!!")
        self.rightView(self.root)

        print()
        print("Left View of the Tree!!!!")
        self.leftView(self.root)

        print()
        print("Top View of the Tree!!!!")
        self.topView(self.root, 0)

        print()
        print("Bottom View of the Tree!!!!")
        self.bottomView(self.root, 0)

    # def bft(self):  # Breadth-First Traversal
    #
    #     self.root.level = 0
    #     queue = [self.root]
    #     out = []
    #     current_level = self.root.level
    #
    #     while len(queue) > 0:
    #
    #         current_node = queue.pop(0)
    #
    #         if current_node.level > current_level:
    #             current_level += 1
    #             out.append("\n")
    #
    #         out.append(str(current_node.info) + " ")
    #
    #         if current_node.left:
    #             current_node.left.level = current_level + 1
    #             queue.append(current_node.left)
    #
    #         if current_node.right:
    #             current_node.right.level = current_level + 1
    #             queue.append(current_node.right)
    #
    #     print("".join(out))


bst = BinarySearchTree()
bst.insert(40)
bst.insert(20)
bst.insert(50)
bst.insert(10)
bst.insert(30)
bst.insert(30)
bst.insert(30)
bst.insert(60)
bst.insert(25)
bst.insert(22)
bst.insert(35)
bst.insert(27)
bst.insert(23)
bst.insert(45)
bst.insert(41)
bst.insert(49)
bst.insert(48)
bst.insert(47)
bst.display()
bst.delete(20)
bst.display()
