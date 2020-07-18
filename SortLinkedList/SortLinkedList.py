class Node:
    def __init__(self, x):
        self.item = x
        self.ref = None


class LinkedList:
    def __init__(self):
        self.start_node = None

    def insert_at_end(self, item):
        new_node = Node(item)
        if self.start_node is None:
            self.start_node = new_node
            return
        n = self.start_node
        while n.ref is not None:
            n = n.ref
        n.ref = new_node

    # Defining function which will sort the linked list using mergeSort
    def mergeSort(self, start_node):
        # Defining function which will merge two lists
        def mergeLists(l1, l2):
            temp = None
            if l1 is None:
                return l2
            if l2 is None:
                return l1
            if l1.item <= l2.item:
                temp = l1
                temp.ref = mergeLists(l1.ref, l2)
            else:
                temp = l2
                temp.ref = mergeLists(l1, l2.ref)
            return temp

        # Defining function which will divide a linked list into two equal linked lists
        def splitLists(start_node):
            slow = start_node  # slow is a pointer to reach the mid of linked list
            fast = start_node  # fast is a pointer to reach the end of the linked list
            if fast:
                fast = fast.ref
            while fast:
                fast = fast.ref  # fast is incremented twice while slow is incremented once per loop
                if fast:
                    fast = fast.ref
                    slow = slow.ref
            mid = slow.ref
            slow.ref = None
            return start_node, mid

        if start_node is None or start_node.ref is None:
            return start_node
        left, right = splitLists(start_node)
        left = self.mergeSort(left)
        right = self.mergeSort(right)
        start_node = mergeLists(left, right)
        return start_node

    def traverse_list(self):
        if self.start_node is None:
            print("List has no element")
            return
        else:
            self.start_node = self.mergeSort(self.start_node)
            n = self.start_node
            while n is not None:
                print(n.item, " ")
                n = n.ref


lst = LinkedList()
lst.insert_at_end(10)
lst.insert_at_end(2)
lst.insert_at_end(7)
lst.insert_at_end(9)
lst.insert_at_end(2)
lst.insert_at_end(1)
lst.traverse_list()
print("Start")
# lst.start_node = lst.mergeSort(lst.start_node)
# lst.traverse_list()
