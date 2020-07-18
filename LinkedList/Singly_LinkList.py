class Node:
    def __init__(self, item):
        self.ref = None
        self.item = item

    def __str__(self):
        if self.ref == None:
            return f"{self.item}"
        else:
            return f"{self.item} => {self.ref.__str__()}"


class LinkedList:
    def __init__(self):
        self.start_node = None

    def traverse_list(self):
        if self.start_node is None:
            print("List has no element")
            return
        else:
            n = self.start_node
            while n is not None:
                print(n.item, " ")
                n = n.ref

    def insert_at_start(self, data):
        new_node = Node(data)
        new_node.ref = self.start_node
        self.start_node = new_node

    def insert_at_end(self, data):
        new_node = Node(data)
        if self.start_node is None:
            self.start_node = new_node
            return
        n = self.start_node
        while n.ref is not None:
            n = n.ref
        n.ref = new_node

    def insert_after_item(self, x, data):

        n = self.start_node
        print(n.ref)
        while n is not None:
            if n.item == x:
                break
            n = n.ref
        if n is None:
            print("item not in the list")
        else:
            new_node = Node(data)
            new_node.ref = n.ref
            n.ref = new_node

    def insert_before_item(self, x, data):
        if self.start_node is None:
            print("List has no element")
            return

        if x == self.start_node.item:
            new_node = Node(data)
            new_node.ref = self.start_node
            self.start_node = new_node
            return

        n = self.start_node
        print(n.ref)
        while n.ref is not None:
            if n.ref.item == x:
                break
            n = n.ref
        if n.ref is None:
            print("item not in the list")
        else:
            new_node = Node(data)
            new_node.ref = n.ref
            n.ref = new_node

    def insert_at_position(self, pos, data):
        if pos == 1:
            new_node = Node(data)
            new_node.ref = self.start_node
            self.start_node = new_node
        else:
            i = 1
            n = self.start_node
            while i < pos - 1 and n is not None:
                n = n.ref
                i = i + 1
            if n is None:
                print("Index out of bound")
            else:
                new_node = Node(data)
                new_node.ref = n.ref
                n.ref = new_node

    def get_count(self):
        if self.start_node is None:
            return 0
        n = self.start_node
        count = 0
        while n is not None:
            count = count + 1
            n = n.ref
        return count

    def search_item(self, x):
        if self.start_node is None:
            print("List has no elements")
            return
        n = self.start_node
        while n is not None:
            if n.item == x:
                print("Item found")
                return True
            n = n.ref
        print("item not found")
        return False

    def make_new_list(self):
        nums = int(input("How many nodes do you want to create: "))
        if nums == 0:
            return
        for i in range(nums):
            value = int(input("Enter the value for the node:"))
            self.insert_at_end(value)

    def delete_at_start(self):
        if self.start_node is None:
            print("The list has no element to delete")
            return
        self.start_node = self.start_node.ref

    def delete_at_end(self):
        if self.start_node is None:
            print("The list has no element to delete")
            return

        n = self.start_node
        while n.ref.ref is not None:
            n = n.ref
        n.ref = None

    def delete_element_by_value(self, x):
        if self.start_node is None:
            print("The list has no element to delete")
            return

        # Deleting first node
        if self.start_node.item == x:
            self.start_node = self.start_node.ref
            return

        n = self.start_node
        while n.ref is not None:
            if n.ref.item == x:
                break
            n = n.ref

        if n.ref is None:
            print("item not found in the list")
        else:
            n.ref = n.ref.ref

    def delete_at_position(self, pos):
        if self.start_node is None:
            print("The list has no element to delete")
            return
        elif pos == 1:
            self.start_node = self.start_node.ref
        elif pos < 1:
            print("Position is not valid!!!")
        else:
            p = 1
            n = self.start_node
            while p + 1 < pos and n.ref is not None:
                n = n.ref
                p += 1
            if n.ref is None and pos > p:
                print("Position is not valid!!!")
                return
            else:
                n.ref = n.ref.ref

    def reverse_LinkedList(self):
        prev = None
        n = self.start_node
        while n is not None:
            next = n.ref
            n.ref = prev
            prev = n
            n = next
        self.start_node = prev

    def isLinkedListContainsLoop(self):
        slow, fast = self.start_node, self.start_node
        while slow != None and fast != None:
            fast = fast.ref
            if fast == slow:
                print("Contains Loop!!!!")
                return 1
            if fast == None:
                print("Doesn't Contains Loop!!!!")
                return 0
            fast = fast.ref
            if fast == slow:
                print("Contains Loop!!!!")
                return 1
            slow = slow.ref
        print("Doesn't Contains Loop!!!!")
        return 0

    def findStartElementOfLoop(self):
        slow, fast = self.start_node, self.start_node
        loopExists = 0
        while slow != None and fast != None:
            fast = fast.ref
            if fast == slow:
                print("Contains Loop!!!!")
                loopExists = 1
                break
            if fast == None:
                print("Doesn't Contains Loop!!!!")
                loopExists = 0
                break
            fast = fast.ref
            if fast == slow:
                print("Contains Loop!!!!")
                loopExists = 1
                break
            slow = slow.ref
        if loopExists:
            slow = self.start_node
            while slow != fast:
                fast = fast.ref
                slow = slow.ref
            return fast
        return None

    def lengthOfTheLoop(self):
        slow, fast = self.start_node, self.start_node
        loopExists = 0
        count = 0
        while slow != None and fast != None:
            fast = fast.ref
            if fast == slow:
                print("Contains Loop!!!!")
                loopExists = 1
                break
            if fast == None:
                print("Doesn't Contains Loop!!!!")
                loopExists = 0
                break
            fast = fast.ref
            if fast == slow:
                print("Contains Loop!!!!")
                loopExists = 1
                break
            slow = slow.ref
        if loopExists:
            while slow != fast:
                fast = fast.ref
                count += 1
            return count
        return 0

    def sortList(self):
        def mergeSort(start_node):
            # Defining function which will merge two sorted lists
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
            left = mergeSort(left)
            right = mergeSort(right)
            start_node = mergeLists(left, right)
            return start_node

        return mergeSort(self.start_node)

    def isLengthEven(self):
        n = self.start_node
        while n != None and n.ref != None:
            n = n.ref.ref
            if n == None:
                return 0  # length is even
        return 1  # Length is odd

    def mergeSortedLists(self, l1, l2):
        temp = None
        if l1 is None:
            return l2
        if l2 is None:
            return l1
        if l1.item <= l2.item:
            temp = l1
            temp.ref = self.mergeSortedLists(l1.ref, l2)
        else:
            temp = l2
            temp.ref = self.mergeSortedLists(l1, l2.ref)
        return temp


lst1 = LinkedList()
lst1.insert_at_end(25)
lst1.insert_at_end(102)
lst1.insert_at_end(15)
lst1.insert_at_end(5)
# lst1.traverse_list()
# lst1.insert_at_start(20)
# lst1.traverse_list()
# lst1.insert_after_item(10, 17)
# lst1.traverse_list()
# lst1.insert_before_item(17, 25)
# lst1.traverse_list()
# print("Start!!!")
# lst1.insert_at_position(1, 8)
# lst1.traverse_list()
# print("Done!!!")
# lst1.search_item(5)
# print(lst1.get_count())
# lst1.make_new_list()
# lst1.traverse_list()
# lst1.reverse_LinkedList()
# lst1.traverse_list()
lst1.insert_at_end(20)
lst1.insert_at_end(19)
lst1.traverse_list()
print("Start")
# lst1.delete_at_position(5)
lst1.isLinkedListContainsLoop()
t = lst1.sortList()
print(t)
lst1.start_node = t
lst1.traverse_list()
print("Done")
# lst1.reverse_LinkedList()
# lst1.traverse_list()
# print(lst1.isLengthEven())
lst2 = LinkedList()
lst2.insert_at_end(10)
lst2.insert_at_end(2)
lst2.insert_at_end(7)
lst2.insert_at_end(9)
lst2.insert_at_end(2)
lst2.insert_at_end(1)
print("lst2")
lst2.traverse_list()
t = lst2.sortList()
lst2.start_node = t
print("lst2 sorted")
lst2.traverse_list()
lst3 = LinkedList()
print("Merged list")
t = lst3.mergeSortedLists(lst2.start_node, lst1.start_node)
lst3.start_node = t
print(t)
lst3.traverse_list()
