#### AVL tree implementation, it will house offset of that segment along with the segment id, so that its easier while doing a memflush.
from .bloom_filter import BloomFilter
from sys import getsizeof

class Node:
    def __init__(self, key, value):
        self.key = key
        self.value = value
        self.height = 1
        self.right = None
        self.left = None

class AVL:
    def __init__(self):
        self.root = None
        self.bloom_filter = BloomFilter()
        self.items = 0
        self.size = 0

    def get_height(self, node):
        if not node:
            return 0
        return node.height

    def get_bf(self, node):
        if node is None:
            return 0
        else:
            return self.get_height(node.left)-self.get_height(node.right)
        
    def search(self, key):
        if key in self.bloom_filter:
            node = self.root
            while node is not None:
                if key == node.key:
                    return node  
                elif key < node.key:
                    node = node.left 
                else:
                    node = node.right 
        
        return None  
    
    def left_rotate(self, node):
        A = node.right
        B = A.left

        A.left = node
        node.right = B

        node.height = 1 + max(self.get_height(node.left), self.get_height(node.right))
        A.height = 1 +  max(self.get_height(A.left), self.get_height(A.right))

        return A

    def right_rotate(self, node):
        A = node.left
        B = A.right

        A.right = node
        node.left = B

        node.height = 1 + max(self.get_height(node.left), self.get_height(node.right))
        A.height = 1 +  max(self.get_height(A.left), self.get_height(A.right))

        return A

    def insert(self, key, value):
        self.root = self._insert(self.root, key, value) 
        if self.bloom_filter: 
            self.bloom_filter.add(key)
        self.size += getsizeof(key)+getsizeof(value)

    def _insert(self, root, key, value): 

        if root is None:
            self.items += 1 
            return Node(key, value)
        elif key == root.key:
            root.value = value
            return root
        elif key < root.key:
            root.left = self._insert(root.left, key, value) 
        else:
            root.right = self._insert(root.right, key, value) 
        
        root.height = 1 + max(self.get_height(root.left), self.get_height(root.right))

        bf_root = self.get_bf(root)

        if bf_root > 1 and key < root.left.key:
            return self.right_rotate(root)
        
        if bf_root < -1 and key > root.right.key:
            return self.left_rotate(root)
        
        if bf_root > 1 and key > root.left.key:
            root.left = self.left_rotate(root.left)
            return self.right_rotate(root)
        
        if bf_root < -1 and key < root.right.key:
            root.right = self.right_rotate(root.right)
            return self.left_rotate(root)

        return root
    
    def delete(self, key):
        self.root = self._delete(self.root, key)

    def _delete(self, root, key):
        # Case 1: Tree is empty
        if root is None:
            return root

        # Case 2: key is in left subtree
        elif key < root.key:
            root.left = self._delete(root.left, key)

        # Case 3: key is in right subtree
        elif key > root.key:
            root.right = self._delete(root.right, key)

        # Case 4: key == root.key
        else:
            # Sub-case 1: root.left is NIL
            # Will return None if root has no children.
            if root.left is None:
                tmp_val = root.value
                tmp = root.right
                root = None
                self.items -= 1
                self.size -= getsizeof(key)
                self.size -= getsizeof(tmp_val)
                return tmp 
            
            # Sub-case 2: root.right is NIL
            # Will return None if root has no children.
            elif root.right is None:
                tmp_val = root.value
                tmp = root.left
                root = None
                self.items -= 1
                self.size -= getsizeof(key)
                self.size -= getsizeof(tmp_val)
                return tmp
            
            # Sub-case 3: Both childern are present.
            # We need to find the the new root node, it can either be minimum node from the right sub-tree or the maximum node from the left sub-tree.
            tmp = root.left
            tmp2 = None
            while tmp is not None:
                tmp2 = tmp
                tmp = tmp.right
            
            root.key = tmp2.key
            tmp_val = root.val
            root.left = self._delete(root.left, tmp2.key)
            self.items -= 1
            self.size -= getsizeof(key)
            self.size -= getsizeof(tmp_val)
        
        root.height = 1 + max(self.get_height(root.left), self.get_height(root.right))

        bf_root = self.get_bf(root)

        if bf_root > 1 and self.get_bf(root.left) >= 0:
            return self.right_rotate(root)
        
        if bf_root < -1 and self.get_bf(root.right) <= 0:
            return self.left_rotate(root)
        
        if bf_root > 1 and self.get_bf(root.left) < 0:
            root.left = self.left_rotate(root.left)
            return self.right_rotate(root)

        if bf_root < -1 and self.get_bf(root.right) > 0:
            root.right = self.right_rotate(root.right)
            return self.left_rotate(root)
        
        return root

    def inorder_traversal(self):
        node = self.root
        stack = []

        while stack or node:
            if node:
                stack.append(node)
                node = node.left
            else:
                node = stack.pop()
                yield node
                node = node.right