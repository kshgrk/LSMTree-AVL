#### AVL tree implementation, it will house offset of that segment along with the segment id, so that its easier while doing a memflush.

class Node:
    def __init__(self, key, value, offset, segment):
        self.key = key
        self.value = value
        self.height = 1
        self.right = None
        self.left = None
        self.offset = offset
        self.segment = segment

class AVL:
    def __init__(self):
        self.root = None

    def get_height(self, node):
        if node is None:
            return 0
        else:
            return node.height

    def get_bf(self, node):
        if node is None:
            return 0
        else:
            return self.get_height(node.left)-self.get_height(node.right)
        
    def search(self, key):
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
        A = node
        B = A.right
        C = B.left

        node = B
        node.left = A
        node.left.right = C

        node.height = 1 + max(self.get_height(node.left), self.get_height(node.right))
        node.left.height = 1 +  max(self.get_height(node.left.left), self.get_height(node.left.right))

        return node

    def right_rotate(self, node):
        A = node
        B = A.left
        D = B.right

        node = B
        node.right = A
        node.right.left = D

        node.bf = 1 + max(self.get_height(node.left), self.get_height(node.right))
        node.left.bf = 1 +  max(self.get_height(node.left.left), self.get_height(node.left.right))

    def insert(self, root, key):
        if root is None:
            return Node(key)
        if key < root.key:
            root.left = self.insert(root.left, key)
        else:
            root.right = self.insert(root.right, key)
        
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
    
    def delete(self, root, key):
        # Case 1: Tree is empty
        if root is None:
            return root

        # Case 2: key is in left subtree
        elif key < root.key:
            root.left = self.delete(root.left, key)

        # Case 3: key is in right subtree
        elif key > root.key:
            root.right = self.delete(root.right, key)

        # Case 4: key == root.key
        else:
            # Sub-case 1: root.left is NIL
            # Will return None if root has no children.
            if root.left is None:
                tmp = root.right
                root = None
                return tmp
            
            # Sub-case 2: root.right is NIL
            # Will return None if root has no children.
            elif root.right is None:
                tmp = root.left
                root = None
                return tmp
            
            # Sub-case 3: Both childern are present.
            # We need to find the the new root node, it can either be minimum node from the right sub-tree or the maximum node from the left sub-tree.
            tmp = root.left
            tmp2 = None
            while tmp is not None:
                tmp2 = tmp
                tmp = tmp.right
            
            root.key = tmp2.key
            root.left = self.delete(root.left, tmp2.key)
        
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

    def inorder_traversal(self, node=None):
        if node is None:
            node = self.root

        if node:
            yield from self.inorder_traversal(node.left)
            yield node
            yield from self.inorder_traversal(node.right)