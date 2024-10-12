from pathlib import Path
import msgpack
import zlib
import os
from .avl import AVL
from .bloom_filter import BloomFilter
from .wal import WAL

class LSM():
    def __init__(self, lsm_dir):
        self.sstables_dir = os.path.join(lsm_dir, 'sstables/')
        self.index = os.path.join(lsm_dir, 'index.bin')
        self.wal = os.path.join(lsm_dir, 'wal.log')
        self.memtable = AVL()
        self.index = AVL()
        self.sparsity = 100

        if not (Path(lsm_dir).exists() and Path(lsm_dir).is_dir()):
            Path(lsm_dir).mkdir()
            Path(self.sstables_dir).mkdir()
        
        self.load_metadata() 
        # Metadata will have: -
        # 1. Segment Number
        # 2. Pre-existing memtable and index to be loaded
        # 3. Num of items and size (in bytes)
        #  

    def load_metadata(self):
        pass #Todo

    def save_metadata(self):
        pass #Todo

    def flush_memtable(self):
        def serialize_node(node):
            if node is None:
                return None
            return {
                'key': node.key,
                'value': node.value,
                'height': node.height,
                'left': serialize_node(node.left),
                'right': serialize_node(node.right)
            }
        
        serialized_tree = serialize_node(self.memtable.root)

        serialized_bloom_filter = {
            'bit_array': self.memtable.bloom_filter.bit_array.tobytes(),
            'size': self.memtable.bloom_filter.size,
            'num_funs': self.memtable.bloom_filter.num_funs,
            'seed': self.memtable.bloom_filter.seed
        }

        combined_data = {
            'tree': serialized_tree,
            'bloom_filter': serialized_bloom_filter
        }

        packed_data = msgpack.packb(combined_data)

        compressed_data = zlib.compress(packed_data)

        segment_name = f'segment_{str(segment_count).bin}' #segment_count will be taken from metadata
        filename = os.path.join(self.sstables_dir, segment_name)
        with open(filename, 'wb') as f:
            f.write(compressed_data)

    ######## Needs to be updated according to new AVL structure.
    def read_from_disk(self, filename):
        with open(filename, 'rb') as f:
            compressed_data = f.read()

        decompressed_data = zlib.decompress(compressed_data)

        unpacked_data = msgpack.unpackb(decompressed_data)

        def deserialize_node(node_data):
            if node_data is None:
                return []
            return [(node_data['key'], node_data['value'])] + \
                deserialize_node(node_data['left']) + \
                deserialize_node(node_data['right'])

        tree = AVL()
        node_list = deserialize_node(unpacked_data['tree'])
        for key, value in node_list:
            tree.root = tree.insert(tree.root, key, value)

        return tree