from pathlib import Path
import msgpack
import zlib
import os
from .avl import AVL
from .bloom_filter import BloomFilter
from .wal import WAL
from bitarray import bitarray

class LSM():
    def __init__(self, lsm_dir):
        self.lsm_dir = lsm_dir
        self.sstables_dir = os.path.join(self.lsm_dir, 'sstables/')
        self.index_path = os.path.join(self.lsm_dir, 'index.bin')
        self.wal_path = os.path.join(self.lsm_dir, 'wal.log')
        self.metadata_path = os.path.join(self.lsm_dir, 'metadata.bin')
        self.all_segments = []
        self.memtable = AVL()
        self.index = AVL()
        self.wal = WAL(wal_file=self.wal_path)
        self.segment_count = 0

        if not (Path(lsm_dir).exists() and Path(lsm_dir).is_dir()):
            Path(lsm_dir).mkdir()
            Path(self.sstables_dir).mkdir()
        
        self.load_metadata() 
    
    def insert(self, key, value):
        #Write to wal first
        if self.wal.append('PUT', key, value):
            self.memtable.insert(key, value)

            if self.memtable.items >= 10000 or self.memtable.size >= 1000000:
                print('Memtable full, flushing to disk.')
                filename = os.path.join(self.lsm_dir, 'rawMemtable.bin')
                self.flush_tree(self.memtable, filename)
                self.wal.truncate()
                # Initiate compaction on separate thread for uninteruppted read and writes in db.
                ### TODO
                # Insert min key value in index
                ### TODO
    
    def search(self, key):
        if key in self.memtable.bloom_filter:
            node = self.memtable.search(key)
            if node:
                return node.value #checking false positive
        else:
            for segment in self.all_segments.sort(reverse=True):
                segment_path = os.path.join(self.sstables_dir, segment)
                blm_fltr = self.load_segment_bloom_fiter(segment_path)
                if key in blm_fltr:
                    segment_data = self.load_tree(segment_path)
                    node = segment_data.search(key)
                    if node:
                        return node.value #checking false positive
        return f'{key} doesn\'t exist in db.'

    def load_metadata(self):
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path) as f:
                compressed_data = f.read()
            decompressed_data = zlib.decompress(compressed_data)
            unpacked_data = msgpack.unpackb(decompressed_data)
            self.segment_count = unpacked_data['segment_count']
            self.all_segments = unpacked_data['all_segments']

            # Loading index
            self.index = self.load_tree(self.index_path)

            # Loading current segment into memtable
            current_segment_path = os.path.join(self.sstables_dir, f'segment_{self.segment_count}.bin') 
            self.memtable = self.load_tree(current_segment_path)
            print('Existing instance found!!\nData successfully loaded.')
            return
        print('No existing instance found.\nNew instance created.')        
        return

    def unpacking_data_bin(self, filename):
        if os.path.exists(filename):
            with open(filename, 'rb') as f:
                compressed_data = f.read()
            decompressed_data = zlib.decompress(compressed_data)
            unpacked_data = msgpack.unpackb(decompressed_data)
            return unpacked_data
        print(f'Error!!\n{filename} doesn\'t exist')
        return None
    
    def save_metadata(self):
        metadata = {
            'segment_count': self.segment_count,
            'all_segments': self.all_segments,
        }
        # Saving index in index.bin
        self.flush_tree(self.index, self.index_path)

        packed_data = msgpack.packb(metadata)
        compressed_data = zlib.compress(packed_data)

        with open(self.metadata_path, 'wb') as f:
            f.write(compressed_data)

        info = f"""DB Path: {self.lsm_dir}
        Total Segments: {self.segment_count}
        Total Items in Index: Items-{self.index.items} Size-{self.index.size}
        Total Items in Memtable: Items-{self.memtable.items} Size-{self.memtable.size}
        Index Sparsity: {self.sparsity}
        """
        print(info)

    def serialize_node(self, node):
        if node is None:
            return None
        return {
            'key': node.key,
            'value': node.value,
            'height': node.height,
            'left': self.serialize_node(node.left),
            'right': self.serialize_node(node.right)
        }

    def deserialize_node(self, node_data):
        if node_data is None:
            return []
        return [(node_data['key'], node_data['value'])] + \
            self.deserialize_node(node_data['left']) + \
            self.deserialize_node(node_data['right'])

    def flush_tree(self, tree, filename):
        
        serialized_tree = self.serialize_node(tree.root)

        blm_filter = tree.bloom_filter
        serialized_bloom_filter = {
            'bit_array': blm_filter.bit_array.tobytes(),
            'size': blm_filter.size,
            'num_funs': blm_filter.num_funs,
            'seed': blm_filter.seed
        }

        combined_data = {
            'segment_data': serialized_tree,
            'bloom_filter': serialized_bloom_filter,
            'segment_items': tree.items ,
            'segment_size': tree.size
        }

        packed_data = msgpack.packb(combined_data)

        compressed_data = zlib.compress(packed_data)

        with open(filename, 'wb') as f:
            f.write(compressed_data)

    def load_tree(self, filename):
        unpacked_data = self.unpacking_data_bin(filename)

        segment_data = AVL()
        node_list = self.deserialize_node(unpacked_data['segment_data'])
        for key, value in node_list:
            segment_data.root = segment_data.insert(segment_data.root, key, value)
        
        # Checking if unpacked correctly or not
        if segment_data.items == unpacked_data['segment_items'] and segment_data.size == unpacked_data['segment_size']:
            print('Items count and size match!!')
        
        else:
            print('Either items count or size don\'t match. Data maybe corrupted')
        
        return segment_data
    
    def load_segment_bloom_filter(self, filename):
        unpacked_data = self.unpacking_data_bin(filename)
        bloom_filter_data = unpacked_data['bloom_filter']
        bloom_filter = BloomFilter() 
        bloom_filter.bit_array = bitarray()
        bloom_filter.bit_array.frombytes(bloom_filter_data['bit_array'])
        bloom_filter.size = bloom_filter_data['size']
        bloom_filter.num_funs = bloom_filter_data['num_funs']
        bloom_filter.seed = bloom_filter_data['seed']

        return bloom_filter