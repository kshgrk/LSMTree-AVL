from mmh3 import hash
from math import log, floor
from bitarray import bitarray

class BloomFilter():
    def __init__(self, num_items = 10000, false_pos_prob = 0.1):
        self.size, self.num_funs = self._bit_arr_size_and_hash_funs(num_items, false_pos_prob)
        self.bit_array = bitarray(self.size)
        self.bit_array.setall(False)
        self.seed = [i*1337 for i in range (self.num_funs)]
    
    def _bit_arr_size_and_hash_funs(self, num_items, flase_pos_prob):
        size = -1*(num_items * log(flase_pos_prob))/(log(2)**2)
        num_funs = max(floor((size/num_items)*log(2)), 2)
        return int(size), int(num_funs)
    
    def _hash(self, key, seed):
        return hash(key, seed) % self.size
    
    def add(self, key):
        for seed in self.seed:
            idx = self._hash(key, seed)
            self.bit_array[idx] = True
        
    def __contains__(self, key):
        for seed in self.seed:
            idx = self._hash(key, seed)
            if not self.bit_array[idx]:
                return False
        return True