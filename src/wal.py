import os
import pickle

class WAL:
    def __init__(self, wal_file = 'wal.log'):
        self.wal_file = wal_file
    
    def append(self, operation, key, value=None):
        record = (operation, key, value)

        try:
            with open(self.wal_file, 'ab') as wal_file:
                pickle.dump(record, wal_file)
                wal_file.flush()
                os.fsync(wal_file.fileno())
            return True
        
        except (OSError, pickle.PickleError)as e:
            print(f'Error writing!!\n{e}')
            return False
        
    def restore(self, memtable):
        if (os.path.exists(self.wal_file)):
            with open(self.wal_file, 'rb') as wal_file:
                try:
                    while True:
                        record = pickle.load(wal_file)
                        operation, key, value = record
                        
                        if operation == 'PUT':
                            memtable.insert(key, value)
                        
                        elif operation == 'DELETE':
                            memtable.delete(key)

                        else:
                            print(f'Unknown Operation: {operation}')

                except EOFError:
                    print('Restore Complete')
                
    def truncate(self):
        try:
            with open(self.wal_file, 'wb'):
                pass
            return True
        
        except OSError as e:
            print(f'Error while truncating:\n{e}')
            return False
