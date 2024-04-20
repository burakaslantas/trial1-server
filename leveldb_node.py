# leveldb_node.py

import plyvel
import os
import logging

class LevelDBNode:
    def __init__(self, db_path):
        self.db_path = db_path
        self.db = None
        try:
            if not os.path.exists(db_path):
                os.makedirs(db_path)
            self.db = plyvel.DB(db_path, create_if_missing=True)
        except Exception as e:
            logging.error(f"Error opening LevelDB at {db_path}: {e}")
            if self.db:
                self.db.close()

    def get(self, key):
        """Fetches the value for a given key from the LevelDB database."""
        try:
            value = self.db.get(key.encode('utf-8'))
            return value.decode('utf-8') if value else None
        except Exception as e:
            logging.error(f"Error retrieving key {key} from LevelDBNode at {self.db_path}: {e}")
            return None

    def put(self, key, value):
        """Inserts or updates a key-value pair in the LevelDB database."""
        try:
            self.db.put(key.encode('utf-8'), value.encode('utf-8'))
            logging.info(f"Key {key} added/updated in LevelDBNode at {self.db_path}")
        except Exception as e:
            logging.error(f"Error adding/updating key {key} in LevelDBNode at {self.db_path}: {e}")

    def get_all(self):
        """Fetches all key-value pairs from the LevelDB database."""
        try:
            return {k.decode('utf-8'): v.decode('utf-8') for k, v in self.db.iterator()}
        except Exception as e:
            logging.error(f"Error fetching all key-value pairs from LevelDBNode at {self.db_path}: {e}")
            return {}
        
    def get_stats(self):
        try:
            # Assuming these methods exist or need to be implemented to fetch the relevant stats
            key_count = len(self.get_all())
            db_size = os.path.getsize(self.db_path)  # This would be simplistic; more accurate measures might be needed
            return {'key_count': key_count, 'db_size': db_size}
        except Exception as e:
            logging.error(f"Error fetching stats from LevelDBNode at {self.db_path}: {e}")
            return {}

    def close(self):
        """Closes the LevelDB database."""
        if self.db:
            self.db.close()
            logging.info(f"Closed LevelDB at {self.db_path}")

# This allows testing the functionality directly if this script is run standalone
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db_path = '/home/node1/Desktop/DBs/db_node1'  # Replace with your LevelDB path
    node = LevelDBNode(db_path)

    
