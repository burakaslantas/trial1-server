# leveldb node app.py

from flask import Flask, request, jsonify
import logging

# Assuming LevelDBNode is defined in leveldb_node.py
from leveldb_node import LevelDBNode

logging.basicConfig(level=logging.INFO)

app = Flask(__name__)

# Initialize the LevelDB Node (adjust the path as necessary)
db_path = '/home/node1/Desktop/DBs/db_node1'
node = None

def get_node():
    global node
    if node is None:
        node = LevelDBNode(db_path)
    return node

@app.route('/get/<key>', methods=['GET'])
def get_key(key):
    node = get_node()
    value = node.get(key)
    if value is None:
        return jsonify({'error': 'Key not found'}), 404
    return jsonify({'key': key, 'value': value})

@app.route('/put', methods=['POST'])
def put_key():
    node = get_node()
    data = request.json
    key = data['key']
    value = data['value']
    node.put(key, value)
    return jsonify({'key': key, 'value': value})

@app.route('/all', methods=['GET'])
def get_all():
    node = get_node()
    all_data = node.get_all()
    return jsonify(all_data)

@app.route('/key-count', methods=['GET'])
def key_count():
    node = get_node()
    count = len(node.get_all())
    return jsonify({'key_count': count})

@app.route('/stats', methods=['GET'])
def get_stats():
    node = get_node()
    stats = node.get_stats()
    return jsonify(stats)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
