from pymongo import MongoClient
from bson.binary import Binary

client = MongoClient("mongodb://localhost:27017")
db = client.test
collection = db.test_collection

import json
import zstandard as zstd

compressor = zstd.ZstdCompressor()

data = {"name": "example", "numbers": list(range(1000))}

# serialize dict → string
serialized = json.dumps(data)

# encode string → bytes
payload = serialized.encode("utf-8")

# compress bytes
compressed = compressor.compress(payload)

print(compressed)
# store
collection.insert_one({
    "compressed_data": Binary(compressed),
    "compression": "zstd"
})
print(collection.find_one({"name": "example"}))