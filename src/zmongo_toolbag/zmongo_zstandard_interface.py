import zstandard as zstd
import json
from pymongo import MongoClient
from bson.binary import Binary

client = MongoClient("mongodb://localhost:27017")
db = client.test
collection = db.test_collection

compressor = zstd.ZstdCompressor()
decompressor = zstd.ZstdDecompressor()

data = {"name": "example", "numbers": list(range(1000))}

# serialize
payload = json.dumps(data).encode()

# compress
compressed = compressor.compress(payload)

# store
collection.insert_one({
    "compressed_data": Binary(compressed),
    "compression": "zstd"
})