import sys
import os

# Add the current directory to sys.path to ensure 'src' is findable
# as a sub-module during the ComfyUI boot sequence.
node_path = os.path.dirname(__file__)
if node_path not in sys.path:
    sys.path.append(node_path)

# Use relative imports to pull from your src directory
try:
    # Assuming these are defined in src/__init__.py or src/nodes.py
    # If they are in a specific file like src/zmongo_nodes.py,
    # change the import to: from .src.zmongo_nodes import ...
    from src import (
        ZMongoConfigNode,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode
    )
except ImportError as e:
    print(f"ZMongo Load Error: {e}")
    # Fallback attempt if they are in a sub-file called nodes.py
    from zmongo_nodes import (
        ZMongoConfigNode,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode
    )

NODE_CLASS_MAPPINGS = {
    "ZMongoConfig": ZMongoConfigNode,
    "ZMongoOperations": ZMongoOperationsNode,
    "ZMongoRecordSplitter": ZMongoRecordSplitter,
    "ZMongoFieldSelector": ZMongoFieldSelector,
    "ZRetriever": ZRetrieverNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConfig": "🛡️ ZMongo Configuration",
    "ZMongoOperations": "📥 ZMongo Operations",
    "ZMongoRecordSplitter": "✂️ ZMongo Record Splitter",
    "ZMongoFieldSelector": "🔍 ZMongo Field Selector",
    "ZRetriever": "🧠 ZMongo Vector Retriever",
}

__all__ = ['NODE_CLASS_MAPPINGS', 'NODE_DISPLAY_NAME_MAPPINGS']