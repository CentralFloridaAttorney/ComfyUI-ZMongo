import sys
import os

# Add the current directory to sys.path so `src` is discoverable
node_path = os.path.dirname(__file__)
if node_path not in sys.path:
    sys.path.append(node_path)

try:
    from src import (
        ZMongoConfigNode,
        ZMongoTextFetcher,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode,
        ZMongoDatabaseBrowserNode,
        ZMongoRecordLoopNode,
    )
except ImportError as e:
    print(f"ZMongo Load Error: {e}")
    from src.zmongo_nodes import (
        ZMongoConfigNode,
        ZMongoTextFetcher,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode,
        ZMongoDatabaseBrowserNode,
        ZMongoRecordLoopNode, ZMongoPromptDemoNode,
)

NODE_CLASS_MAPPINGS = {
    "ZMongoConfig": ZMongoConfigNode,
    "ZMongoTextFetcher": ZMongoTextFetcher,
    "ZMongoOperations": ZMongoOperationsNode,
    "ZMongoRecordSplitter": ZMongoRecordSplitter,
    "ZMongoFieldSelector": ZMongoFieldSelector,
    "ZRetriever": ZRetrieverNode,
    "ZMongoDatabaseBrowser": ZMongoDatabaseBrowserNode,
    "ZMongoRecordLoopNode": ZMongoRecordLoopNode,
    "ZMongoLoopControllerNode": ZMongoRecordLoopNode,
    "ZMongoPromptDemoNode": ZMongoPromptDemoNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConfig": "🛡️ ZMongo Configuration",
    "ZMongoTextFetcher": "📝 ZMongo Text Fetcher",
    "ZMongoOperations": "📥 ZMongo Operations",
    "ZMongoRecordSplitter": "✂️ ZMongo Record Splitter",
    "ZMongoFieldSelector": "🔍 ZMongo Field Selector",
    "ZRetriever": "🧠 ZMongo Vector Retriever",
    "ZMongoDatabaseBrowser": "🗂️ ZMongo Database Browser",
    "ZMongoRecordLoopNode": "🗂️ ZMongo Record Loop Node",
    "ZMongoLoopControllerNode": " ZMongo Loop Controller Node",
    "ZMongoPromptDemoNode": " ZMongo Prompt Demo Node"
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS"]