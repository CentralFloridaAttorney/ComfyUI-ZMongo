import logging

from src.nodes.zmongo_config_node import ZMongoConfigNode
from src.nodes.zmongo_operations_node import ZMongoOperationsNode
from src.nodes.zmongo_record_node import ZMongoRecordSplitter, ZMongoFieldSelector
from src.nodes.zretriever_node import ZRetrieverNode

# Set directory for JS components
WEB_DIRECTORY = "./web"
logger = logging.getLogger(__name__)

NODE_CLASS_MAPPINGS = {
    "ZMongoConfigNode": ZMongoConfigNode,
    "ZMongoOperationsNode": ZMongoOperationsNode,
    "ZMongoRecordSplitter": ZMongoRecordSplitter,
    "ZMongoFieldSelector": ZMongoFieldSelector,
    "ZRetrieverNode": ZRetrieverNode
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConfigNode": "ZMongo Configuration",
    "ZMongoOperationsNode": "ZMongo Operations Manager",
    "ZMongoRecordSplitter": "ZMongo Record Splitter",
    "ZMongoFieldSelector": "ZMongo Field Selector",
    "ZRetrieverNode": "ZMongo Semantic Retriever"
}

__all__ = ["NODE_CLASS_MAPPINGS", "NODE_DISPLAY_NAME_MAPPINGS", "WEB_DIRECTORY"]