import logging
import os
import sys

NODE_ROOT = os.path.dirname(__file__)
if NODE_ROOT not in sys.path:
    sys.path.append(NODE_ROOT)

WEB_DIRECTORY = "./js"

try:
    from server import PromptServer
    from .preset_api import register_preset_routes

    register_preset_routes(PromptServer.instance)
    print("### [ZMongo] Preset API registered")
except Exception:
    logging.exception("### [ZMongo] Preset API registration failed")

try:
    from .zmongo_nodes import (
        ZMongoConfigNode,
        ZMongoTextFetcher,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode, NODE_CLASS_MAPPINGS, NODE_DISPLAY_NAME_MAPPINGS,
)
except Exception:
    logging.exception("### [ZMongo] Node import failed")
    raise


__all__ = [
    "WEB_DIRECTORY",
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
]