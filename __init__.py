import os
import sys
import logging

# Add the current directory to sys.path so `src` is discoverable
node_path = os.path.dirname(__file__)
if node_path not in sys.path:
    sys.path.append(node_path)

WEB_DIRECTORY = "./js"

logger = logging.getLogger(__name__)

try:
    from server import PromptServer

    from src import (
        ZMongoConfigNode,
        ZMongoTextFetcher,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode,
        ZMongoDatabaseBrowserNode,
        ZMongoRecordLoopNode,
        ZMongoSaveTextNode,
        ZMongoDataPassThroughNode,
        ZMongoSaveBatchTextNode,
        ZMongoSaveValueNode,
        ZMongoTabularRecordViewNode,
        register_zmongo_field_selector_routes,
        register_zmongo_tabular_record_view_routes,
        register_zmongo_record_editor_routes,
        ZMongoRecordEditorNode,
    )
    from src.zmongo_field_selector_node import ZMongoFieldSelectorNode
    from src.zmongo_flattened_field_selector_node import ZMongoFlattenedFieldSelectorNode
    from src.preset_api import register_preset_routes

except ImportError as e:
    print(f"ZMongo Load Error: {e}")

    from server import PromptServer

    from .src.zmongo_nodes import (
        ZMongoConfigNode,
        ZMongoTextFetcher,
        ZMongoOperationsNode,
        ZMongoRecordSplitter,
        ZMongoFieldSelector,
        ZRetrieverNode,
        ZMongoDatabaseBrowserNode,
        ZMongoRecordLoopNode,
        ZMongoSaveTextNode,
        ZMongoDataPassThroughNode,
        ZMongoSaveBatchTextNode,
        ZMongoSaveValueNode,
    )
    from .src.zmongo_tabular_record_view_node import ZMongoTabularRecordViewNode
    from .src.zmongo_tabular_record_view_api import register_zmongo_tabular_record_view_routes
    from .src.zmongo_field_selector_node import ZMongoFieldSelectorNode
    from .src.zmongo_field_selector_api import register_zmongo_field_selector_routes
    from .src.zmongo_flattened_field_selector_node import ZMongoFlattenedFieldSelectorNode
    from .src.zmongo_record_editor_node import ZMongoRecordEditorNode
    from .src.zmongo_record_editor_api import register_zmongo_record_editor_routes
    from .src.preset_api import register_preset_routes


try:
    register_preset_routes(PromptServer.instance)
except Exception as exc:
    print(f"ZMongo preset route registration warning: {exc}")

try:
    register_zmongo_field_selector_routes(PromptServer.instance)
except Exception as exc:
    print(f"ZMongo field route registration warning: {exc}")

try:
    register_zmongo_tabular_record_view_routes(PromptServer.instance)
except Exception as exc:
    print(f"ZMongo tabular route registration warning: {exc}")

try:
    register_zmongo_record_editor_routes(PromptServer.instance)
except Exception as exc:
    print(f"ZMongo record editor route registration warning: {exc}")


NODE_CLASS_MAPPINGS = {
    "ZMongoConfig": ZMongoConfigNode,
    "ZMongoTextFetcher": ZMongoTextFetcher,
    "ZMongoOperations": ZMongoOperationsNode,
    "ZMongoRecordSplitter": ZMongoRecordSplitter,
    "ZMongoFieldSelector": ZMongoFieldSelector,
    "ZRetriever": ZRetrieverNode,
    "ZMongoDatabaseBrowser": ZMongoDatabaseBrowserNode,
    "ZMongoRecordLoopNode": ZMongoRecordLoopNode,
    "ZMongoSaveTextNode": ZMongoSaveTextNode,
    "ZMongoDataPassThroughNode": ZMongoDataPassThroughNode,
    "ZMongoSaveBatchTextNode": ZMongoSaveBatchTextNode,
    "ZMongoSaveValueNode": ZMongoSaveValueNode,
    "ZMongoFlattenedFieldSelectorNode": ZMongoFlattenedFieldSelectorNode,
    "ZMongoFieldSelectorNode": ZMongoFieldSelectorNode,
    "ZMongoTabularRecordViewNode": ZMongoTabularRecordViewNode,
    "ZMongoRecordEditorNode": ZMongoRecordEditorNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoConfig": "🛡️ ZMongo Configuration",
    "ZMongoTextFetcher": "📝 ZMongo Text Fetcher",
    "ZMongoOperations": "📥 ZMongo Operations",
    "ZMongoRecordSplitter": "✂️ ZMongo Record Splitter",
    "ZMongoFieldSelector": "🔍 ZMongo Field Selector",
    "ZRetriever": "🧠 ZMongo Vector Retriever",
    "ZMongoDatabaseBrowser": "🗂️ ZMongo Database Browser",
    "ZMongoRecordLoopNode": "🔁 ZMongo Record Loop Node",
    "ZMongoSaveTextNode": "💾 ZMongo Save Text Node",
    "ZMongoDataPassThroughNode": "🔀 ZMongo Data Pass Through Node",
    "ZMongoSaveBatchTextNode": "📚 ZMongo Save Batch Text Node",
    "ZMongoSaveValueNode": "🧷 ZMongo Save Value Node",
    "ZMongoFlattenedFieldSelectorNode": "🧭 ZMongo Flattened Field Selector",
    "ZMongoFieldSelectorNode": "🔎 ZMongo Field Selector Node",
    "ZMongoTabularRecordViewNode": "📊 ZMongo Tabular Record View",
    "ZMongoRecordEditorNode": "📝 ZMongo Record Editor",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]