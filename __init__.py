import logging
import os
import sys

# Ensure the custom node package directory is importable so `.src...` works
NODE_PATH = os.path.dirname(__file__)
if NODE_PATH not in sys.path:
    sys.path.append(NODE_PATH)

WEB_DIRECTORY = "./web"

logger = logging.getLogger(__name__)


def _safe_register(route_func, label: str) -> None:
    """Register PromptServer routes without breaking node loading."""
    try:
        route_func(PromptServer.instance)
    except Exception as exc:
        logger.warning("ZMongo %s route registration warning: %s", label, exc)


from server import PromptServer

from .src.zmongo_nodes import (
    ZMongoConfigNode,
    ZMongoDataPassThroughNode,
    ZMongoDatabaseBrowserNode,
    ZMongoFieldSelector,
    ZMongoOperationsNode,
    ZMongoRecordLoopNode,
    ZMongoRecordSplitter,
    ZMongoSaveBatchTextNode,
    ZMongoSaveTextNode,
    ZMongoSaveValueNode,
    ZMongoTextFetcher,
    ZRetrieverNode,
)
from .src.zmongo_field_selector_node import ZMongoFieldSelectorNode
from .src.zmongo_flattened_field_selector_node import ZMongoFlattenedFieldSelectorNode
from .src.zmongo_tabular_record_view_node import ZMongoTabularRecordViewNode
from .src.zmongo_tabular_record_view_api import (
    register_zmongo_tabular_record_view_routes,
)
from .src.zmongo_field_selector_api import register_zmongo_field_selector_routes
from .src.zmongo_record_editor_node import ZMongoRecordEditorNode
from .src.zmongo_record_editor_api import register_zmongo_record_editor_routes
from .src.preset_api import register_preset_routes

from .src.model_loader import (
    ZMongoModelCompatibilityDisplayNode,
    ZMongoModelIntrospectorNode,
    ZMongoUniversalModelSelectorNode,
    ZMongoUniversalModelLoaderNode,
    ZMongoUniversalModelAdapterNode,
    ZMongoBuiltInLoaderAdapterNode,
)

_safe_register(register_preset_routes, "preset")
_safe_register(register_zmongo_field_selector_routes, "field selector")
_safe_register(register_zmongo_tabular_record_view_routes, "tabular record view")
_safe_register(register_zmongo_record_editor_routes, "record editor")

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
    "ZMongoUniversalModelSelectorNode": ZMongoUniversalModelSelectorNode,
    "ZMongoModelIntrospectorNode": ZMongoModelIntrospectorNode,
    "ZMongoModelCompatibilityDisplayNode": ZMongoModelCompatibilityDisplayNode,
    "ZMongoUniversalModelLoaderNode": ZMongoUniversalModelLoaderNode,
    "ZMongoUniversalModelAdapterNode": ZMongoUniversalModelAdapterNode,
    "ZMongoBuiltInLoaderAdapterNode": ZMongoBuiltInLoaderAdapterNode,
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
    "ZMongoUniversalModelSelectorNode": "🧩 ZMongo Universal Model Selector",
    "ZMongoModelIntrospectorNode": "🧠 ZMongo Model Introspector",
    "ZMongoModelCompatibilityDisplayNode": "✅ ZMongo Model Compatibility Display",
    "ZMongoUniversalModelAdapterNode": " ZMongo Universal Model Adapter",
    "ZMongoBuiltInLoaderAdapterNode": " ZMongo BuiltIn Loader Adapter",
}

__all__ = [
    "NODE_CLASS_MAPPINGS",
    "NODE_DISPLAY_NAME_MAPPINGS",
    "WEB_DIRECTORY",
]