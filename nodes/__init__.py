# 1. SPECIALIZED MODEL NODE IMPORTS 🧠
# Using absolute imports ensures these are found during standalone tests.
from ..nodes.model_loader.zmongo_model_compatibility_display_node import ZMongoModelCompatibilityDisplayNode
from ..nodes.model_loader.zmongo_model_introspector_node import ZMongoModelIntrospectorNode
from ..nodes.model_loader.zmongo_universal_model_selector_node import ZMongoUniversalModelSelectorNode
from ..nodes.model_loader.zmongo_universal_model_loader_node import ZMongoUniversalModelLoaderNode
from ..nodes.model_loader.zmongo_universal_adapter_node import ZMongoUniversalModelAdapterNode
from ..nodes.model_loader.zmongo_builtin_loader_adapter_node import ZMongoBuiltInLoaderAdapterNode

# 2. SUB-PACKAGE MAPPINGS 📋
# Defining these here allows the parent 'nodes' folder to simply import and merge them.
NODE_CLASS_MAPPINGS = {
    "ZMongoUniversalModelSelectorNode": ZMongoUniversalModelSelectorNode,
    "ZMongoModelIntrospectorNode": ZMongoModelIntrospectorNode,
    "ZMongoModelCompatibilityDisplayNode": ZMongoModelCompatibilityDisplayNode,
    "ZMongoUniversalModelLoaderNode": ZMongoUniversalModelLoaderNode,
    "ZMongoUniversalModelAdapterNode": ZMongoUniversalModelAdapterNode,
    "ZMongoBuiltInLoaderAdapterNode": ZMongoBuiltInLoaderAdapterNode,
}

NODE_DISPLAY_NAME_MAPPINGS = {
    "ZMongoUniversalModelSelectorNode": "🧩 ZMongo Universal Model Selector",
    "ZMongoModelIntrospectorNode": "🧠 ZMongo Model Introspector",
    "ZMongoModelCompatibilityDisplayNode": "✅ ZMongo Model Compatibility Display",
    "ZMongoUniversalModelLoaderNode": "📦 ZMongo Universal Model Loader",
    "ZMongoUniversalModelAdapterNode": "🔌 ZMongo Universal Model Adapter",
    "ZMongoBuiltInLoaderAdapterNode": "⚙️ ZMongo Built-In Loader Adapter",
}