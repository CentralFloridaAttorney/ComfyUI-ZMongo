# 1. ROUTE REGISTRATION IMPORTS 🌐
# We use absolute imports to ensure these work correctly during standalone testing.
from ComfyUI.custom_nodes..api.zmongo_field_selector_api import register_zmongo_field_selector_routes
from api.zmongo_record_editor_api import register_zmongo_record_editor_routes
from api.zmongo_tabular_record_view_api import register_zmongo_tabular_record_view_routes

# 2. BUNDLED REGISTRATION HELPER 🚦
# This single function can be called in your root __init__.py to set up everything at once.
def register_all_zmongo_routes(prompt_server_instance):
    """
    Registers all ZMongo-related API routes with the ComfyUI server.
    """
    register_zmongo_field_selector_routes(prompt_server_instance)
    register_zmongo_record_editor_routes(prompt_server_instance)
    register_zmongo_tabular_record_view_routes(prompt_server_instance)

# 3. PUBLIC EXPORTS 📤
__all__ = [
    "register_all_zmongo_routes",
    "register_zmongo_field_selector_routes",
    "register_zmongo_record_editor_routes",
    "register_zmongo_tabular_record_view_routes",
]