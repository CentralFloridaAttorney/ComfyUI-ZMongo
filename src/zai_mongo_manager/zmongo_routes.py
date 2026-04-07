import server
import mimetypes
from aiohttp import web
from zai_mongo_manager.zmongo_server import ZMongoServer
# 1. Initialize the manager
z_manager = ZMongoServer()
prompt_server = server.PromptServer.instance

# --- FIX: MANUALLY REGISTER MIME TYPES ---
# This prevents the "disallowed MIME type (application/octet-stream)" error
# that causes browsers to block your .js and .css files.
mimetypes.add_type('application/javascript', '.js')
mimetypes.add_type('text/css', '.css')

# 2. Register API Routes directly
# Using prompt_server.app.add_routes is more reliable than the startup hook.
routes = [
    web.get("/zai/zmongo/healthz", z_manager.healthz),
    web.get("/zai/zmongo/collections", z_manager.list_collections),
    web.get("/zai/zmongo/docs/{coll}", z_manager.get_docs),
    web.get("/zai/zmongo/doc/{coll}/{id}", z_manager.get_single_doc),
    web.post("/zai/zmongo/update", z_manager.update_doc),
    web.post("/zai/zmongo/create", z_manager.create_doc),
    web.post("/zai/zmongo/delete", z_manager.delete_doc),
    web.get("/zai/zmongo/dashboard", z_manager.serve_dashboard),
]

# Add these routes to the existing ComfyUI aiohttp app
prompt_server.app.add_routes(routes)

# 3. Required by ComfyUI
NODE_CLASS_MAPPINGS = {}

# CRITICAL: This maps the 'web' folder inside your custom node.
# It allows ComfyUI to serve files at: /extensions/[your_node_name]/
WEB_DIRECTORY = "web"