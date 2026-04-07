import server
from .zmongo_server import ZMongoServer

# Initialize the server logic
z_manager = ZMongoServer()


# ComfyUI Server Hook
@server.PromptServer.instance.app.on_startup.append
async def setup_zmongo_routes(app):
    # Register API Routes
    app.router.add_get("/zai/zmongo/collections", z_manager.list_collections)
    app.router.add_get("/zai/zmongo/docs/{coll}", z_manager.get_docs)
    app.router.add_get("/zai/zmongo/doc/{coll}/{id}", z_manager.get_single_doc)
    app.router.add_post("/zai/zmongo/update", z_manager.update_doc)
    app.router.add_post("/zai/zmongo/create", z_manager.create_doc)
    app.router.add_post("/zai/zmongo/delete", z_manager.delete_doc)

    # Serve the Dashboard HTML
    app.router.add_get("/zai/zmongo/dashboard", z_manager.serve_dashboard)


# Required by ComfyUI
NODE_CLASS_MAPPINGS = {}

__all__ = ["ZMongoServer"]