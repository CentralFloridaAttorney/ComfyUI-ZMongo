import os
import sys
import logging
import asyncio
from aiohttp import web

# 1. PATH CONFIGURATION 🛠️
# Ensures we can find 'nodes' and 'zmongo_toolbag'
ROOT_PATH = os.path.dirname(os.path.abspath(__file__))
if ROOT_PATH not in sys.path:
    sys.path.append(ROOT_PATH)

# 2. WEB ASSET REGISTRATION 🌐
WEB_DIRECTORY = "./web"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ZMongo-Demo")

# 3. IMPORTS
# We import the routes and the server class we just made
from server import PromptServer
from nodes.zmongo_field_selector_api import register_zmongo_field_selector_routes
from nodes.zmongo_record_editor_api import register_zmongo_record_editor_routes
from nodes.zmongo_tabular_record_view_api import register_zmongo_tabular_record_view_routes


# 4. SERVER API REGISTRATION 🚦
def setup_server():
    server = PromptServer()

    # Register your custom routes with the server instance
    register_zmongo_field_selector_routes(server)
    register_zmongo_record_editor_routes(server)
    register_zmongo_tabular_record_view_routes(server)

    return server


# 5. RUNNABLE DEMO 🚀
if __name__ == "__main__":
    # Create the server environment
    demo_server = setup_server()

    # Run the async loop
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(demo_server.start())
        loop.run_forever()
    except KeyboardInterrupt:
        print("\nStopping ZMongo Demo Server...")