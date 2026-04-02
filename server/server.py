import logging
from aiohttp import web

logger = logging.getLogger(__name__)

class PromptServer:
    """A mock of the ComfyUI PromptServer for standalone testing."""
    instance = None

    def __init__(self):
        self.app = web.Application()
        self.routes = web.RouteTableDef()
        # Initialize the singleton instance
        PromptServer.instance = self

    def add_routes(self):
        """Finalize and add the collected routes to the app."""
        self.app.add_routes(self.routes)

    async def start(self, host="127.0.0.1", port=8188):
        """Starts a real local web server."""
        self.add_routes()
        runner = web.AppRunner(self.app)
        await runner.setup()
        site = web.TCPSite(runner, host, port)
        print(f"🚀 ZMongo Demo Server started at http://{host}:{port}")
        print(f"🔗 Test Link: http://{host}:{port}/zmongo/flattened_fields?collection_name=test")
        await site.start()