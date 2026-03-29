import asyncio
import logging
import os
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import ttk

from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient

from zmongo_admin_tabs import ZMongoAdminTabs
from zmongo_toolbag.zmongo import ZMongo

load_dotenv(Path.home() / ".resources" / ".env")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "test")


class ZMongoAdminBackend:
    """
    Non-GUI backend bridge for the admin GUI.

    This is what ZMongoAdminTabs should receive.
    """
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop
        self.zmongo = ZMongo()

        self.async_client = AsyncIOMotorClient(MONGO_URI)
        self.db = self.async_client[MONGO_DATABASE_NAME]

        self.sync_client = MongoClient(MONGO_URI)
        self.sync_db = self.sync_client[MONGO_DATABASE_NAME]

        self.comfyui_to_zmongo: queue.Queue = queue.Queue()
        self.zmongo_to_comfyui: queue.Queue = queue.Queue()

    def run_in_async_loop(self, async_func, *args, **kwargs):
        return asyncio.run_coroutine_threadsafe(async_func(*args, **kwargs), self.loop)

    def submit_from_comfyui(self, payload: dict):
        self.comfyui_to_zmongo.put(payload)

    def submit_to_comfyui(self, payload: dict):
        self.zmongo_to_comfyui.put(payload)

    async def process_comfyui_payload(self, payload: dict):
        """
        Store inbound ComfyUI events/results in Mongo.
        """
        document = {
            "source": "comfyui",
            "payload_type": payload.get("type", "unknown"),
            "payload": payload,
        }
        return await self.zmongo.insert_document("comfyui_events", document)

    def close(self):
        try:
            self.async_client.close()
        except Exception as exc:
            logging.error("Error closing async Mongo client: %s", exc)

        try:
            self.sync_client.close()
        except Exception as exc:
            logging.error("Error closing sync Mongo client: %s", exc)


class ZAdminGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("ZMongo Professional Admin System")
        self.geometry("1400x900")

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(
            target=self._run_loop,
            name="zadmin-async-loop",
            daemon=True,
        )
        self.loop_thread.start()

        self.engine = ZMongoAdminBackend(self.loop)

        self.main_notebook = ttk.Notebook(self)
        self.main_notebook.pack(expand=True, fill="both", padx=10, pady=10)

        self.tabs = ZMongoAdminTabs(self.main_notebook, self.engine, self.loop)

        self.after(100, self._pump_bridge)
        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _pump_bridge(self):
        """
        Pump ComfyUI -> Mongo payloads from the Tk thread.
        """
        try:
            while True:
                payload = self.engine.comfyui_to_zmongo.get_nowait()
                future = self.engine.run_in_async_loop(self.engine.process_comfyui_payload, payload)
                future.add_done_callback(self._handle_bridge_result)
        except queue.Empty:
            pass

        self.after(100, self._pump_bridge)

    def _handle_bridge_result(self, future):
        try:
            result = future.result()
            self.engine.submit_to_comfyui({
                "type": "zmongo_ack",
                "success": getattr(result, "success", False),
                "data": getattr(result, "data", None),
                "error": getattr(result, "error", None),
            })
        except Exception as exc:
            logging.error("Bridge task failed: %s", exc)

    def on_close(self):
        try:
            if getattr(self, "engine", None) is not None:
                self.engine.close()

            if hasattr(self, "loop") and self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)

            if hasattr(self, "loop_thread") and self.loop_thread.is_alive():
                self.loop_thread.join(timeout=1.0)
        finally:
            self.destroy()


if __name__ == "__main__":
    app = ZAdminGUI()
    app.mainloop()