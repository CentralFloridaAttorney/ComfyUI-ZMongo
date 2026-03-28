import asyncio
import logging
import threading
import tkinter as tk
from tkinter import ttk

from .zmongo_admin_tabs import ZMongoAdminTabs
from .zmongo_manager import ZMongoManager


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

        self.engine = self._init_engine()
        self._init_ui()

        self.protocol("WM_DELETE_WINDOW", self.on_close)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _init_engine(self):
        """
        Initialize the async singleton correctly and wait for the real instance.
        """
        future = asyncio.run_coroutine_threadsafe(
            ZMongoManager.get_instance(self.loop),
            self.loop,
        )
        return future.result()

    def _init_ui(self):
        self.main_notebook = ttk.Notebook(self)
        self.main_notebook.pack(expand=True, fill="both", padx=10, pady=10)

        self.tabs = ZMongoAdminTabs(
            self.main_notebook,
            self.engine,
            self.loop,
        )

    def on_close(self):
        try:
            if getattr(self, "engine", None) is not None:
                try:
                    if hasattr(self.engine, "async_client"):
                        self.engine.async_client.close()
                    if hasattr(self.engine, "sync_client"):
                        self.engine.sync_client.close()
                except Exception as exc:
                    logging.error("Error closing Mongo clients: %s", exc)

                try:
                    type(self.engine)._instance = None
                except Exception:
                    pass

            if hasattr(self, "loop") and self.loop.is_running():
                self.loop.call_soon_threadsafe(self.loop.stop)

            if hasattr(self, "loop_thread") and self.loop_thread.is_alive():
                self.loop_thread.join(timeout=1.0)
        finally:
            self.destroy()


if __name__ == "__main__":
    app = ZAdminGUI()
    app.mainloop()