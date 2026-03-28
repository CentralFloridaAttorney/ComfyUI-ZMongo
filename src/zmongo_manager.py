# zmongo_manager.py
import asyncio
import csv
import json
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import ttk, filedialog, Tk, Listbox, Entry, Button, Frame, Toplevel, END, BOTH, LEFT, RIGHT, Y, X, TOP
from tkinter.scrolledtext import ScrolledText

from bson import errors, json_util
from bson.objectid import ObjectId
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, InsertOne, ReplaceOne
from pymongo.errors import BulkWriteError

# Handle dynamic imports for the toolbag
try:
    from .zmongo_toolbag.data_processing import DataProcessor
    from .zmongo_toolbag.zmongo import ZMongo
    from .zmongo_toolbag.safe_result import SafeResult
except (ImportError, ValueError):
    from zmongo_toolbag.data_processing import DataProcessor
    from zmongo_toolbag.zmongo import ZMongo
    from zmongo_toolbag.safe_result import SafeResult

# BSON Streaming support check
try:
    from bson import BSON, decode_file_iter

    HAVE_BSON_STREAM = True
except ImportError:
    HAVE_BSON_STREAM = False

# --- Configuration ---
load_dotenv(Path.home() / ".resources" / ".env")
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

MONGO_URI = os.getenv("MONGO_URI", "mongodb://127.0.0.1:27017")
MONGO_DATABASE_NAME = os.getenv("MONGO_DATABASE_NAME", "test")
MONGO_BACKUP_DIR_REL = os.getenv("MONGO_BACKUP_DIR", '.resources/mongo_backups')


@dataclass
class Pager:
    limit: int = 100
    skip: int = 0


class ZMongoManager(Tk):
    _instance = None
    _instance_lock = asyncio.Lock()

    def __init__(self, loop: asyncio.AbstractEventLoop = None):
        super().__init__()
        self.zmongo = ZMongo()
        self.title("ZMongo System Manager")
        self.geometry("1400x900")

        # Fallback to current loop if not provided
        self.loop = loop or asyncio.get_event_loop()
        self.db_name = MONGO_DATABASE_NAME
        self.backup_dir = Path.home() / MONGO_BACKUP_DIR_REL / MONGO_DATABASE_NAME
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        try:
            self.async_client = AsyncIOMotorClient(MONGO_URI)
            self.db = self.async_client[self.db_name]
            self.sync_client = MongoClient(MONGO_URI)
            self.sync_db = self.sync_client[self.db_name]
        except Exception as e:
            logging.error(f"Failed to connect to MongoDB: {e}")
            self.destroy()
            return

        self.cv_pager = Pager()
        self.cv_ids_cache = []

        self._create_widgets()
        self.run_periodic_updates()

    @classmethod
    async def get_instance(cls):
        """
        Async singleton accessor. Requires no arguments.
        Detects the running loop automatically.
        """
        if cls._instance is not None:
            return cls._instance

        async with cls._instance_lock:
            if cls._instance is None:
                # Get the loop currently running this coroutine
                loop = asyncio.get_running_loop()
                cls._instance = cls(loop)
        return cls._instance

    # ---------- UI BUILD ----------
    def _create_widgets(self):
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(expand=True, fill=BOTH, padx=10, pady=10)

        tabs = {
            "info": ttk.Frame(self.notebook),
            "maint": ttk.Frame(self.notebook),
            "viewer": ttk.Frame(self.notebook),
            "system": ttk.Frame(self.notebook)
        }

        self.notebook.add(tabs["info"], text='Database Info')
        self.notebook.add(tabs["maint"], text='Backup & Restore')
        self.notebook.add(tabs["viewer"], text='Collection Viewer / Editor')
        self.notebook.add(tabs["system"], text='System Runner')

        self.db_info_text = ScrolledText(tabs["info"], wrap="word", font=("Courier New", 10))
        self.db_info_text.pack(expand=True, fill=BOTH, padx=5, pady=5)

        self._build_maintenance_tab(tabs["maint"])
        self._build_collection_tab(tabs["viewer"])

    def _build_maintenance_tab(self, parent):
        root = ttk.Frame(parent)
        root.pack(fill=BOTH, expand=True, padx=8, pady=8)
        root.grid_columnconfigure((1, 3), weight=1)
        root.grid_rowconfigure(5, weight=1)

        # Collection List
        self.collection_listbox = Listbox(root, exportselection=False, height=12)
        self.collection_listbox.grid(row=1, column=0, sticky="nswe", padx=5)
        self.collection_listbox.bind('<<ListboxSelect>>', self.on_collection_select)

        # Backup Files List
        self.backup_files_listbox = Listbox(root, exportselection=False, height=12)
        self.backup_files_listbox.grid(row=1, column=2, sticky="nswe", padx=5)
        self.backup_files_listbox.bind('<<ListboxSelect>>', self.on_backup_file_select)

        # Entries
        self.selected_collection_entry = Entry(root, state='readonly')
        self.selected_collection_entry.grid(row=1, column=1, sticky="we", padx=5)
        self.selected_backup_entry = Entry(root, state='readonly')
        self.selected_backup_entry.grid(row=1, column=3, sticky="we", padx=5)

        # Actions
        actions = ttk.Frame(root)
        actions.grid(row=2, column=1, columnspan=3, sticky="we", pady=10)
        self.backup_format_combo = ttk.Combobox(actions, state='readonly', values=["JSON", "BSON", "CSV"], width=8)
        self.backup_format_combo.current(0)
        self.backup_format_combo.pack(side=LEFT, padx=5)

        Button(actions, text='Backup Selected', command=self.on_backup_selected_clicked).pack(side=LEFT, padx=5)
        Button(actions, text='Restore Selected', command=self.on_restore_clicked).pack(side=LEFT, padx=5)
        Button(actions, text='Browse...', command=self.open_file_explorer).pack(side=LEFT, padx=5)

        self.message_text = ScrolledText(root, height=8, state='disabled')
        self.message_text.grid(row=5, column=0, columnspan=4, sticky="nswe", padx=5)

    def _build_collection_tab(self, parent):
        root = ttk.Frame(parent)
        root.pack(fill=BOTH, expand=True, padx=8, pady=8)
        root.grid_columnconfigure(1, weight=1)
        root.grid_rowconfigure(2, weight=1)

        self.cv_collection_entry = Entry(root)
        self.cv_collection_entry.grid(row=0, column=1, sticky="we", padx=5)
        self.cv_filter_entry = Entry(root)
        self.cv_filter_entry.insert(0, "{}")
        self.cv_filter_entry.grid(row=1, column=1, sticky="we", padx=5)

        # Listbox
        self.cv_doc_listbox = Listbox(root, width=40)
        self.cv_doc_listbox.grid(row=2, column=0, sticky="ns", padx=5)
        self.cv_doc_listbox.bind('<<ListboxSelect>>', self.cv_on_doc_select)

        # JSON Viewer
        self.cv_json_text = ScrolledText(root, font=("Courier New", 10))
        self.cv_json_text.grid(row=2, column=1, sticky="nswe", padx=5)

        # Controls
        btn_frame = ttk.Frame(root)
        btn_frame.grid(row=0, column=2, rowspan=2)
        Button(btn_frame, text="Refresh", command=self.cv_refresh_docs_clicked).pack(fill=X)
        Button(btn_frame, text="Apply $set", command=self.on_apply_dotkey_value_clicked).pack(fill=X)

    # ---------- Logic ----------
    def run_in_async_loop(self, async_func, *args, **kwargs):
        future = asyncio.run_coroutine_threadsafe(async_func(*args, **kwargs), self.loop)
        future.add_done_callback(lambda f: self._handle_future_result(f))
        return future

    def _handle_future_result(self, future):
        try:
            future.result()
        except Exception as e:
            self.log_message(f"Async Error: {e}")

    def log_message(self, message):
        def _append():
            self.message_text.config(state='normal')
            self.message_text.insert("end", f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
            self.message_text.config(state='disabled')
            self.message_text.see("end")

        self.after(0, _append)

    async def backup_collection(self, collection_name, fmt):
        try:
            docs = await self.db[collection_name].find({}).to_list(length=None)
            ts = datetime.now().strftime("%Y%m%d%H%M%S")
            path = self.backup_dir / f"{collection_name}[{ts}].{fmt.lower()}"

            if fmt == "JSON":
                path.write_text(json_util.dumps(docs, indent=2))
            elif fmt == "BSON" and HAVE_BSON_STREAM:
                with open(path, 'wb') as f:
                    for d in docs: f.write(BSON.encode(d))
            elif fmt == "CSV":
                flat = [DataProcessor.flatten_json(d) for d in docs]
                if flat:
                    keys = sorted(list(set().union(*(d.keys() for d in flat))))
                    with open(path, 'w', newline='') as f:
                        writer = csv.DictWriter(f, fieldnames=keys)
                        writer.writeheader()
                        writer.writerows(flat)

            self.log_message(f"Backup saved: {path.name}")
            self.after(0, lambda: self.update_backup_files_listbox(collection_name))
        except Exception as e:
            self.log_message(f"Backup failed: {e}")

    # ---------- Event Handlers ----------
    def on_collection_select(self, event=None):
        idx = self.collection_listbox.curselection()
        if not idx: return
        name = self.collection_listbox.get(idx[0])
        self.selected_collection_entry.config(state='normal')
        self.selected_collection_entry.delete(0, END)
        self.selected_collection_entry.insert(0, name)
        self.selected_collection_entry.config(state='readonly')
        self.update_backup_files_listbox(name)

    def on_backup_selected_clicked(self):
        name = self.selected_collection_entry.get()
        fmt = self.backup_format_combo.get()
        if name: self.run_in_async_loop(self.backup_collection, name, fmt)

    def cv_refresh_docs_clicked(self):
        coll = self.cv_collection_entry.get().strip()
        filt = json.loads(self.cv_filter_entry.get() or "{}")

        async def _fetch():
            cursor = self.db[coll].find(filt, {"_id": 1}).limit(100)
            items = await cursor.to_list(length=100)
            return [(str(i["_id"]), i["_id"]) for i in items]

        def _update(pairs):
            self.cv_doc_listbox.delete(0, END)
            self.cv_ids_cache = pairs
            for s, _ in pairs: self.cv_doc_listbox.insert(END, s)

        self.run_in_async_loop(_fetch).add_done_callback(
            lambda f: self.after(0, lambda: _update(f.result()))
        )

    def cv_on_doc_select(self, event=None):
        idx = self.cv_doc_listbox.curselection()
        if not idx: return
        _, oid = self.cv_ids_cache[idx[0]]
        coll = self.cv_collection_entry.get()

        async def _get():
            return await self.db[coll].find_one({"_id": oid})

        def _show(doc):
            self.cv_json_text.delete("1.0", END)
            self.cv_json_text.insert(END, json_util.dumps(doc, indent=2))

        self.run_in_async_loop(_get).add_done_callback(
            lambda f: self.after(0, lambda: _show(f.result()))
        )

    # --- Cleanup and Periodic ---
    def update_backup_files_listbox(self, name):
        self.backup_files_listbox.delete(0, END)
        if self.backup_dir.exists():
            for f in self.backup_dir.glob(f"{name}[*].*"):
                self.backup_files_listbox.insert(END, f.name)

    async def fetch_and_update_collections(self):
        names = await self.db.list_collection_names()
        self.after(0, lambda: [self.collection_listbox.insert(END, n) for n in sorted(names)])

    def run_periodic_updates(self):
        self.run_in_async_loop(self.fetch_and_update_collections)
        self.after(60000, self.run_periodic_updates)

    def on_closing(self):
        self.async_client.close()
        self.sync_client.close()
        type(self)._instance = None
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.destroy()

    # Stub methods for missing logic from snippet
    def on_restore_clicked(self):
        pass

    def open_file_explorer(self):
        pass

    def on_backup_file_select(self, event):
        pass

    def on_apply_dotkey_value_clicked(self):
        pass


def main():
    loop = asyncio.new_event_loop()
    threading.Thread(target=loop.run_forever, daemon=True).start()

    app = ZMongoManager(loop)
    app.protocol("WM_DELETE_WINDOW", app.on_closing)
    app.mainloop()


if __name__ == "__main__":
    main()