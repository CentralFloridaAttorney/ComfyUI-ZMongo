import asyncio
import csv
import json
import logging
import os
import re
import subprocess
import threading
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from tkinter import (
    BOTH,
    END,
    LEFT,
    RIGHT,
    TOP,
    X,
    Y,
    Button,
    Entry,
    Frame,
    Listbox,
    filedialog,
    ttk,
)
from tkinter.scrolledtext import ScrolledText
import tkinter as tk

from bson import errors
from bson.objectid import ObjectId

from zmongo_toolbag.data_processor import DataProcessor
from zmongo_toolbag.safe_result import SafeResult

try:
    from bson import BSON, decode_file_iter

    HAVE_BSON_STREAM = True
except Exception:
    HAVE_BSON_STREAM = False

logger = logging.getLogger(__name__)


@dataclass
class Pager:
    limit: int = 100
    skip: int = 0


class ZMongoAdminTabs:
    """
    Modular admin tabs for Mongo/ZMongo management.

    Supports either:
    - legacy wrapper objects exposing `.client`
    - newer backend objects exposing `.zmongo`
    - raw ZMongo-like objects directly
    """

    def __init__(self, parent_notebook: ttk.Notebook, zmongo_singleton, loop: asyncio.AbstractEventLoop):
        self.parent = parent_notebook
        self.notebook = parent_notebook
        self.loop = loop
        self.manager = zmongo_singleton

        self.zmongo = self._resolve_zmongo(zmongo_singleton)
        self.db = self._resolve_db(zmongo_singleton, self.zmongo)
        self.sync_db = getattr(zmongo_singleton, "sync_db", None)

        if self.db is None:
            raise AttributeError(
                "ZMongoAdminTabs requires an object with either '.client' or '.zmongo', "
                "and a usable '.db' attribute."
            )

        self.db_name = self._resolve_db_name(zmongo_singleton, self.zmongo)

        self.backup_dir = Path.home() / os.getenv("MONGO_BACKUP_DIR", ".resources/backups") / self.db_name
        self.backup_dir.mkdir(parents=True, exist_ok=True)

        self.cv_pager = Pager(limit=100, skip=0)
        self.cv_ids_cache: list[tuple[str, object]] = []
        self.active_processes: dict[str, subprocess.Popen] = {}

        self.collection_listbox = None
        self.backup_files_listbox = None
        self.backup_format_combo = None
        self.message_text = None
        self.console = None
        self.cv_collection_entry = None
        self.cv_filter_entry = None
        self.cv_doc_listbox = None
        self.cv_json_text = None
        self.cv_dotkey_entry = None
        self.cv_value_entry = None

        self._build_tabs(parent_notebook)
        self.run_periodic_updates()

    @staticmethod
    def _resolve_zmongo(zmongo_singleton):
        if hasattr(zmongo_singleton, "client"):
            return zmongo_singleton.client
        if hasattr(zmongo_singleton, "zmongo"):
            return zmongo_singleton.zmongo
        return zmongo_singleton

    @staticmethod
    def _resolve_db(zmongo_singleton, zmongo_obj):
        direct_db = getattr(zmongo_singleton, "db", None)
        if direct_db is not None:
            return direct_db
        return getattr(zmongo_obj, "db", None)

    @staticmethod
    def _resolve_db_name(zmongo_singleton, zmongo_obj):
        db_name = getattr(zmongo_singleton, "db_name", None)
        if db_name:
            return db_name
        db_name = getattr(zmongo_obj, "db_name", None)
        if db_name:
            return db_name
        db = getattr(zmongo_singleton, "db", None) or getattr(zmongo_obj, "db", None)
        if db is not None and hasattr(db, "name"):
            return db.name
        return os.getenv("MONGO_DATABASE_NAME", "test")

    def _build_tabs(self, notebook):
        self.explorer_frame = ttk.Frame(notebook)
        notebook.add(self.explorer_frame, text="Collection Viewer")
        self._build_collection_tab(self.explorer_frame)

        self.maintenance_frame = ttk.Frame(notebook)
        notebook.add(self.maintenance_frame, text="Backup & Restore")
        self._build_maintenance_tab(self.maintenance_frame)

        self.system_frame = ttk.Frame(notebook)
        notebook.add(self.system_frame, text="Service Runners")
        self._build_system_tab(self.system_frame)

    def _build_collection_tab(self, parent: Frame):
        root = ttk.Frame(parent)
        root.pack(fill=BOTH, expand=True, padx=8, pady=8)
        root.grid_columnconfigure(1, weight=1)
        root.grid_rowconfigure(2, weight=1)

        ttk.Label(root, text="Collection:").grid(row=0, column=0, sticky="w")
        self.cv_collection_entry = Entry(root, width=40)
        self.cv_collection_entry.grid(row=0, column=1, sticky="w", padx=6)

        ttk.Label(root, text="Filter (JSON):").grid(row=1, column=0, sticky="w")
        self.cv_filter_entry = Entry(root)
        self.cv_filter_entry.insert(0, "{}")
        self.cv_filter_entry.grid(row=1, column=1, sticky="we", padx=6)

        ctrl = ttk.Frame(root)
        ctrl.grid(row=0, column=2, rowspan=2, sticky="ne")
        Button(ctrl, text="Refresh", command=self.cv_refresh_docs_clicked).pack(side=TOP, padx=4, pady=2)
        Button(ctrl, text="Load More", command=self.cv_load_more_clicked).pack(side=TOP, padx=4, pady=2)

        left = ttk.Frame(root, borderwidth=1, relief="groove")
        left.grid(row=2, column=0, sticky="ns")

        self.cv_doc_listbox = Listbox(left, height=30, width=36, exportselection=False)
        self.cv_doc_listbox.pack(side=LEFT, fill=Y)
        self.cv_doc_listbox.bind("<<ListboxSelect>>", self.cv_on_doc_select)

        scrollbar = ttk.Scrollbar(left, orient="vertical", command=self.cv_doc_listbox.yview)
        scrollbar.pack(side=RIGHT, fill=Y)
        self.cv_doc_listbox.config(yscrollcommand=scrollbar.set)

        right = ttk.Frame(root, borderwidth=1, relief="groove")
        right.grid(row=2, column=1, columnspan=2, sticky="nswe", padx=(8, 0))
        right.grid_columnconfigure(0, weight=1)
        right.grid_rowconfigure(0, weight=1)

        self.cv_json_text = ScrolledText(right, font=("Courier New", 10), wrap="none")
        self.cv_json_text.grid(row=0, column=0, sticky="nswe", padx=5, pady=5)

        editor = ttk.LabelFrame(right, text="Dot-Key Update (Single Doc)")
        editor.grid(row=1, column=0, sticky="we", padx=5, pady=5)

        ttk.Label(editor, text="Dot-key:").pack(side=LEFT, padx=5)
        self.cv_dotkey_entry = Entry(editor, width=30)
        self.cv_dotkey_entry.pack(side=LEFT, padx=5)

        ttk.Label(editor, text="Value:").pack(side=LEFT, padx=5)
        self.cv_value_entry = Entry(editor, width=30)
        self.cv_value_entry.pack(side=LEFT, padx=5)

        Button(editor, text="Apply $set", command=self.on_apply_dotkey_value_clicked).pack(side=RIGHT, padx=5)

    def _build_maintenance_tab(self, parent: Frame):
        root = ttk.Frame(parent)
        root.pack(fill=BOTH, expand=True, padx=8, pady=8)
        root.grid_columnconfigure(0, weight=1)
        root.grid_columnconfigure(2, weight=1)
        root.grid_rowconfigure(3, weight=1)

        ttk.Label(root, text="Collections:").grid(row=0, column=0, sticky="w", padx=5)
        self.collection_listbox = Listbox(root, exportselection=False, height=12)
        self.collection_listbox.grid(row=1, column=0, sticky="nswe", padx=5)
        self.collection_listbox.bind("<<ListboxSelect>>", self.on_collection_select)

        ttk.Label(root, text="Backup Files:").grid(row=0, column=2, sticky="w", padx=5)
        self.backup_files_listbox = Listbox(root, exportselection=False, height=12)
        self.backup_files_listbox.grid(row=1, column=2, sticky="nswe", padx=5)
        self.backup_files_listbox.bind("<<ListboxSelect>>", self.on_backup_file_select)

        actions = ttk.Frame(root)
        actions.grid(row=2, column=0, columnspan=4, sticky="we", pady=10)

        self.backup_format_combo = ttk.Combobox(actions, values=["JSON", "BSON", "CSV"], state="readonly", width=8)
        self.backup_format_combo.current(0)
        self.backup_format_combo.pack(side=LEFT, padx=5)

        Button(actions, text="Backup Selected", command=self.on_backup_selected_clicked).pack(side=LEFT, padx=5)
        Button(actions, text="Restore Selected", command=self.on_restore_clicked).pack(side=LEFT, padx=5)
        Button(actions, text="Browse File...", command=self.open_file_explorer).pack(side=LEFT, padx=5)

        self.message_text = ScrolledText(root, height=8, wrap="word", state="disabled")
        self.message_text.grid(row=3, column=0, columnspan=4, sticky="nswe", padx=5, pady=5)

    def _build_system_tab(self, parent):
        controls = ttk.LabelFrame(parent, text="Process Controls")
        controls.pack(fill=X, padx=10, pady=10)

        tk.Button(controls, text="Start OCR Runner", command=lambda: self.start_service("ocr_runner.py")).pack(
            side=LEFT, padx=5
        )
        tk.Button(controls, text="Start ZRetriever API", command=lambda: self.start_service("zretriever.py")).pack(
            side=LEFT, padx=5
        )

        self.console = ScrolledText(parent, height=20, bg="black", fg="white")
        self.console.pack(expand=True, fill=BOTH, padx=10, pady=10)

    def start_service(self, script_path):
        def _stream():
            self.notebook.after(0, lambda: self.console.insert(END, f"\n[SYSTEM] Starting {script_path}...\n"))
            process = subprocess.Popen(
                ["python", script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
            )
            self.active_processes[script_path] = process
            for line in process.stdout:
                self.notebook.after(0, lambda l=line: (self.console.insert(END, l), self.console.see(END)))

        threading.Thread(target=_stream, daemon=True).start()

    def run_periodic_updates(self):
        self.run_in_async_loop(self.fetch_and_update_db_info)
        self.run_in_async_loop(self.fetch_and_update_collections)
        self.notebook.after(30000, self.run_periodic_updates)

    async def fetch_and_update_db_info(self):
        try:
            await self.db.list_collection_names()
            await self.db.command("dbstats")
        except Exception as e:
            logger.error("DB Info update failed: %s", e)

    async def fetch_and_update_collections(self):
        try:
            names = sorted(await self.db.list_collection_names())
            self.notebook.after(0, lambda: self._update_collection_listbox(names))
        except Exception as e:
            logger.error("Collection list update failed: %s", e)

    def _update_collection_listbox(self, names):
        self.collection_listbox.delete(0, END)
        for name in names:
            self.collection_listbox.insert(END, name)

    def run_in_async_loop(self, async_func, *args, **kwargs):
        future = asyncio.run_coroutine_threadsafe(async_func(*args, **kwargs), self.loop)
        future.add_done_callback(self.on_async_task_done)
        return future

    def on_async_task_done(self, future):
        try:
            future.result()
        except Exception as e:
            self.log_message(f"Async Error: {e}")

    def log_message(self, message):
        if self.message_text is None:
            logger.error(message)
            return

        def _append():
            self.message_text.config(state="normal")
            self.message_text.insert(END, f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
            self.message_text.config(state="disabled")
            self.message_text.see(END)

        self.notebook.after(0, _append)

    def cv_refresh_docs_clicked(self):
        self.cv_pager.skip = 0
        self.cv_refresh_docs(reset=True)

    def cv_load_more_clicked(self):
        self.cv_pager.skip += self.cv_pager.limit
        self.cv_refresh_docs(reset=False)

    def cv_refresh_docs(self, reset: bool):
        collection = self.cv_collection_entry.get().strip()
        if not collection:
            self.log_message("Collection is required.")
            return

        try:
            filt = json.loads(self.cv_filter_entry.get() or "{}")
            if not isinstance(filt, dict):
                raise ValueError
        except Exception:
            filt = {}
            self.log_message("Invalid JSON filter; using {}.")

        async def _fetch():
            cursor = self.db[collection].find(filt, projection={"_id": 1}).skip(self.cv_pager.skip).limit(
                self.cv_pager.limit
            )
            items = await cursor.to_list(length=self.cv_pager.limit)
            ids = [(str(it.get("_id")), it.get("_id")) for it in items]
            self.notebook.after(0, lambda: self._update_cv_listbox(ids, reset))

        self.run_in_async_loop(_fetch)

    def _update_cv_listbox(self, ids, reset):
        if reset:
            self.cv_doc_listbox.delete(0, END)
            self.cv_ids_cache = []

        self.cv_ids_cache.extend(ids)
        for sid, _ in ids:
            self.cv_doc_listbox.insert(END, sid)

    def cv_on_doc_select(self, event=None):
        selection = self.cv_doc_listbox.curselection()
        if not selection:
            return

        _, _id_obj = self.cv_ids_cache[selection[0]]
        collection = self.cv_collection_entry.get().strip()
        if not collection:
            return

        async def _fetch():
            if hasattr(self.zmongo, "find_one_async"):
                res = await self.zmongo.find_one_async(collection, {"_id": _id_obj})
            elif hasattr(self.zmongo, "find_one"):
                maybe = self.zmongo.find_one(collection, {"_id": _id_obj})
                res = await maybe if asyncio.iscoroutine(maybe) else maybe
            else:
                raise AttributeError("Resolved ZMongo object does not expose find_one_async or find_one.")

            if isinstance(res, SafeResult) and res.success:
                pretty = json.dumps(res.data, indent=2, default=str)
                self.notebook.after(
                    0,
                    lambda: (
                        self.cv_json_text.delete("1.0", END),
                        self.cv_json_text.insert(END, pretty),
                    ),
                )
            elif isinstance(res, SafeResult):
                self.log_message(f"Fetch failed: {res.error}")
            else:
                pretty = json.dumps(res, indent=2, default=str)
                self.notebook.after(
                    0,
                    lambda: (
                        self.cv_json_text.delete("1.0", END),
                        self.cv_json_text.insert(END, pretty),
                    ),
                )

        self.run_in_async_loop(_fetch)

    def on_apply_dotkey_value_clicked(self):
        collection = self.cv_collection_entry.get().strip()
        path = self.cv_dotkey_entry.get().strip()
        raw_val = self.cv_value_entry.get().strip()
        selection = self.cv_doc_listbox.curselection()

        if not all([collection, path, selection]):
            self.log_message("Collection, dot-key, and selected document are required.")
            return

        _id_str, _id_obj = self.cv_ids_cache[selection[0]]

        try:
            value = json.loads(raw_val)
        except Exception:
            value = raw_val

        async def _update():
            if hasattr(self.zmongo, "update_one_async"):
                res = await self.zmongo.update_one_async(collection, {"_id": _id_obj}, {"$set": {path: value}})
            elif hasattr(self.zmongo, "update_many_async"):
                res = await self.zmongo.update_many_async(collection, {"_id": _id_obj}, {"$set": {path: value}})
            else:
                raise AttributeError("Resolved ZMongo object does not expose update_one_async or update_many_async.")

            if isinstance(res, SafeResult) and res.success:
                self.log_message(f"Updated {path} for {_id_str}")
                self.cv_on_doc_select()
            elif isinstance(res, SafeResult):
                self.log_message(f"Update failed: {res.error}")
            else:
                self.log_message(f"Update completed for {_id_str}")

        self.run_in_async_loop(_update)

    def on_collection_select(self, event=None):
        selection = self.collection_listbox.curselection()
        if selection:
            collection_name = self.collection_listbox.get(selection[0])
            self.update_backup_files_listbox(collection_name)

    def update_backup_files_listbox(self, collection_name: str):
        self.backup_files_listbox.delete(0, END)
        pattern = re.compile(rf"^{re.escape(collection_name)}\[\d{{14}}\]\.(json|bson|csv)$", re.I)

        try:
            for file_path in self.backup_dir.iterdir():
                if file_path.is_file() and pattern.match(file_path.name):
                    self.backup_files_listbox.insert(END, file_path.name)
        except FileNotFoundError:
            self.log_message(f"Backup directory not found: {self.backup_dir}")

    def on_backup_file_select(self, event=None):
        pass

    def on_backup_selected_clicked(self):
        selection = self.collection_listbox.curselection()
        if not selection:
            self.log_message("No collection selected for backup.")
            return

        collection_name = self.collection_listbox.get(selection[0])
        fmt = self.backup_format_combo.get()
        self.run_in_async_loop(self.backup_collection, collection_name, fmt)

    async def backup_collection(self, collection_name: str, fmt: str):
        try:
            cursor = self.db[collection_name].find({})
            docs = await cursor.to_list(length=None)
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            output_path = self.backup_dir / f"{collection_name}[{timestamp}].{fmt.lower()}"

            if fmt == "JSON":
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(DataProcessor.to_json_compatible(docs), f, indent=2)
            elif fmt == "BSON":
                if not HAVE_BSON_STREAM:
                    raise RuntimeError("BSON streaming not available in this environment.")
                with open(output_path, "wb") as f:
                    for doc in docs:
                        f.write(BSON.encode(doc))
            elif fmt == "CSV":
                rows = [DataProcessor.flatten_json(DataProcessor.to_json_compatible(doc)) for doc in docs]
                headers = sorted({key for row in rows for key in row.keys()})
                with open(output_path, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=headers)
                    writer.writeheader()
                    for row in rows:
                        writer.writerow(row)
            else:
                raise ValueError(f"Unsupported backup format: {fmt}")

            self.log_message(f"Backed up {collection_name} to {output_path.name}")
            self.notebook.after(0, lambda: self.update_backup_files_listbox(collection_name))
        except Exception as e:
            self.log_message(f"Backup Error: {e}")

    def on_restore_clicked(self):
        selection = self.collection_listbox.curselection()
        file_selection = self.backup_files_listbox.curselection()

        if not selection:
            self.log_message("No collection selected for restore.")
            return
        if not file_selection:
            self.log_message("No backup file selected for restore.")
            return

        collection_name = self.collection_listbox.get(selection[0])
        filename = self.backup_files_listbox.get(file_selection[0])

        self.run_in_async_loop(self.restore_collection, collection_name, filename)

    async def restore_collection(self, collection_name: str, filename: str):
        restore_path = self.backup_dir / filename
        if not restore_path.exists():
            self.log_message(f"Restore file not found: {restore_path}")
            return

        try:
            ext = restore_path.suffix.lower()
            docs = []

            if ext == ".json":
                with open(restore_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                docs = data if isinstance(data, list) else [data]

            elif ext == ".bson":
                if not HAVE_BSON_STREAM:
                    raise RuntimeError("BSON restore not supported in this environment.")
                with open(restore_path, "rb") as f:
                    docs = list(decode_file_iter(f))

            elif ext == ".csv":
                with open(restore_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    docs = [self._unflatten_csv_row(row) for row in reader]
            else:
                raise ValueError(f"Unsupported restore file type: {ext}")

            normalized_docs = [self._normalize_doc_ids(doc) for doc in docs]
            if not normalized_docs:
                self.log_message("Restore file contained no documents.")
                return

            await self.db[collection_name].delete_many({})
            await self.db[collection_name].insert_many(normalized_docs)
            self.log_message(f"Restored {len(normalized_docs)} documents into {collection_name}")
        except Exception as e:
            self.log_message(f"Restore Error: {e}")

    def open_file_explorer(self):
        filepath = filedialog.askopenfilename(
            initialdir=str(self.backup_dir),
            title="Select a Backup File",
            filetypes=[
                ("All supported", "*.json *.bson *.csv"),
                ("JSON files", "*.json"),
                ("BSON files", "*.bson"),
                ("CSV files", "*.csv"),
            ],
        )
        if not filepath:
            return

        file_path_obj = Path(filepath)
        filename = file_path_obj.name

        self.backup_files_listbox.selection_clear(0, END)
        existing = list(self.backup_files_listbox.get(0, END))
        if filename not in existing:
            self.backup_files_listbox.insert(END, filename)

        index = list(self.backup_files_listbox.get(0, END)).index(filename)
        self.backup_files_listbox.selection_set(index)
        self.backup_files_listbox.see(index)

    @staticmethod
    def _normalize_doc_ids(doc: dict):
        if isinstance(doc, dict) and "_id" in doc and isinstance(doc["_id"], str) and ObjectId.is_valid(doc["_id"]):
            try:
                doc["_id"] = ObjectId(doc["_id"])
            except errors.InvalidId:
                pass
        return doc

    @staticmethod
    def _unflatten_csv_row(row: dict) -> dict:
        root = {}

        def set_path(container, parts, value):
            if not parts:
                return value

            key = parts[0]
            is_index = key.isdigit()

            if is_index:
                index = int(key)
                if not isinstance(container, list):
                    container = []
                while len(container) <= index:
                    container.append({})
                container[index] = set_path(container[index], parts[1:], value)
                return container

            if not isinstance(container, dict):
                container = {}

            container[key] = set_path(container.get(key, {}), parts[1:], value)
            return container

        for k, v in row.items():
            if v in (None, ""):
                continue
            try:
                parsed = json.loads(v)
            except Exception:
                parsed = v
            root = set_path(root, k.split("."), parsed)

        return root