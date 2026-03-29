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

from bson import errors, BSON, decode_file_iter
from bson.objectid import ObjectId
from bson import json_util
from dotenv import load_dotenv
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import MongoClient, InsertOne, ReplaceOne
from pymongo.errors import BulkWriteError

from preset_api import HAVE_BSON_STREAM
from data_processor import DataProcessor
from safe_result import SafeResult
from zmongo import ZMongo

# --- Local Toolbag Imports ---
# Assuming these exist in your project structure



load_dotenv(Path.home() / '.resources' / '.env')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://127.0.0.1:27017')
MONGO_DATABASE_NAME = os.getenv('MONGO_DATABASE_NAME', 'wiki_kb')
MONGO_BACKUP_DIR_REL = os.getenv('MONGO_BACKUP_DIR', '.resources/mongo_backups')


@dataclass
class Pager:
    limit: int = 100
    skip: int = 0


class ZManager(Tk):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        super().__init__()
        self.zmongo = ZMongo()
        self.title('ZMongo System Manager')
        self.geometry('1600x900')
        self.loop = loop
        self.db_name = MONGO_DATABASE_NAME
        self.backup_dir = Path.home() / MONGO_BACKUP_DIR_REL / MONGO_DATABASE_NAME
        self.make_dir_if_not_exists(self.backup_dir)

        try:
            self.async_client = AsyncIOMotorClient(MONGO_URI)
            self.db = self.async_client[self.db_name]
            self.sync_client = MongoClient(MONGO_URI)
            self.sync_db = self.sync_client[self.db_name]
        except Exception as e:
            logging.error(f'Failed to connect to MongoDB: {e}')
            self.destroy()
            return

        self.cv_pager = Pager(limit=100, skip=0)
        self.cv_ids_cache = []
        self._create_widgets()
        self.run_periodic_updates()

    # --- UI & LOGGING HELPERS ---

    def log_message(self, message):
        """Thread-safe logging to the UI message box."""

        def _append():
            self.message_text.config(state='normal')
            self.message_text.insert('end', f"{datetime.now().strftime('%H:%M:%S')} - {message}\n")
            self.message_text.config(state='disabled')
            self.message_text.see('end')

        self.after(0, _append)

    def run_in_async_loop(self, async_func, *args, **kwargs):
        """Schedules an async function to run in the background loop."""
        future = asyncio.run_coroutine_threadsafe(async_func(*args, **kwargs), self.loop)
        future.add_done_callback(self.on_async_task_done)
        return future

    def on_async_task_done(self, future):
        try:
            future.result()
        except Exception as e:
            logging.error(f'Async task failed: {e}')
            self.log_message(f'Error: {e}')

    # --- WIDGET BUILDING ---

    def _create_widgets(self):
        main_notebook = ttk.Notebook(self)
        main_notebook.pack(expand=True, fill=BOTH, padx=10, pady=10)

        db_info_tab = ttk.Frame(main_notebook)
        maintenance_tab = ttk.Frame(main_notebook)
        collection_tab = ttk.Frame(main_notebook)
        system_tab = ttk.Frame(main_notebook)

        main_notebook.add(db_info_tab, text='Database Info')
        main_notebook.add(maintenance_tab, text='Backup & Restore')
        main_notebook.add(collection_tab, text='Collection Viewer / Editor')
        main_notebook.add(system_tab, text='System Runner')

        self.db_info_text = ScrolledText(db_info_tab, wrap='word', font=('Courier New', 10))
        self.db_info_text.pack(expand=True, fill=BOTH, padx=5, pady=5)

        self._build_maintenance_tab(maintenance_tab)
        self._build_collection_tab(collection_tab)

        st = ScrolledText(system_tab, height=8, wrap='word')
        st.pack(expand=True, fill=BOTH, padx=8, pady=8)
        st.insert('end', 'System Runner: (reserve for future services)\n')

    def _build_maintenance_tab(self, parent: Frame):
        root = ttk.Frame(parent)
        root.pack(fill=BOTH, expand=True, padx=8, pady=8)
        root.grid_columnconfigure(1, weight=1)
        root.grid_columnconfigure(3, weight=1)
        root.grid_rowconfigure(5, weight=1)

        ttk.Label(root, text='Collections:').grid(row=0, column=0, sticky='w', padx=5)
        listbox_frame = ttk.Frame(root)
        listbox_frame.grid(row=1, column=0, rowspan=4, sticky='nswe', padx=5)
        self.collection_listbox = Listbox(listbox_frame, exportselection=False, height=12)
        self.collection_listbox.pack(side=LEFT, fill=BOTH, expand=True)
        sb1 = ttk.Scrollbar(listbox_frame, orient='vertical', command=self.collection_listbox.yview)
        sb1.pack(side=RIGHT, fill=Y)
        self.collection_listbox.config(yscrollcommand=sb1.set)
        self.collection_listbox.bind('<<ListboxSelect>>', self.on_collection_select)

        ttk.Label(root, text='Selected Collection:').grid(row=0, column=1, sticky='w', padx=5)
        self.selected_collection_entry = Entry(root, state='readonly')
        self.selected_collection_entry.grid(row=1, column=1, sticky='we', padx=5)

        ttk.Label(root, text='Backup Files:').grid(row=0, column=2, sticky='w', padx=5)
        backup_frame = ttk.Frame(root)
        backup_frame.grid(row=1, column=2, rowspan=4, sticky='nswe', padx=5)
        self.backup_files_listbox = Listbox(backup_frame, exportselection=False, height=12)
        self.backup_files_listbox.pack(side=LEFT, fill=BOTH, expand=True)
        sb2 = ttk.Scrollbar(backup_frame, orient='vertical', command=self.backup_files_listbox.yview)
        sb2.pack(side=RIGHT, fill=Y)
        self.backup_files_listbox.config(yscrollcommand=sb2.set)
        self.backup_files_listbox.bind('<<ListboxSelect>>', self.on_backup_file_select)

        ttk.Label(root, text='Selected Backup File:').grid(row=0, column=3, sticky='w', padx=5)
        self.selected_backup_entry = Entry(root, state='readonly')
        self.selected_backup_entry.grid(row=1, column=3, sticky='we', padx=5)

        actions = ttk.Frame(root)
        actions.grid(row=2, column=1, columnspan=3, sticky='we', pady=10)
        ttk.Label(actions, text='Backup format:').pack(side=LEFT, padx=(0, 6))
        self.backup_format_combo = ttk.Combobox(actions, state='readonly', values=['JSON', 'BSON', 'CSV'], width=8)
        self.backup_format_combo.current(0)
        self.backup_format_combo.pack(side=LEFT, padx=(0, 12))

        Button(actions, text='Backup Selected', command=self.on_backup_selected_clicked).pack(side=LEFT, padx=5)
        Button(actions, text='Backup All', command=self.on_backup_all_clicked).pack(side=LEFT, padx=5)
        Button(actions, text='Restore Selected', command=self.on_restore_clicked).pack(side=LEFT, padx=5)
        Button(actions, text='Browse for File...', command=self.open_file_explorer).pack(side=LEFT, padx=5)

        self.restore_options = ttk.Combobox(root, state='readonly', values=['Merge (Upsert)', 'Replace'])
        self.restore_options.current(0)
        self.restore_options.grid(row=3, column=1, columnspan=3, sticky='we', padx=5)

        self.message_text = ScrolledText(root, height=8, wrap='word', state='disabled')
        self.message_text.grid(row=5, column=0, columnspan=4, sticky='nswe', padx=5, pady=5)

    def _build_collection_tab(self, parent: Frame):
        root = ttk.Frame(parent)
        root.pack(fill=BOTH, expand=True, padx=8, pady=8)
        root.grid_columnconfigure(0, weight=0)
        root.grid_columnconfigure(1, weight=1)
        root.grid_rowconfigure(2, weight=1)

        ttk.Label(root, text='Collection:').grid(row=0, column=0, sticky='w')
        self.cv_collection_entry = Entry(root, width=40)
        self.cv_collection_entry.grid(row=0, column=1, sticky='w', padx=6)

        ttk.Label(root, text='Filter (JSON):').grid(row=1, column=0, sticky='w')
        self.cv_filter_entry = Entry(root)
        self.cv_filter_entry.insert(0, '{}')
        self.cv_filter_entry.grid(row=1, column=1, sticky='we', padx=6)

        ctrl = ttk.Frame(root)
        ctrl.grid(row=0, column=2, rowspan=2, sticky='ne')
        Button(ctrl, text='Use Selected', command=self._prefill_collection_from_tab2).pack(side=TOP, padx=4, pady=2)
        Button(ctrl, text='Refresh', command=self.cv_refresh_docs_clicked).pack(side=TOP, padx=4, pady=2)
        Button(ctrl, text='Load More', command=self.cv_load_more_clicked).pack(side=TOP, padx=4, pady=2)

        left = ttk.Frame(root, borderwidth=1, relief='groove')
        left.grid(row=2, column=0, sticky='ns')
        left.grid_rowconfigure(1, weight=1)

        ttk.Label(left, text='Documents').grid(row=0, column=0, sticky='we')
        doc_list_frame = ttk.Frame(left)
        doc_list_frame.grid(row=1, column=0, sticky='ns')
        self.cv_doc_listbox = Listbox(doc_list_frame, height=30, width=36, exportselection=False)
        self.cv_doc_listbox.pack(side=LEFT, fill=Y, expand=False)
        sb_docs = ttk.Scrollbar(doc_list_frame, orient='vertical', command=self.cv_doc_listbox.yview)
        sb_docs.pack(side=RIGHT, fill=Y)
        self.cv_doc_listbox.config(yscrollcommand=sb_docs.set)
        self.cv_doc_listbox.bind('<<ListboxSelect>>', self.cv_on_doc_select)

        actions = ttk.Frame(left)
        actions.grid(row=2, column=0, sticky='we', pady=(8, 0))
        Button(actions, text='Insert Doc', command=self.cv_insert_doc_dialog).pack(side=LEFT, padx=4)
        Button(actions, text='Delete Selected', command=self.cv_delete_selected).pack(side=LEFT, padx=4)

        right = ttk.Frame(root, borderwidth=1, relief='groove')
        right.grid(row=2, column=1, columnspan=2, sticky='nswe', padx=(8, 0))
        right.grid_columnconfigure(1, weight=1)
        right.grid_rowconfigure(1, weight=1)

        ttk.Label(right, text='Selected Document JSON').grid(row=0, column=0, columnspan=2, sticky='w', pady=(4, 0))
        self.cv_json_text = ScrolledText(right, font=('Courier New', 10), wrap='none')
        self.cv_json_text.grid(row=1, column=0, columnspan=2, sticky='nswe', padx=6, pady=6)

        editor = ttk.LabelFrame(right, text='Dot-Key Update (single doc)')
        editor.grid(row=2, column=0, columnspan=2, sticky='we', padx=6, pady=(0, 8))
        editor.grid_columnconfigure(1, weight=1)

        ttk.Label(editor, text='Document _id:').grid(row=0, column=0, sticky='e', padx=5, pady=5)
        self.cv_id_entry = Entry(editor)
        self.cv_id_entry.grid(row=0, column=1, sticky='we', padx=5, pady=5)
        Button(editor, text='Use Selected ID', command=self._use_selected_id).grid(row=0, column=2, padx=5, pady=5)

        ttk.Label(editor, text='Dot-key:').grid(row=1, column=0, sticky='e', padx=5, pady=5)
        self.cv_dotkey_entry = Entry(editor)
        self.cv_dotkey_entry.grid(row=1, column=1, sticky='we', padx=5, pady=5)

        ttk.Label(editor, text='Value (JSON/Text):').grid(row=2, column=0, sticky='e', padx=5, pady=5)
        self.cv_value_entry = Entry(editor)
        self.cv_value_entry.grid(row=2, column=1, sticky='we', padx=5, pady=5)
        Button(editor, text='Apply $set', command=self.on_apply_dotkey_value_clicked).grid(row=3, column=0,
                                                                                           columnspan=3, sticky='we',
                                                                                           padx=6, pady=(6, 8))

    # --- TAB 1: DB INFO ASYNC ---

    async def _fetch_and_update_db_info_task(self):
        try:
            collections = await self.db.list_collection_names()
            db_stats = await self.db.command('dbstats')
            info_lines = [
                f'Database: {self.db_name}',
                f"Collections ({db_stats.get('collections', 0)}):",
                '--------------------',
                *sorted(collections),
                '\n--- DB Stats ---',
                f"Objects: {db_stats.get('objects', 'N/A')}",
                f"Data Size: {db_stats.get('dataSize', 0) / 1024 ** 2:.2f} MB",
                f"Storage Size: {db_stats.get('storageSize', 0) / 1024 ** 2:.2f} MB",
                f"Index Size: {db_stats.get('indexSize', 0) / 1024 ** 2:.2f} MB"
            ]
            info_str = '\n'.join(info_lines)
            self.after(0, lambda: self._update_db_info_ui(info_str))
        except Exception as e:
            self.log_message(f'Error fetching DB info: {e}')

    def _update_db_info_ui(self, info_str):
        self.db_info_text.delete('1.0', 'end')
        self.db_info_text.insert('end', info_str)

    async def _fetch_collections_task(self):
        try:
            names = await self.db.list_collection_names()
            self.after(0, lambda: self._update_collection_listbox_ui(sorted(names)))
        except Exception as e:
            logging.error(f'Failed to fetch collections: {e}')

    def _update_collection_listbox_ui(self, names):
        current_selection = self.collection_listbox.curselection()
        self.collection_listbox.delete(0, 'end')
        for name in names:
            self.collection_listbox.insert('end', name)
        if current_selection:
            try:
                self.collection_listbox.selection_set(current_selection)
            except:
                pass

    # --- TAB 2: BACKUP & RESTORE ---

    def on_collection_select(self, event=None):
        selection = self.collection_listbox.curselection()
        if not selection: return
        name = self.collection_listbox.get(selection[0])
        self.selected_collection_entry.config(state='normal')
        self.selected_collection_entry.delete(0, 'end')
        self.selected_collection_entry.insert(0, name)
        self.selected_collection_entry.config(state='readonly')
        self.update_backup_files_listbox(name)

    def on_backup_selected_clicked(self):
        coll = self.selected_collection_entry.get().strip()
        if not coll: return
        fmt = self.backup_format_combo.get()
        self.log_message(f"Starting {fmt} backup for '{coll}'...")
        self.run_in_async_loop(self.backup_collection_task, coll, fmt)

    def on_backup_all_clicked(self):
        fmt = self.backup_format_combo.get()
        self.log_message(f'Starting {fmt} backup for all collections...')
        self.run_in_async_loop(self.backup_all_collections_task, fmt)

    def on_restore_clicked(self):
        coll = self.selected_collection_entry.get().strip()
        file_path = self.selected_backup_entry.get().strip()
        mode = self.restore_options.get()
        if not coll or not file_path:
            self.log_message('Error: Need collection and backup file.')
            return
        self.run_in_async_loop(self.restore_from_backup_task, coll, file_path, mode)

    async def backup_collection_task(self, collection_name: str, fmt: str):
        collection = self.db[collection_name]
        docs = await collection.find({}).to_list(length=None)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        ext = fmt.lower()
        backup_file = self.backup_dir / f'{collection_name}[{timestamp}].{ext}'

        if fmt == 'JSON':
            with open(backup_file, 'w', encoding='utf-8') as f:
                f.write(json_util.dumps(docs, indent=2))
        elif fmt == 'BSON':
            if not HAVE_BSON_STREAM:
                self.log_message("BSON library missing. Use JSON instead.")
                return
            with open(backup_file, 'wb') as f:
                for d in docs: f.write(BSON.encode(d))
        elif fmt == 'CSV':
            flat_rows, headers = ([], set())
            for d in docs:
                flat = DataProcessor.flatten_json(d)
                flat_rows.append(flat)
                headers.update(flat.keys())
            headers = sorted(list(headers))
            with open(backup_file, 'w', encoding='utf-8', newline='') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                for row in flat_rows:
                    writer.writerow({k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in row.items()})

        self.log_message(f"Backup complete: {backup_file.name}")
        self.after(0, lambda: self.update_backup_files_listbox(collection_name))

    async def backup_all_collections_task(self, fmt: str):
        collections = await self.db.list_collection_names()
        for name in collections:
            await self.backup_collection_task(name, fmt)
        self.log_message('Finished backing up all collections.')

    async def restore_from_backup_task(self, collection_name: str, filename: str, mode: str):
        p = Path(filename)
        if not p.is_absolute(): p = (self.backup_dir / filename).resolve()
        if not p.exists(): return

        docs = []
        ext = p.suffix.lower()
        if ext == '.json':
            with open(p, 'r', encoding='utf-8') as f:
                data = json_util.loads(f.read())
                docs = list(data) if isinstance(data, list) else [data]
        elif ext == '.bson' and HAVE_BSON_STREAM:
            with open(p, 'rb') as f:
                docs = list(decode_file_iter(f))
        elif ext == '.csv':
            with open(p, 'r', encoding='utf-8', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Logic to unflatten keys like 'a.b.c' back into nested dicts
                    docs.append(self._unflatten_from_csv(row))

        coll = self.db[collection_name]
        if mode.startswith('Replace'):
            await coll.delete_many({})
            if docs: await coll.insert_many(docs)
        else:
            ops = [ReplaceOne({'_id': d['_id']}, d, upsert=True) if '_id' in d else InsertOne(d) for d in docs]
            if ops: await coll.bulk_write(ops, ordered=False)

        self.log_message(f"Restore finished for '{collection_name}'.")

    def _unflatten_from_csv(self, row: dict) -> dict:
        """Helper to reconstruct a nested dictionary from a flat CSV row."""
        result = {}
        for key, value in row.items():
            if value is None or value == "": continue
            # Try parsing JSON for nested lists/dicts
            try:
                parsed_val = json.loads(value)
            except:
                parsed_val = value

            parts = key.split('.')
            curr = result
            for part in parts[:-1]:
                if part not in curr: curr[part] = {}
                curr = curr[part]
            curr[parts[-1]] = parsed_val
        return result

    # --- TAB 3: COLLECTION VIEWER ---

    def cv_refresh_docs_clicked(self):
        self.cv_pager.skip = 0
        self.run_in_async_loop(self.cv_refresh_docs_task, reset=True)

    def cv_load_more_clicked(self):
        self.cv_pager.skip += self.cv_pager.limit
        self.run_in_async_loop(self.cv_refresh_docs_task, reset=False)

    async def cv_refresh_docs_task(self, reset: bool):
        collection = (self.cv_collection_entry.get() or '').strip()
        if not collection: return
        filt_text = (self.cv_filter_entry.get() or '{}').strip()
        try:
            filt = json.loads(filt_text)
        except:
            filt = {}

        cursor = self.db[collection].find(filt, projection={'_id': 1}).skip(self.cv_pager.skip).limit(
            self.cv_pager.limit)
        items = await cursor.to_list(length=self.cv_pager.limit)
        pairs = [(str(it.get('_id')), it.get('_id')) for it in items]
        self.after(0, lambda: self._cv_update_ids_list_ui(pairs, reset))

    def _cv_update_ids_list_ui(self, pairs, reset):
        if reset:
            self.cv_doc_listbox.delete(0, END)
            self.cv_ids_cache = []
        self.cv_ids_cache.extend(pairs)
        for sid, _ in pairs:
            self.cv_doc_listbox.insert(END, sid)

    def cv_on_doc_select(self, event=None):
        idxs = self.cv_doc_listbox.curselection()
        if not idxs: return
        _, _id_obj = self.cv_ids_cache[idxs[0]]
        collection = (self.cv_collection_entry.get() or '').strip()
        self.run_in_async_loop(self.cv_fetch_single_doc_task, collection, _id_obj)

    async def cv_fetch_single_doc_task(self, collection, id_obj):
        res = await self.zmongo.find_one(collection, {'_id': id_obj})
        self.after(0, lambda: self._cv_display_doc_ui(res))

    def _cv_display_doc_ui(self, res: SafeResult):
        self.cv_json_text.delete('1.0', END)
        if not res.success or not res.data:
            self.cv_json_text.insert('end', "Document not found.")
            return
        pretty = json_util.dumps(res.data, indent=2)
        self.cv_json_text.insert('end', pretty)
        self.cv_id_entry.delete(0, END)
        self.cv_id_entry.insert(0, str(res.data.get('_id')))

    def cv_insert_doc_dialog(self):
        win = Toplevel(self)
        win.title('Insert Document')
        txt = ScrolledText(win, font=('Courier New', 10))
        txt.pack(fill=BOTH, expand=True, padx=8, pady=8)
        txt.insert('end', '{\n  \n}')

        def _submit():
            try:
                doc = json.loads(txt.get('1.0', END))
                coll = self.cv_collection_entry.get().strip()
                self.run_in_async_loop(self.cv_insert_task, coll, doc, win)
            except Exception as e:
                self.log_message(f"Insert JSON error: {e}")

        Button(win, text='Insert', command=_submit).pack(pady=5)

    async def cv_insert_task(self, coll, doc, window):
        res = await self.zmongo.insert_document(coll, doc)
        if res.success:
            self.log_message("Document inserted.")
            self.after(0, window.destroy)
            await self.cv_refresh_docs_task(reset=True)

    def cv_delete_selected(self):
        idxs = self.cv_doc_listbox.curselection()
        if not idxs: return
        sid, _id_obj = self.cv_ids_cache[idxs[0]]
        coll = self.cv_collection_entry.get().strip()
        self.run_in_async_loop(self.cv_delete_task, coll, _id_obj, idxs[0])

    async def cv_delete_task(self, coll, id_obj, index):
        res = await self.zmongo.delete_document(coll, {'_id': id_obj})
        if res.success:
            self.log_message(f"Deleted {id_obj}")
            self.after(0, lambda: self._cv_remove_from_list_ui(index))

    def _cv_remove_from_list_ui(self, index):
        self.cv_doc_listbox.delete(index)
        del self.cv_ids_cache[index]

    def on_apply_dotkey_value_clicked(self):
        coll = self.cv_collection_entry.get().strip()
        key = self.cv_dotkey_entry.get().strip()
        val = self._parse_input_value(self.cv_value_entry.get())
        raw_id = self.cv_id_entry.get().strip()
        if not all([coll, key, raw_id]): return
        q = {'_id': ObjectId(raw_id) if ObjectId.is_valid(raw_id) else raw_id}
        self.run_in_async_loop(self.cv_update_task, coll, q, key, val)

    async def cv_update_task(self, coll, query, key, val):
        res = await self.zmongo.update_document(coll, query, {'$set': {key: val}})
        if res.success:
            self.log_message("Update successful.")
            await self.cv_fetch_single_doc_task(coll, query['_id'])

    # --- MISC UTILS ---

    @staticmethod
    def _parse_input_value(raw: str):
        s = (raw or '').strip()
        try:
            return json.loads(s)
        except:
            return s

    def update_backup_files_listbox(self, collection_name: str):
        self.backup_files_listbox.delete(0, 'end')
        try:
            pattern = re.compile(f'^{re.escape(collection_name)}\\[\\d{{14}}\\]\\.(json|bson|csv)$', re.I)
            if self.backup_dir.exists():
                for file_path in self.backup_dir.iterdir():
                    if file_path.is_file() and pattern.match(file_path.name):
                        self.backup_files_listbox.insert('end', file_path.name)
        except Exception as e:
            self.log_message(f'Error listing backup files: {e}')

    def on_backup_file_select(self, event=None):
        selection = self.backup_files_listbox.curselection()
        if not selection: return
        filename = self.backup_files_listbox.get(selection[0])
        self.selected_backup_entry.config(state='normal')
        self.selected_backup_entry.delete(0, 'end')
        self.selected_backup_entry.insert(0, filename)
        self.selected_backup_entry.config(state='readonly')

    def open_file_explorer(self):
        fp = filedialog.askopenfilename(initialdir=str(self.backup_dir))
        if fp:
            self.selected_backup_entry.config(state='normal')
            self.selected_backup_entry.delete(0, END)
            self.selected_backup_entry.insert(0, fp)
            self.selected_backup_entry.config(state='readonly')

    def _prefill_collection_from_tab2(self):
        name = self.selected_collection_entry.get().strip()
        if name:
            self.cv_collection_entry.delete(0, END)
            self.cv_collection_entry.insert(0, name)

    def _use_selected_id(self):
        idxs = self.cv_doc_listbox.curselection()
        if idxs:
            sid, _ = self.cv_ids_cache[idxs[0]]
            self.cv_id_entry.delete(0, END)
            self.cv_id_entry.insert(0, sid)

    @staticmethod
    def make_dir_if_not_exists(directory: Path):
        directory.mkdir(parents=True, exist_ok=True)

    def run_periodic_updates(self):
        self.run_in_async_loop(self._fetch_and_update_db_info_task)
        self.run_in_async_loop(self._fetch_collections_task)
        self.after(30000, self.run_periodic_updates)

    def on_closing(self):
        """Cleanup logic when the window is closed."""
        self.async_client.close()
        self.sync_client.close()
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.destroy()


def main():
    loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=loop.run_forever, daemon=True)
    loop_thread.start()
    app = ZManager(loop)
    app.protocol('WM_DELETE_WINDOW', app.on_closing)
    app.mainloop()


if __name__ == '__main__':
    main()