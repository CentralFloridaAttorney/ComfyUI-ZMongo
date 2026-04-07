import { app } from "../../scripts/app.js";

const STORAGE_KEYS = {
  remoteBaseUrl: "zmongo.remoteBaseUrl",
  remoteUsername: "zmongo.remoteUsername",
  remoteToken: "zmongo.remoteToken",
  localExpanded: "zmongo.localExpanded",
  remoteExpanded: "zmongo.remoteExpanded",
};

const DEFAULTS = {
  remoteBaseUrl: "https://ztarot.app",
  localApiBase: "/zai/zmongo",
};

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function safeJsonParse(text, fallback = null) {
  try {
    return JSON.parse(text);
  } catch {
    return fallback;
  }
}

function prettyJson(value) {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function extractObjectId(value) {
  if (!value) return "";
  if (typeof value === "string") return value;
  if (typeof value === "object") {
    if (typeof value.$oid === "string") return value.$oid;
    if (typeof value._id === "string") return value._id;
  }
  return String(value);
}

function flattenObject(obj, prefix = "", out = {}) {
  if (obj === null || obj === undefined) {
    if (prefix) out[prefix] = obj;
    return out;
  }

  if (Array.isArray(obj)) {
    if (!prefix && obj.length === 0) return out;
    obj.forEach((item, index) => {
      const next = prefix ? `${prefix}.${index}` : `${index}`;
      flattenObject(item, next, out);
    });
    if (prefix && obj.length === 0) out[prefix] = [];
    return out;
  }

  if (typeof obj === "object") {
    const keys = Object.keys(obj);
    if (prefix && keys.length === 0) {
      out[prefix] = {};
      return out;
    }
    for (const key of keys) {
      const next = prefix ? `${prefix}.${key}` : key;
      flattenObject(obj[key], next, out);
    }
    return out;
  }

  if (prefix) out[prefix] = obj;
  return out;
}

function summarizeDoc(doc) {
  if (!doc || typeof doc !== "object") return "";
  const keys = Object.keys(doc).filter((k) => k !== "_id").slice(0, 6);
  return keys.length ? keys.join(", ") : "<only _id>";
}

class ZMongoManagerPanel {
  constructor() {
    this.state = {
      tab: "local",
      local: {
        collections: [],
        selectedCollection: "",
        docs: [],
        selectedDocId: "",
        selectedDoc: null,
        health: null,
      },
      remote: {
        baseUrl: localStorage.getItem(STORAGE_KEYS.remoteBaseUrl) || DEFAULTS.remoteBaseUrl,
        username: localStorage.getItem(STORAGE_KEYS.remoteUsername) || "",
        token: localStorage.getItem(STORAGE_KEYS.remoteToken) || "",
        collections: [],
        selectedCollection: "",
        docs: [],
        selectedDocId: "",
        selectedDoc: null,
        verifiedUser: "",
      },
    };

    this.root = null;
    this.backdrop = null;
    this.statusBox = null;
    this.refs = {};
  }

  mount() {
    this.injectStyles();
    this.createLauncher();
    this.createPanel();
  }

  injectStyles() {
    if (document.getElementById("zmongo-manager-panel-styles")) return;

    const style = document.createElement("style");
    style.id = "zmongo-manager-panel-styles";
    style.textContent = `
      .zmongo-launcher {
        position: fixed;
        right: 20px;
        bottom: 20px;
        z-index: 100001;
        background: #2f7dd6;
        color: #fff;
        border: 1px solid #2f7dd6;
        border-radius: 999px;
        padding: 10px 16px;
        font-weight: 600;
        cursor: pointer;
        box-shadow: 0 8px 24px rgba(0,0,0,0.35);
      }
      .zmongo-launcher:hover { background: #4ea1ff; border-color: #4ea1ff; }

      .zmongo-backdrop {
        position: fixed;
        inset: 0;
        background: rgba(0,0,0,0.45);
        z-index: 100000;
        display: none;
      }

      .zmongo-panel {
        position: fixed;
        inset: 4vh 4vw;
        z-index: 100002;
        display: none;
        background: #0f1115;
        color: #e8edf7;
        border: 1px solid #2d3647;
        border-radius: 14px;
        overflow: hidden;
        box-shadow: 0 18px 48px rgba(0,0,0,0.45);
        font-family: Arial, Helvetica, sans-serif;
      }

      .zmongo-panel * { box-sizing: border-box; }

      .zmongo-panel-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: 14px 18px;
        background: #171a21;
        border-bottom: 1px solid #2d3647;
      }

      .zmongo-panel-title {
        display: flex;
        align-items: center;
        gap: 14px;
      }

      .zmongo-panel-title h2 {
        margin: 0;
        font-size: 18px;
      }

      .zmongo-tabs {
        display: flex;
        gap: 8px;
      }

      .zmongo-tab {
        background: #273142;
        color: #e8edf7;
        border: 1px solid #334155;
        border-radius: 8px;
        padding: 8px 12px;
        cursor: pointer;
      }

      .zmongo-tab.active {
        background: #2f7dd6;
        border-color: #2f7dd6;
      }

      .zmongo-close-btn {
        background: #6b2525;
        color: #fff;
        border: 1px solid #8f3030;
        border-radius: 8px;
        padding: 8px 12px;
        cursor: pointer;
      }

      .zmongo-status {
        margin: 12px 18px 0 18px;
        padding: 10px 12px;
        border-radius: 8px;
        background: #101722;
        border: 1px solid #2d3647;
        font-size: 13px;
      }

      .zmongo-status.ok { color: #8ee0ad; border-color: rgba(57,179,107,0.35); }
      .zmongo-status.error { color: #ffaaa7; border-color: rgba(217,83,79,0.35); }
      .zmongo-status.warn { color: #f4d58b; border-color: rgba(217,164,65,0.35); }

      .zmongo-panel-body {
        display: grid;
        grid-template-columns: 320px 1fr;
        gap: 16px;
        padding: 16px 18px 18px 18px;
        height: calc(100% - 76px);
      }

      .zmongo-sidebar,
      .zmongo-main {
        display: flex;
        flex-direction: column;
        gap: 16px;
        min-height: 0;
      }

      .zmongo-card {
        background: #171a21;
        border: 1px solid #2d3647;
        border-radius: 12px;
        overflow: hidden;
        min-height: 0;
      }

      .zmongo-card-header {
        padding: 12px 14px;
        background: #1e2430;
        border-bottom: 1px solid #2d3647;
        display: flex;
        align-items: center;
        justify-content: space-between;
        gap: 12px;
      }

      .zmongo-card-header h3 {
        margin: 0;
        font-size: 15px;
      }

      .zmongo-card-body {
        padding: 14px;
      }

      .zmongo-field {
        margin-bottom: 12px;
      }

      .zmongo-field label {
        display: block;
        margin-bottom: 6px;
        font-size: 12px;
        color: #9aa7bd;
      }

      .zmongo-row {
        display: flex;
        gap: 8px;
        align-items: center;
      }

      .zmongo-input,
      .zmongo-select,
      .zmongo-textarea,
      .zmongo-button {
        width: 100%;
        background: #11161e;
        color: #e8edf7;
        border: 1px solid #2d3647;
        border-radius: 8px;
        padding: 10px 12px;
        font-size: 13px;
      }

      .zmongo-textarea {
        min-height: 300px;
        resize: vertical;
        font-family: Consolas, Monaco, monospace;
        line-height: 1.45;
      }

      .zmongo-button {
        width: auto;
        cursor: pointer;
        background: #2f7dd6;
        border-color: #2f7dd6;
      }

      .zmongo-button.secondary {
        background: #273142;
        border-color: #334155;
      }

      .zmongo-button.danger {
        background: #6b2525;
        border-color: #8f3030;
      }

      .zmongo-button:disabled {
        opacity: 0.6;
        cursor: not-allowed;
      }

      .zmongo-list {
        display: flex;
        flex-direction: column;
        gap: 6px;
        max-height: 320px;
        overflow: auto;
      }

      .zmongo-list-item {
        padding: 10px 12px;
        border: 1px solid #2d3647;
        border-radius: 8px;
        background: #10151d;
        cursor: pointer;
      }

      .zmongo-list-item.active {
        border-color: #4ea1ff;
        background: #162538;
      }

      .zmongo-doc-list {
        border: 1px solid #2d3647;
        border-radius: 10px;
        max-height: 260px;
        overflow: auto;
        background: #10151d;
      }

      .zmongo-doc-item {
        padding: 10px 12px;
        border-bottom: 1px solid #232c3b;
        cursor: pointer;
      }

      .zmongo-doc-item:last-child { border-bottom: none; }
      .zmongo-doc-item.active { background: #162538; }

      .zmongo-doc-id {
        font-weight: 700;
        font-size: 12px;
        margin-bottom: 4px;
        word-break: break-all;
      }

      .zmongo-doc-summary {
        font-size: 11px;
        color: #9aa7bd;
      }

      .zmongo-grid-2 {
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 12px;
      }

      .zmongo-hidden { display: none !important; }

      @media (max-width: 1200px) {
        .zmongo-panel-body {
          grid-template-columns: 1fr;
        }
      }
    `;
    document.head.appendChild(style);
  }

  createLauncher() {
    if (document.getElementById("zmongo-launcher")) return;

    const button = document.createElement("button");
    button.id = "zmongo-launcher";
    button.className = "zmongo-launcher";
    button.textContent = "ZMongo";
    button.addEventListener("click", () => this.open());
    document.body.appendChild(button);
  }

  createPanel() {
    this.backdrop = document.createElement("div");
    this.backdrop.className = "zmongo-backdrop";
    this.backdrop.addEventListener("click", () => this.close());

    this.root = document.createElement("div");
    this.root.className = "zmongo-panel";

    this.root.innerHTML = `
      <div class="zmongo-panel-header">
        <div class="zmongo-panel-title">
          <h2>ZMongo Manager</h2>
          <div class="zmongo-tabs">
            <button class="zmongo-tab active" data-tab="local">Local</button>
            <button class="zmongo-tab" data-tab="remote">Remote</button>
          </div>
        </div>
        <button class="zmongo-close-btn">Close</button>
      </div>

      <div id="zmongo-status" class="zmongo-status">Ready.</div>

      <div class="zmongo-panel-body">
        <div class="zmongo-sidebar">
          <div class="zmongo-card" id="zmongo-local-sidebar">
            <div class="zmongo-card-header">
              <h3>Local Connection</h3>
              <button class="zmongo-button secondary" id="zmongo-local-health">Health</button>
            </div>
            <div class="zmongo-card-body">
              <div class="zmongo-field">
                <label>Filter Collections</label>
                <input class="zmongo-input" id="zmongo-local-filter" type="text" placeholder="Type to filter">
              </div>
              <div class="zmongo-field">
                <label>New Collection</label>
                <input class="zmongo-input" id="zmongo-local-new-collection" type="text" placeholder="example_collection">
              </div>
              <div class="zmongo-row" style="margin-bottom:12px;">
                <button class="zmongo-button" id="zmongo-local-refresh">Refresh</button>
                <button class="zmongo-button secondary" id="zmongo-local-create-collection">Create</button>
                <button class="zmongo-button danger" id="zmongo-local-delete-collection">Delete</button>
              </div>
              <div id="zmongo-local-collections" class="zmongo-list"></div>
            </div>
          </div>

          <div class="zmongo-card zmongo-hidden" id="zmongo-remote-sidebar">
            <div class="zmongo-card-header">
              <h3>Remote Connection</h3>
              <button class="zmongo-button secondary" id="zmongo-remote-verify">Verify</button>
            </div>
            <div class="zmongo-card-body">
              <div class="zmongo-field">
                <label>Base URL</label>
                <input class="zmongo-input" id="zmongo-remote-base-url" type="text" placeholder="https://ztarot.app">
              </div>
              <div class="zmongo-field">
                <label>Username</label>
                <input class="zmongo-input" id="zmongo-remote-username" type="text" placeholder="username">
              </div>
              <div class="zmongo-field">
                <label>Password</label>
                <input class="zmongo-input" id="zmongo-remote-password" type="password" placeholder="password">
              </div>
              <div class="zmongo-row" style="margin-bottom:12px;">
                <button class="zmongo-button" id="zmongo-remote-login">Login</button>
                <button class="zmongo-button secondary" id="zmongo-remote-refresh">Refresh</button>
                <button class="zmongo-button danger" id="zmongo-remote-logout">Logout</button>
              </div>
              <div class="zmongo-field">
                <label>Filter Collections</label>
                <input class="zmongo-input" id="zmongo-remote-filter" type="text" placeholder="Type to filter">
              </div>
              <div class="zmongo-field">
                <label>New Collection</label>
                <input class="zmongo-input" id="zmongo-remote-new-collection" type="text" placeholder="example_collection">
              </div>
              <div class="zmongo-row" style="margin-bottom:12px;">
                <button class="zmongo-button secondary" id="zmongo-remote-create-collection">Create</button>
                <button class="zmongo-button danger" id="zmongo-remote-delete-collection">Delete</button>
              </div>
              <div id="zmongo-remote-collections" class="zmongo-list"></div>
            </div>
          </div>
        </div>

        <div class="zmongo-main">
          <div class="zmongo-card">
            <div class="zmongo-card-header">
              <h3>Collection Viewer</h3>
              <div class="zmongo-row">
                <button class="zmongo-button" id="zmongo-load-docs">Load Docs</button>
                <button class="zmongo-button secondary" id="zmongo-new-doc">New</button>
                <button class="zmongo-button" id="zmongo-save-doc">Save JSON</button>
                <button class="zmongo-button danger" id="zmongo-delete-doc">Delete</button>
              </div>
            </div>
            <div class="zmongo-card-body">
              <div class="zmongo-grid-2">
                <div>
                  <div class="zmongo-field">
                    <label>Selected Collection</label>
                    <input class="zmongo-input" id="zmongo-selected-collection" type="text" readonly>
                  </div>
                  <div class="zmongo-grid-2">
                    <div class="zmongo-field">
                      <label>Limit</label>
                      <input class="zmongo-input" id="zmongo-limit" type="number" value="50" min="1" max="500">
                    </div>
                    <div class="zmongo-field">
                      <label>Skip</label>
                      <input class="zmongo-input" id="zmongo-skip" type="number" value="0" min="0">
                    </div>
                  </div>
                  <div class="zmongo-field">
                    <label>Documents</label>
                    <div id="zmongo-docs" class="zmongo-doc-list"></div>
                  </div>
                </div>

                <div>
                  <div class="zmongo-field">
                    <label>Selected Document ID</label>
                    <input class="zmongo-input" id="zmongo-selected-doc-id" type="text" readonly>
                  </div>
                  <div class="zmongo-field">
                    <label>JSON</label>
                    <textarea class="zmongo-textarea" id="zmongo-json-editor" placeholder='{"name":"example"}'></textarea>
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="zmongo-card">
            <div class="zmongo-card-header">
              <h3>Flattened Field Editor</h3>
              <div class="zmongo-row">
                <button class="zmongo-button secondary" id="zmongo-refresh-flattened">Refresh Fields</button>
                <button class="zmongo-button" id="zmongo-save-field">Save Field</button>
              </div>
            </div>
            <div class="zmongo-card-body">
              <div class="zmongo-grid-2">
                <div class="zmongo-field">
                  <label>Flattened Field Path</label>
                  <select class="zmongo-select" id="zmongo-field-select"></select>
                </div>
                <div class="zmongo-field">
                  <label>Manual Override Field Path</label>
                  <input class="zmongo-input" id="zmongo-field-manual" type="text" placeholder="metadata.author.name">
                </div>
              </div>
              <div class="zmongo-field">
                <label>Value</label>
                <textarea class="zmongo-textarea" id="zmongo-field-value" style="min-height:160px;" placeholder='"hello"' ></textarea>
              </div>
              <div class="zmongo-field">
                <label>
                  <input id="zmongo-parse-field-json" type="checkbox" checked>
                  Parse field value as JSON when possible
                </label>
              </div>
            </div>
          </div>
        </div>
      </div>
    `;

    document.body.appendChild(this.backdrop);
    document.body.appendChild(this.root);

    this.statusBox = this.root.querySelector("#zmongo-status");

    this.refs = {
      closeBtn: this.root.querySelector(".zmongo-close-btn"),
      tabs: [...this.root.querySelectorAll(".zmongo-tab")],

      localSidebar: this.root.querySelector("#zmongo-local-sidebar"),
      remoteSidebar: this.root.querySelector("#zmongo-remote-sidebar"),

      localHealth: this.root.querySelector("#zmongo-local-health"),
      localFilter: this.root.querySelector("#zmongo-local-filter"),
      localRefresh: this.root.querySelector("#zmongo-local-refresh"),
      localNewCollection: this.root.querySelector("#zmongo-local-new-collection"),
      localCreateCollection: this.root.querySelector("#zmongo-local-create-collection"),
      localDeleteCollection: this.root.querySelector("#zmongo-local-delete-collection"),
      localCollections: this.root.querySelector("#zmongo-local-collections"),

      remoteVerify: this.root.querySelector("#zmongo-remote-verify"),
      remoteBaseUrl: this.root.querySelector("#zmongo-remote-base-url"),
      remoteUsername: this.root.querySelector("#zmongo-remote-username"),
      remotePassword: this.root.querySelector("#zmongo-remote-password"),
      remoteLogin: this.root.querySelector("#zmongo-remote-login"),
      remoteRefresh: this.root.querySelector("#zmongo-remote-refresh"),
      remoteLogout: this.root.querySelector("#zmongo-remote-logout"),
      remoteFilter: this.root.querySelector("#zmongo-remote-filter"),
      remoteNewCollection: this.root.querySelector("#zmongo-remote-new-collection"),
      remoteCreateCollection: this.root.querySelector("#zmongo-remote-create-collection"),
      remoteDeleteCollection: this.root.querySelector("#zmongo-remote-delete-collection"),
      remoteCollections: this.root.querySelector("#zmongo-remote-collections"),

      selectedCollection: this.root.querySelector("#zmongo-selected-collection"),
      limit: this.root.querySelector("#zmongo-limit"),
      skip: this.root.querySelector("#zmongo-skip"),
      docs: this.root.querySelector("#zmongo-docs"),
      selectedDocId: this.root.querySelector("#zmongo-selected-doc-id"),
      jsonEditor: this.root.querySelector("#zmongo-json-editor"),

      loadDocs: this.root.querySelector("#zmongo-load-docs"),
      newDoc: this.root.querySelector("#zmongo-new-doc"),
      saveDoc: this.root.querySelector("#zmongo-save-doc"),
      deleteDoc: this.root.querySelector("#zmongo-delete-doc"),

      refreshFlattened: this.root.querySelector("#zmongo-refresh-flattened"),
      fieldSelect: this.root.querySelector("#zmongo-field-select"),
      fieldManual: this.root.querySelector("#zmongo-field-manual"),
      fieldValue: this.root.querySelector("#zmongo-field-value"),
      parseFieldJson: this.root.querySelector("#zmongo-parse-field-json"),
      saveField: this.root.querySelector("#zmongo-save-field"),
    };

    this.bindEvents();
    this.loadStoredValues();
    this.render();
  }

  bindEvents() {
    this.refs.closeBtn.addEventListener("click", () => this.close());

    for (const tab of this.refs.tabs) {
      tab.addEventListener("click", () => {
        this.state.tab = tab.dataset.tab;
        this.render();
      });
    }

    this.refs.localHealth.addEventListener("click", () => this.loadLocalHealth());
    this.refs.localRefresh.addEventListener("click", () => this.loadLocalCollections());
    this.refs.localCreateCollection.addEventListener("click", () => this.createCollection("local"));
    this.refs.localDeleteCollection.addEventListener("click", () => this.deleteCollection("local"));
    this.refs.localFilter.addEventListener("input", () => this.renderCollections("local"));

    this.refs.remoteLogin.addEventListener("click", () => this.remoteLogin());
    this.refs.remoteVerify.addEventListener("click", () => this.remoteVerify());
    this.refs.remoteRefresh.addEventListener("click", () => this.loadRemoteCollections());
    this.refs.remoteLogout.addEventListener("click", () => this.remoteLogout());
    this.refs.remoteCreateCollection.addEventListener("click", () => this.createCollection("remote"));
    this.refs.remoteDeleteCollection.addEventListener("click", () => this.deleteCollection("remote"));
    this.refs.remoteFilter.addEventListener("input", () => this.renderCollections("remote"));

    this.refs.remoteBaseUrl.addEventListener("change", () => this.persistRemoteSettings());
    this.refs.remoteUsername.addEventListener("change", () => this.persistRemoteSettings());

    this.refs.loadDocs.addEventListener("click", () => this.loadDocs());
    this.refs.newDoc.addEventListener("click", () => this.clearEditorForNewDoc());
    this.refs.saveDoc.addEventListener("click", () => this.saveFullDocument());
    this.refs.deleteDoc.addEventListener("click", () => this.deleteDocument());

    this.refs.refreshFlattened.addEventListener("click", () => this.refreshFlattenedFields());
    this.refs.fieldSelect.addEventListener("change", () => this.syncFieldValueFromSelection());
    this.refs.saveField.addEventListener("click", () => this.saveSelectedField());
  }

  loadStoredValues() {
    this.refs.remoteBaseUrl.value = this.state.remote.baseUrl;
    this.refs.remoteUsername.value = this.state.remote.username;
  }

  persistRemoteSettings() {
    this.state.remote.baseUrl = (this.refs.remoteBaseUrl.value || "").trim();
    this.state.remote.username = (this.refs.remoteUsername.value || "").trim();

    localStorage.setItem(STORAGE_KEYS.remoteBaseUrl, this.state.remote.baseUrl);
    localStorage.setItem(STORAGE_KEYS.remoteUsername, this.state.remote.username);
  }

  open() {
    this.backdrop.style.display = "block";
    this.root.style.display = "block";

    if (this.state.tab === "local" && this.state.local.collections.length === 0) {
      this.loadLocalHealth();
      this.loadLocalCollections();
    }
  }

  close() {
    this.backdrop.style.display = "none";
    this.root.style.display = "none";
  }

  setStatus(message, kind = "") {
    this.statusBox.textContent = message;
    this.statusBox.className = "zmongo-status";
    if (kind) this.statusBox.classList.add(kind);
  }

  render() {
    for (const tab of this.refs.tabs) {
      tab.classList.toggle("active", tab.dataset.tab === this.state.tab);
    }

    this.refs.localSidebar.classList.toggle("zmongo-hidden", this.state.tab !== "local");
    this.refs.remoteSidebar.classList.toggle("zmongo-hidden", this.state.tab !== "remote");

    const activeState = this.getActiveState();
    this.refs.selectedCollection.value = activeState.selectedCollection || "";
    this.refs.selectedDocId.value = activeState.selectedDocId || "";

    this.renderCollections("local");
    this.renderCollections("remote");
    this.renderDocs();
    this.refreshFlattenedFields();
  }

  getActiveState() {
    return this.state.tab === "remote" ? this.state.remote : this.state.local;
  }

  getLocalApiBase() {
    return DEFAULTS.localApiBase;
  }

  getRemoteApiBase() {
    return (this.state.remote.baseUrl || DEFAULTS.remoteBaseUrl).replace(/\/+$/, "");
  }

  async requestJson(url, options = {}) {
    const response = await fetch(url, options);
    let data = null;
    try {
      data = await response.json();
    } catch {
      data = null;
    }

    if (!response.ok) {
      const msg = data?.error || data?.message || `HTTP ${response.status}`;
      throw new Error(msg);
    }

    return data;
  }

  getRemoteHeaders(includeJson = false) {
    const headers = {};
    if (includeJson) headers["Content-Type"] = "application/json";
    if (this.state.remote.token) {
      headers.Authorization = `Bearer ${this.state.remote.token}`;
    }
    return headers;
  }

  async loadLocalHealth() {
    this.setStatus("Checking local health...");
    try {
      const data = await this.requestJson(`${this.getLocalApiBase()}/healthz`);
      this.state.local.health = data;
      this.setStatus(`Local healthy: ${data.db_name || "unknown_db"}`, "ok");
    } catch (error) {
      this.setStatus(`Local health failed: ${error.message}`, "error");
    }
  }

  async loadLocalCollections() {
    this.setStatus("Loading local collections...");
    try {
      const data = await this.requestJson(`${this.getLocalApiBase()}/collections`);
      this.state.local.collections = Array.isArray(data.collections) ? data.collections : [];
      if (!this.state.local.selectedCollection && this.state.local.collections.length) {
        this.state.local.selectedCollection = this.state.local.collections[0];
      }
      this.render();
      this.setStatus(`Loaded ${this.state.local.collections.length} local collection(s).`, "ok");
    } catch (error) {
      this.setStatus(`Local collections failed: ${error.message}`, "error");
    }
  }

  async remoteLogin() {
    this.persistRemoteSettings();

    const baseUrl = this.getRemoteApiBase();
    const username = this.refs.remoteUsername.value.trim();
    const password = this.refs.remotePassword.value;

    if (!baseUrl || !username || !password) {
      this.setStatus("Remote base URL, username, and password are required.", "warn");
      return;
    }

    this.setStatus("Logging in to remote manager...");
    try {
      const data = await this.requestJson(`${baseUrl}/api/auth/login`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      });

      if (!data?.token) {
        throw new Error("No token returned.");
      }

      this.state.remote.token = data.token;
      this.state.remote.username = data.username || username;
      localStorage.setItem(STORAGE_KEYS.remoteToken, this.state.remote.token);
      localStorage.setItem(STORAGE_KEYS.remoteUsername, this.state.remote.username);

      this.refs.remotePassword.value = "";
      this.refs.remoteUsername.value = this.state.remote.username;

      this.setStatus(`Remote login successful for ${this.state.remote.username}.`, "ok");
      await this.loadRemoteCollections();
    } catch (error) {
      this.setStatus(`Remote login failed: ${error.message}`, "error");
    }
  }

  async remoteVerify() {
    this.persistRemoteSettings();

    if (!this.state.remote.token) {
      this.setStatus("No remote token stored.", "warn");
      return;
    }

    this.setStatus("Verifying remote token...");
    try {
      const data = await this.requestJson(`${this.getRemoteApiBase()}/api/auth/verify`, {
        headers: this.getRemoteHeaders(false),
      });
      this.state.remote.verifiedUser = data.username || "";
      this.setStatus(`Remote token valid for ${data.username || "unknown user"}.`, "ok");
    } catch (error) {
      this.setStatus(`Remote verify failed: ${error.message}`, "error");
    }
  }

  remoteLogout() {
    this.state.remote.token = "";
    this.state.remote.collections = [];
    this.state.remote.selectedCollection = "";
    this.state.remote.docs = [];
    this.state.remote.selectedDocId = "";
    this.state.remote.selectedDoc = null;
    this.state.remote.verifiedUser = "";

    localStorage.removeItem(STORAGE_KEYS.remoteToken);
    this.render();
    this.setStatus("Remote token cleared.", "ok");
  }

  async loadRemoteCollections() {
    this.persistRemoteSettings();

    if (!this.state.remote.token) {
      this.setStatus("Remote login required.", "warn");
      return;
    }

    this.setStatus("Loading remote collections...");
    try {
      const data = await this.requestJson(
        `${this.getRemoteApiBase()}/user/manager/api/collections`,
        { headers: this.getRemoteHeaders(false) }
      );

      const collections = data?.data?.collections || data?.collections || [];
      this.state.remote.collections = Array.isArray(collections) ? collections : [];

      if (!this.state.remote.selectedCollection && this.state.remote.collections.length) {
        this.state.remote.selectedCollection = this.state.remote.collections[0];
      }

      this.render();
      this.setStatus(`Loaded ${this.state.remote.collections.length} remote collection(s).`, "ok");
    } catch (error) {
      this.setStatus(`Remote collections failed: ${error.message}`, "error");
    }
  }

  renderCollections(mode) {
    const state = mode === "remote" ? this.state.remote : this.state.local;
    const filterInput = mode === "remote" ? this.refs.remoteFilter : this.refs.localFilter;
    const target = mode === "remote" ? this.refs.remoteCollections : this.refs.localCollections;

    const term = (filterInput.value || "").trim().toLowerCase();
    const filtered = term
      ? state.collections.filter((name) => String(name).toLowerCase().includes(term))
      : [...state.collections];

    target.innerHTML = "";

    if (!filtered.length) {
      target.innerHTML = `<div class="zmongo-list-item">${escapeHtml("No collections found.")}</div>`;
      return;
    }

    for (const name of filtered) {
      const item = document.createElement("div");
      item.className = "zmongo-list-item" + (state.selectedCollection === name ? " active" : "");
      item.textContent = name;
      item.addEventListener("click", () => {
        state.selectedCollection = name;
        if (this.state.tab === mode) {
          this.refs.selectedCollection.value = name;
        }
        this.renderCollections(mode);
      });
      target.appendChild(item);
    }
  }

  async createCollection(mode) {
    const state = mode === "remote" ? this.state.remote : this.state.local;
    const input = mode === "remote" ? this.refs.remoteNewCollection : this.refs.localNewCollection;
    const name = (input.value || "").trim();

    if (!name) {
      this.setStatus("Collection name is required.", "warn");
      return;
    }

    this.setStatus(`Creating ${mode} collection ${name}...`);

    try {
      if (mode === "remote") {
        await this.requestJson(`${this.getRemoteApiBase()}/user/manager/api/collection/create`, {
          method: "POST",
          headers: this.getRemoteHeaders(true),
          body: JSON.stringify({ name }),
        });
        await this.loadRemoteCollections();
      } else {
        await this.requestJson(`${this.getLocalApiBase()}/collections/create`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ collection: name }),
        });
        await this.loadLocalCollections();
      }

      state.selectedCollection = name;
      input.value = "";
      this.render();
      this.setStatus(`Created ${mode} collection ${name}.`, "ok");
    } catch (error) {
      this.setStatus(`Create collection failed: ${error.message}`, "error");
    }
  }

  async deleteCollection(mode) {
    const state = mode === "remote" ? this.state.remote : this.state.local;
    const name = state.selectedCollection;

    if (!name) {
      this.setStatus("Select a collection first.", "warn");
      return;
    }

    if (!window.confirm(`Delete ${mode} collection '${name}'?`)) return;

    this.setStatus(`Deleting ${mode} collection ${name}...`);

    try {
      if (mode === "remote") {
        await this.requestJson(`${this.getRemoteApiBase()}/user/manager/api/collection/delete`, {
          method: "POST",
          headers: this.getRemoteHeaders(true),
          body: JSON.stringify({ name }),
        });
        await this.loadRemoteCollections();
      } else {
        await this.requestJson(`${this.getLocalApiBase()}/collections/delete`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ collection: name, force: true }),
        });
        await this.loadLocalCollections();
      }

      state.selectedCollection = "";
      state.docs = [];
      state.selectedDocId = "";
      state.selectedDoc = null;
      this.render();
      this.setStatus(`Deleted ${mode} collection ${name}.`, "ok");
    } catch (error) {
      this.setStatus(`Delete collection failed: ${error.message}`, "error");
    }
  }

  async loadDocs() {
    const state = this.getActiveState();
    const collection = state.selectedCollection;
    if (!collection) {
      this.setStatus("Select a collection first.", "warn");
      return;
    }

    const limit = Math.max(1, Math.min(500, parseInt(this.refs.limit.value || "50", 10)));
    const skip = Math.max(0, parseInt(this.refs.skip.value || "0", 10));

    this.setStatus(`Loading ${this.state.tab} documents from ${collection}...`);

    try {
      let docs = [];
      if (this.state.tab === "remote") {
        const data = await this.requestJson(
          `${this.getRemoteApiBase()}/user/manager/api/docs/${encodeURIComponent(collection)}?limit=${limit}&skip=${skip}`,
          { headers: this.getRemoteHeaders(false) }
        );
        docs = data?.data || data?.documents || [];
      } else {
        const data = await this.requestJson(
          `${this.getLocalApiBase()}/docs/${encodeURIComponent(collection)}?limit=${limit}&skip=${skip}&sort_field=_id&sort_dir=desc`
        );
        docs = data?.docs || [];
      }

      state.docs = Array.isArray(docs) ? docs : [];

      if (state.docs.length) {
        state.selectedDoc = state.docs[0];
        state.selectedDocId = extractObjectId(state.docs[0]._id);
        this.refs.jsonEditor.value = prettyJson(state.docs[0]);
      } else {
        state.selectedDoc = null;
        state.selectedDocId = "";
        this.refs.jsonEditor.value = "{\n  \n}";
      }

      this.render();
      this.setStatus(`Loaded ${state.docs.length} document(s).`, "ok");
    } catch (error) {
      this.setStatus(`Load docs failed: ${error.message}`, "error");
    }
  }

  renderDocs() {
    const state = this.getActiveState();
    this.refs.docs.innerHTML = "";
    this.refs.selectedCollection.value = state.selectedCollection || "";
    this.refs.selectedDocId.value = state.selectedDocId || "";

    if (!state.docs.length) {
      this.refs.docs.innerHTML = `<div class="zmongo-doc-item">No documents loaded.</div>`;
      return;
    }

    for (const doc of state.docs) {
      const id = extractObjectId(doc._id);
      const row = document.createElement("div");
      row.className = "zmongo-doc-item" + (id === state.selectedDocId ? " active" : "");
      row.innerHTML = `
        <div class="zmongo-doc-id">${escapeHtml(id || "<no _id>")}</div>
        <div class="zmongo-doc-summary">${escapeHtml(summarizeDoc(doc))}</div>
      `;
      row.addEventListener("click", () => {
        state.selectedDoc = doc;
        state.selectedDocId = id;
        this.refs.selectedDocId.value = id;
        this.refs.jsonEditor.value = prettyJson(doc);
        this.refreshFlattenedFields();
        this.renderDocs();
      });
      this.refs.docs.appendChild(row);
    }
  }

  clearEditorForNewDoc() {
    const state = this.getActiveState();
    state.selectedDoc = null;
    state.selectedDocId = "";
    this.refs.selectedDocId.value = "";
    this.refs.jsonEditor.value = "{\n  \n}";
    this.refreshFlattenedFields();
    this.renderDocs();
    this.setStatus("Editor cleared for new document.", "ok");
  }

  async saveFullDocument() {
    const state = this.getActiveState();
    const collection = state.selectedCollection;

    if (!collection) {
      this.setStatus("Select a collection first.", "warn");
      return;
    }

    let payload;
    try {
      payload = JSON.parse(this.refs.jsonEditor.value);
    } catch (error) {
      this.setStatus(`Invalid JSON: ${error.message}`, "error");
      return;
    }

    if (payload && typeof payload === "object") {
      delete payload._id;
    }

    this.setStatus(`Saving ${this.state.tab} document...`);

    try {
      if (this.state.tab === "remote") {
        if (state.selectedDocId) {
          await this.requestJson(`${this.getRemoteApiBase()}/user/manager/api/save-value`, {
            method: "POST",
            headers: this.getRemoteHeaders(true),
            body: JSON.stringify({
              collection,
              document_id: state.selectedDocId,
              value: payload,
              upsert_if_missing: true,
              metadata: {
                source: "comfyui-zmongo",
                mode: "remote-panel-full-save",
              },
            }),
          });
        } else {
          await this.requestJson(`${this.getRemoteApiBase()}/user/manager/api/create`, {
            method: "POST",
            headers: this.getRemoteHeaders(true),
            body: JSON.stringify({
              collection,
              document: payload,
            }),
          });
        }
      } else {
        if (state.selectedDocId) {
          await this.requestJson(`${this.getLocalApiBase()}/update`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              collection,
              id: state.selectedDocId,
              update: payload,
            }),
          });
        } else {
          await this.requestJson(`${this.getLocalApiBase()}/create`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              collection,
              document: payload,
            }),
          });
        }
      }

      await this.loadDocs();
      this.setStatus("Document saved successfully.", "ok");
    } catch (error) {
      this.setStatus(`Save failed: ${error.message}`, "error");
    }
  }

  async deleteDocument() {
    const state = this.getActiveState();
    const collection = state.selectedCollection;
    const id = state.selectedDocId;

    if (!collection || !id) {
      this.setStatus("Select a document first.", "warn");
      return;
    }

    if (!window.confirm(`Delete document ${id}?`)) return;

    this.setStatus(`Deleting ${this.state.tab} document ${id}...`);

    try {
      if (this.state.tab === "remote") {
        await this.requestJson(`${this.getRemoteApiBase()}/user/manager/api/delete`, {
          method: "POST",
          headers: this.getRemoteHeaders(true),
          body: JSON.stringify({ collection, id }),
        });
      } else {
        await this.requestJson(`${this.getLocalApiBase()}/delete`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ collection, id }),
        });
      }

      await this.loadDocs();
      this.setStatus(`Deleted document ${id}.`, "ok");
    } catch (error) {
      this.setStatus(`Delete failed: ${error.message}`, "error");
    }
  }

  refreshFlattenedFields() {
    const state = this.getActiveState();
    const select = this.refs.fieldSelect;
    select.innerHTML = "";

    if (!state.selectedDoc || typeof state.selectedDoc !== "object") {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No document selected";
      select.appendChild(option);
      return;
    }

    const flattened = flattenObject(state.selectedDoc);
    const keys = Object.keys(flattened).filter((k) => k !== "_id").sort();

    if (!keys.length) {
      const option = document.createElement("option");
      option.value = "";
      option.textContent = "No flattened fields found";
      select.appendChild(option);
      return;
    }

    for (const key of keys) {
      const option = document.createElement("option");
      option.value = key;
      option.textContent = key;
      select.appendChild(option);
    }

    if (this.refs.fieldManual.value.trim()) return;

    select.selectedIndex = 0;
    this.syncFieldValueFromSelection();
  }

  syncFieldValueFromSelection() {
    const state = this.getActiveState();
    if (!state.selectedDoc) return;

    const manualPath = this.refs.fieldManual.value.trim();
    const path = manualPath || this.refs.fieldSelect.value;
    if (!path) return;

    const flattened = flattenObject(state.selectedDoc);
    const value = flattened[path];
    this.refs.fieldValue.value = prettyJson(value);
  }

  async saveSelectedField() {
    const state = this.getActiveState();
    const collection = state.selectedCollection;
    const docId = state.selectedDocId;
    const fieldPath = this.refs.fieldManual.value.trim() || this.refs.fieldSelect.value;

    if (!collection || !docId) {
      this.setStatus("Select a document first.", "warn");
      return;
    }

    if (!fieldPath) {
      this.setStatus("Select or enter a field path.", "warn");
      return;
    }

    let value = this.refs.fieldValue.value;
    if (this.refs.parseFieldJson.checked) {
      const parsed = safeJsonParse(value, undefined);
      if (parsed !== undefined) {
        value = parsed;
      }
    }

    this.setStatus(`Saving field ${fieldPath}...`);

    try {
      if (this.state.tab === "remote") {
        await this.requestJson(`${this.getRemoteApiBase()}/user/manager/api/save-value`, {
          method: "POST",
          headers: this.getRemoteHeaders(true),
          body: JSON.stringify({
            collection,
            document_id: docId,
            field_path: fieldPath,
            value,
            upsert_if_missing: true,
            metadata: {
              source: "comfyui-zmongo",
              mode: "remote-panel-field-save",
              field_path: fieldPath,
            },
          }),
        });
      } else {
        await this.requestJson(`${this.getLocalApiBase()}/update`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            collection,
            id: docId,
            key: fieldPath,
            value,
          }),
        });
      }

      await this.loadDocs();
      this.setStatus(`Saved field ${fieldPath}.`, "ok");
    } catch (error) {
      this.setStatus(`Save field failed: ${error.message}`, "error");
    }
  }
}

app.registerExtension({
  name: "ZMongo.ManagerPanel",
  async setup() {
    const panel = new ZMongoManagerPanel();
    panel.mount();
  },
});