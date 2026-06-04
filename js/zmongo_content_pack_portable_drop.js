import { app } from "../../scripts/app.js";

const PORTABLE_SCHEMA_KIND = "zmongo_portable_content_pack";
const CONTENT_PACK_SCHEMA_KIND = "zmongo_content_pack";

const JSON_TEXT_LOADER_NODE_TYPE = "ZMongoContentPackJSONTextLoaderV3";
const JSON_FILE_LOADER_NODE_TYPE = "ZMongoContentPackLoadJSONFileV3";
const JSON_FILE_EXPORT_NODE_TYPE = "ZMongoContentPackExportJSONFileV3";

const EXTENSION_NAME = "BusinessProcessApplications.ZMongo.ContentPackPortableDownloads";

function parseJsonMaybe(text) {
    if (text === null || text === undefined) return null;
    if (typeof text === "object") return text;
    if (typeof text !== "string") return null;

    const trimmed = text.trim();
    if (!trimmed) return null;

    try {
        return JSON.parse(trimmed);
    } catch {
        return null;
    }
}

function isPortableContentPack(value) {
    return !!(
        value &&
        typeof value === "object" &&
        !Array.isArray(value) &&
        (
            value.schema_kind === PORTABLE_SCHEMA_KIND ||
            value.schema_kind === CONTENT_PACK_SCHEMA_KIND ||
            (Array.isArray(value.fields) && value.content_pack_name)
        )
    );
}

function safeFilenameStem(value, fallback = "content_pack") {
    const raw = String(value || fallback).trim() || fallback;
    const cleaned = raw
        .replace(/[^a-zA-Z0-9._-]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 96);
    return cleaned || fallback;
}

function timestampForFilename() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return [
        now.getFullYear(),
        pad(now.getMonth() + 1),
        pad(now.getDate()),
        "_",
        pad(now.getHours()),
        pad(now.getMinutes()),
        pad(now.getSeconds()),
    ].join("");
}

function filenameFromPortableJson(jsonText, fallback = "content_pack") {
    const parsed = parseJsonMaybe(jsonText);
    const name = parsed?.content_pack_name || parsed?.name || fallback;
    return `${safeFilenameStem(name, fallback)}_${timestampForFilename()}.json`;
}

function prettyPortableJsonText(jsonText) {
    const parsed = parseJsonMaybe(jsonText);
    if (!parsed) return String(jsonText || "");
    try {
        return JSON.stringify(parsed, null, 2);
    } catch {
        return String(jsonText || "");
    }
}

function downloadTextToBrowserDownloads(text, filename) {
    const cleanText = String(text || "");
    if (!cleanText.trim()) {
        alert("No portable content-pack JSON is available to download.");
        return false;
    }

    const parsed = parseJsonMaybe(cleanText);
    if (!isPortableContentPack(parsed)) {
        const proceed = confirm(
            "The selected text does not look like a ZMongo portable content pack. Download it anyway?"
        );
        if (!proceed) return false;
    }

    const finalFilename = filename && filename.toLowerCase().endsWith(".json")
        ? filename
        : `${safeFilenameStem(filename || "content_pack")}.json`;

    const blob = new Blob([prettyPortableJsonText(cleanText)], {
        type: "application/json;charset=utf-8",
    });

    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = finalFilename;
    anchor.style.display = "none";

    document.body.appendChild(anchor);
    anchor.click();

    window.setTimeout(() => {
        try { URL.revokeObjectURL(url); } catch {}
        try { anchor.remove(); } catch {}
    }, 1000);

    return true;
}

function getWidget(node, widgetName) {
    return node?.widgets?.find((widget) => widget.name === widgetName) || null;
}

function getWidgetValue(node, widgetName, fallback = "") {
    const widget = getWidget(node, widgetName);
    return widget ? widget.value : fallback;
}

function setWidgetValue(node, widgetName, value) {
    const widget = getWidget(node, widgetName);
    if (!widget) return false;

    widget.value = value;
    try { widget.callback?.(value); } catch {}
    return true;
}

function addButtonOnce(node, name, label, callback) {
    if (!node || node[`__zmongo_${name}_installed`]) return;
    node[`__zmongo_${name}_installed`] = true;

    node.addWidget("button", label, null, () => {
        try {
            callback(node);
        } catch (error) {
            console.warn(`[ZMongo] Button failed: ${label}`, error);
            alert(`ZMongo button failed: ${error?.message || error}`);
        }
    });
}

function addToggleOnce(node, name, label, defaultValue = true) {
    if (!node || node[`__zmongo_${name}_toggle_installed`]) return null;
    node[`__zmongo_${name}_toggle_installed`] = true;

    const widget = node.addWidget("toggle", label, defaultValue, (value) => {
        node[`__zmongo_${name}_toggle_value`] = !!value;
    });
    node[`__zmongo_${name}_toggle_value`] = !!defaultValue;
    return widget;
}

function getCanvasGraphPosition(event) {
    const canvas = app.canvas;
    const rect = canvas?.canvas?.getBoundingClientRect?.();

    const x = rect ? event.clientX - rect.left : event.clientX;
    const y = rect ? event.clientY - rect.top : event.clientY;

    try {
        if (canvas?.convertEventToCanvasOffset) {
            return canvas.convertEventToCanvasOffset(event);
        }
    } catch {}

    try {
        const ds = canvas.ds;
        return [
            (x - ds.offset[0]) / ds.scale,
            (y - ds.offset[1]) / ds.scale,
        ];
    } catch {}

    return [x, y];
}

function nodeTypeExists(typeName) {
    try {
        return !!LiteGraph.registered_node_types?.[typeName];
    } catch {
        return false;
    }
}

function addLoaderNode(jsonText, event, filename = "portable_content_pack.json") {
    if (!nodeTypeExists(JSON_TEXT_LOADER_NODE_TYPE)) {
        alert(`ZMongo portable content-pack loader node is not registered: ${JSON_TEXT_LOADER_NODE_TYPE}`);
        return null;
    }

    const node = LiteGraph.createNode(JSON_TEXT_LOADER_NODE_TYPE);
    if (!node) {
        alert(`Failed to create ${JSON_TEXT_LOADER_NODE_TYPE}.`);
        return null;
    }

    const pos = getCanvasGraphPosition(event);
    node.pos = [pos[0], pos[1]];
    node.title = `📦 Portable Content Pack: ${filename}`;

    app.graph.add(node);

    setWidgetValue(node, "content_pack_json", prettyPortableJsonText(jsonText));
    setWidgetValue(node, "validate_schema", true);

    installNodeDownloadButtons(node, JSON_TEXT_LOADER_NODE_TYPE);

    try { node.size = node.computeSize?.() || node.size; } catch {}
    try { app.graph.setDirtyCanvas(true, true); } catch {}
    try { app.canvas.setDirty(true, true); } catch {}

    return node;
}

async function handlePortableFileDrop(event) {
    const files = Array.from(event.dataTransfer?.files || []);
    if (!files.length) return false;

    const candidates = files.filter((file) => file.name.toLowerCase().endsWith(".json"));
    if (!candidates.length) return false;

    let handled = false;

    for (const file of candidates) {
        let text = "";

        try {
            text = await file.text();
        } catch (error) {
            console.warn("[ZMongo] Failed to read dropped JSON file", file.name, error);
            continue;
        }

        const parsed = parseJsonMaybe(text);
        if (!isPortableContentPack(parsed)) continue;

        event.preventDefault();
        event.stopPropagation();

        addLoaderNode(text, event, file.name);
        handled = true;
    }

    return handled;
}

function installDropHandler() {
    const canvasEl = app.canvas?.canvas;
    if (!canvasEl || canvasEl.zmongoPortableDropInstalled) return;

    canvasEl.zmongoPortableDropInstalled = true;

    canvasEl.addEventListener(
        "dragover",
        (event) => {
            const items = Array.from(event.dataTransfer?.items || []);
            const hasJson = items.some((item) => {
                return (
                    item.kind === "file" &&
                    (
                        item.type === "application/json" ||
                        item.type === "text/json" ||
                        item.type === "" ||
                        String(item.type || "").includes("json")
                    )
                );
            });

            if (hasJson) {
                event.preventDefault();
            }
        },
        true
    );

    canvasEl.addEventListener(
        "drop",
        (event) => {
            handlePortableFileDrop(event).catch((error) => {
                console.warn("[ZMongo] Portable content-pack drop failed", error);
            });
        },
        true
    );
}

function deepFindPortableJson(value, depth = 0) {
    if (depth > 7 || value === null || value === undefined) return "";

    if (typeof value === "string") {
        const parsed = parseJsonMaybe(value);
        return isPortableContentPack(parsed) ? value : "";
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const found = deepFindPortableJson(item, depth + 1);
            if (found) return found;
        }
        return "";
    }

    if (typeof value === "object") {
        if (isPortableContentPack(value)) {
            try {
                return JSON.stringify(value, null, 2);
            } catch {
                return "";
            }
        }

        const preferredKeys = [
            "portable_json",
            "portable_content_pack_json",
            "content_pack_json",
            "download_json",
            "json_text",
            "text",
            "ui",
            "output",
            "outputs",
            "data",
        ];

        for (const key of preferredKeys) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const found = deepFindPortableJson(value[key], depth + 1);
                if (found) return found;
            }
        }

        for (const key of Object.keys(value)) {
            const found = deepFindPortableJson(value[key], depth + 1);
            if (found) return found;
        }
    }

    return "";
}

function installNodeDownloadButtons(node, nodeTypeName) {
    if (!node) return;

    if (nodeTypeName === JSON_TEXT_LOADER_NODE_TYPE) {
        addButtonOnce(node, "download_text_loader_json", "⬇️ Download JSON to Downloads", () => {
            const jsonText = getWidgetValue(node, "content_pack_json", "");
            const filename = filenameFromPortableJson(jsonText, "portable_content_pack");
            downloadTextToBrowserDownloads(jsonText, filename);
        });
    }

    if (nodeTypeName === JSON_FILE_LOADER_NODE_TYPE) {
        addButtonOnce(node, "download_loaded_file_again", "⬇️ Download Loaded JSON to Downloads", async () => {
            const filePath = String(getWidgetValue(node, "content_pack_file", "") || "").trim();
            if (!filePath) {
                alert("This node has no content_pack_file path selected.");
                return;
            }

            alert(
                "Browser JavaScript cannot directly read arbitrary server file paths. " +
                "Use the JSON Text Loader node for browser Downloads, or run the Export JSON File node with the UI payload patch below."
            );
        });
    }

    if (nodeTypeName === JSON_FILE_EXPORT_NODE_TYPE) {
        addToggleOnce(node, "auto_download_after_export", "Auto-download to browser Downloads after run", true);

        addButtonOnce(node, "download_last_export_json", "⬇️ Download Last Export to Downloads", () => {
            const jsonText = node.__zmongo_last_portable_json || "";
            if (!jsonText) {
                alert(
                    "No portable JSON is available on this node yet. Run the node first. " +
                    "If this still appears after execution, apply the Python UI payload patch so the export node exposes portable_json to the frontend."
                );
                return;
            }
            const filename = node.__zmongo_last_portable_filename || filenameFromPortableJson(jsonText, "content_pack");
            downloadTextToBrowserDownloads(jsonText, filename);
        });
    }
}

app.registerExtension({
    name: EXTENSION_NAME,

    async setup() {
        setTimeout(installDropHandler, 500);
        setTimeout(installDropHandler, 1500);
        setTimeout(installDropHandler, 3000);
        console.info("[ZMongo] Portable content-pack Downloads helper loaded.");
    },

    async beforeRegisterNodeDef(nodeType, nodeData) {
        const nodeName = nodeData?.name;
        const isTargetNode = [
            JSON_TEXT_LOADER_NODE_TYPE,
            JSON_FILE_LOADER_NODE_TYPE,
            JSON_FILE_EXPORT_NODE_TYPE,
        ].includes(nodeName);

        if (!isTargetNode) return;

        const originalOnNodeCreated = nodeType.prototype.onNodeCreated;
        nodeType.prototype.onNodeCreated = function () {
            const result = originalOnNodeCreated?.apply(this, arguments);
            installNodeDownloadButtons(this, nodeName);
            return result;
        };

        if (nodeName === JSON_FILE_EXPORT_NODE_TYPE) {
            const originalOnExecuted = nodeType.prototype.onExecuted;
            nodeType.prototype.onExecuted = function (message) {
                const result = originalOnExecuted?.apply(this, arguments);

                const portableJson = deepFindPortableJson(message);
                if (portableJson) {
                    this.__zmongo_last_portable_json = portableJson;
                    this.__zmongo_last_portable_filename = filenameFromPortableJson(portableJson, "content_pack");

                    if (this.__zmongo_auto_download_after_export_toggle_value !== false) {
                        downloadTextToBrowserDownloads(
                            this.__zmongo_last_portable_json,
                            this.__zmongo_last_portable_filename
                        );
                    }
                } else {
                    console.warn(
                        "[ZMongo] Export node executed, but no portable_json UI payload was found. " +
                        "Add the Python UI payload patch to enable browser Downloads."
                    );
                }

                return result;
            };
        }
    },

    async nodeCreated() {
        installDropHandler();
    },
});