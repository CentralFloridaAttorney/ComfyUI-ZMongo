import { app } from "../../scripts/app.js";

/*
 * ComfyUI-ZMongo Content Pack Browser Download + Drop Helper
 * ----------------------------------------------------------
 *
 * What this does:
 * 1. Auto-downloads ZMongoContentPackExportJSONFileV3 output to browser Downloads.
 * 2. Adds a "Download Last Export" button to the export node.
 * 3. Makes valid ZMongo workflow_json files droppable onto the canvas/page.
 * 4. Makes raw zmongo_portable_content_pack JSON droppable by creating loader/getter nodes.
 *
 * Important:
 * - Python saves to the ComfyUI server output folder.
 * - JavaScript saves to the user's browser Downloads folder.
 */

const PORTABLE_SCHEMA_KIND = "zmongo_portable_content_pack";
const CONTENT_PACK_SCHEMA_KIND = "zmongo_content_pack";

const JSON_TEXT_LOADER_NODE_TYPE = "ZMongoContentPackJSONTextLoaderV3";
const JSON_FILE_EXPORT_NODE_TYPE = "ZMongoContentPackExportJSONFileV3";
const GET_IMAGE_NODE_TYPE = "ZMongoContentPackGetImageV3";
const PREVIEW_IMAGE_NODE_TYPE = "PreviewImage";

const EXTENSION_NAME = "BusinessProcessApplications.ZMongo.ContentPackDownloadsAndDrop";

function parseJsonMaybe(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === "object") return value;
    if (typeof value !== "string") return null;

    const text = value.trim();
    if (!text) return null;

    try {
        return JSON.parse(text);
    } catch {
        return null;
    }
}

function isPlainObject(value) {
    return !!value && typeof value === "object" && !Array.isArray(value);
}

function isPortableContentPack(value) {
    return !!(
        isPlainObject(value) &&
        (
            value.schema_kind === PORTABLE_SCHEMA_KIND ||
            value.schema_kind === CONTENT_PACK_SCHEMA_KIND ||
            (Array.isArray(value.fields) && value.content_pack_name)
        )
    );
}

function isComfyWorkflow(value) {
    return !!(
        isPlainObject(value) &&
        Array.isArray(value.nodes) &&
        (
            Array.isArray(value.links) ||
            typeof value.last_node_id !== "undefined" ||
            typeof value.last_link_id !== "undefined"
        )
    );
}

function isZMongoWorkflow(value) {
    if (!isComfyWorkflow(value)) return false;

    if (value.extra?.zmongo?.workflow_kind === "portable_content_pack_workflow") {
        return true;
    }

    return value.nodes.some((node) => {
        return (
            node?.type === JSON_TEXT_LOADER_NODE_TYPE ||
            node?.type === GET_IMAGE_NODE_TYPE
        );
    });
}

function isZMongoExportJson(value) {
    return isPortableContentPack(value) || isZMongoWorkflow(value) || isComfyWorkflow(value);
}

function safeFilenameStem(value, fallback = "content_pack") {
    const raw = String(value || fallback).trim() || fallback;
    const clean = raw
        .replace(/[^a-zA-Z0-9._-]+/g, "_")
        .replace(/^_+|_+$/g, "")
        .slice(0, 120);

    return clean || fallback;
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

function filenameFromJsonText(jsonText, fallback = "content_pack") {
    const parsed = parseJsonMaybe(jsonText);

    let name = fallback;

    if (isPortableContentPack(parsed)) {
        name = parsed.content_pack_name || parsed.name || fallback;
    } else if (isZMongoWorkflow(parsed)) {
        name =
            parsed.extra?.zmongo?.content_pack_name ||
            parsed.extra?.zmongo?.workflow_name ||
            fallback;
    } else if (isComfyWorkflow(parsed)) {
        name =
            parsed.extra?.zmongo?.content_pack_name ||
            parsed.extra?.workflow_name ||
            fallback;
    }

    return `${safeFilenameStem(name, fallback)}_${timestampForFilename()}.json`;
}

function prettyJsonText(value) {
    const parsed = parseJsonMaybe(value);

    if (!parsed) {
        return String(value || "");
    }

    try {
        return JSON.stringify(parsed, null, 2);
    } catch {
        return String(value || "");
    }
}

function showNotice(message, severity = "info") {
    const detail = String(message || "");

    const toast =
        window.comfyAPI?.app?.app?.extensionManager?.toast ||
        window.comfyAPI?.app?.extensionManager?.toast ||
        app?.extensionManager?.toast;

    if (toast && typeof toast.add === "function") {
        try {
            toast.add({
                severity,
                summary: "ZMongo",
                detail,
                life: 4500,
            });
            return;
        } catch {}
    }

    if (severity === "error" || severity === "warn") {
        alert(detail);
    } else {
        console.info(`[ZMongo] ${detail}`);
    }
}

function downloadTextToBrowserDownloads(text, filename = "") {
    const cleanText = String(text || "");

    if (!cleanText.trim()) {
        showNotice("No ZMongo export JSON is available to download.", "warn");
        return false;
    }

    const parsed = parseJsonMaybe(cleanText);
    if (!isZMongoExportJson(parsed)) {
        const proceed = confirm(
            "The selected text does not look like a ZMongo content pack or workflow JSON. Download anyway?"
        );
        if (!proceed) return false;
    }

    const finalFilename = filename && filename.toLowerCase().endsWith(".json")
        ? filename
        : filenameFromJsonText(cleanText, "content_pack");

    const blob = new Blob([prettyJsonText(cleanText)], {
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

    showNotice(`Downloaded ${finalFilename} to browser Downloads.`);
    return true;
}

function deepFindExportJson(value, depth = 0) {
    if (depth > 10 || value === null || value === undefined) return "";

    if (typeof value === "string") {
        const parsed = parseJsonMaybe(value);
        return isZMongoExportJson(parsed) ? value : "";
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const found = deepFindExportJson(item, depth + 1);
            if (found) return found;
        }
        return "";
    }

    if (typeof value === "object") {
        if (isZMongoExportJson(value)) {
            try {
                return JSON.stringify(value, null, 2);
            } catch {
                return "";
            }
        }

        /*
         * Comfy node UI payloads usually arrive as direct keys:
         * {
         *   portable_json: ["..."],
         *   filename: ["..."]
         * }
         *
         * But this recursive search also handles nested forms.
         */
        const preferredKeys = [
            "workflow_json",
            "content_pack_workflow_json",
            "portable_json",
            "portable_content_pack_json",
            "content_pack_json",
            "json",
            "json_text",
            "text",
            "ui",
            "output",
            "outputs",
            "data",
        ];

        for (const key of preferredKeys) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const found = deepFindExportJson(value[key], depth + 1);
                if (found) return found;
            }
        }

        for (const key of Object.keys(value)) {
            const found = deepFindExportJson(value[key], depth + 1);
            if (found) return found;
        }
    }

    return "";
}

function deepFindFilename(value, depth = 0) {
    if (depth > 8 || value === null || value === undefined) return "";

    if (typeof value === "string") {
        return value.toLowerCase().endsWith(".json") ? value : "";
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const found = deepFindFilename(item, depth + 1);
            if (found) return found;
        }
        return "";
    }

    if (typeof value === "object") {
        const preferredKeys = ["filename", "file_name", "download_filename"];

        for (const key of preferredKeys) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const found = deepFindFilename(value[key], depth + 1);
                if (found) return found;
            }
        }
    }

    return "";
}

function addButtonOnce(node, internalName, label, callback) {
    if (!node || node[`__zmongo_${internalName}_installed`]) return;

    node[`__zmongo_${internalName}_installed`] = true;

    node.addWidget("button", label, null, () => {
        try {
            callback(node);
        } catch (error) {
            console.error(`[ZMongo] Button failed: ${label}`, error);
            showNotice(`ZMongo button failed: ${error?.message || error}`, "error");
        }
    });
}

function addToggleOnce(node, internalName, label, defaultValue = true) {
    if (!node || node[`__zmongo_${internalName}_installed`]) return;

    node[`__zmongo_${internalName}_installed`] = true;
    node[`__zmongo_${internalName}_value`] = !!defaultValue;

    node.addWidget("toggle", label, defaultValue, (value) => {
        node[`__zmongo_${internalName}_value`] = !!value;
    });
}

function getWidget(node, widgetName) {
    if (!node?.widgets) return null;

    return node.widgets.find((widget) => {
        return (
            widget.name === widgetName ||
            widget.label === widgetName ||
            String(widget.name || "").toLowerCase() === String(widgetName || "").toLowerCase()
        );
    }) || null;
}

function setWidgetValue(node, widgetName, value) {
    const widget = getWidget(node, widgetName);
    if (!widget) return false;

    widget.value = value;

    try {
        widget.callback?.(value, app.canvas, node, null);
    } catch {
        try { widget.callback?.(value); } catch {}
    }

    return true;
}

function nodeTypeExists(typeName) {
    try {
        return !!window.LiteGraph?.registered_node_types?.[typeName];
    } catch {
        return false;
    }
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

function markGraphDirty() {
    try { app.graph.setDirtyCanvas(true, true); } catch {}
    try { app.canvas.setDirty(true, true); } catch {}
    try { app.canvas.draw(true, true); } catch {}
}

function clearGraph() {
    try {
        app.graph.clear();
    } catch {
        try {
            while (app.graph._nodes?.length) {
                app.graph.remove(app.graph._nodes[0]);
            }
        } catch {}
    }
}

async function loadWorkflowJson(workflowJson) {
    /*
     * File menu works because ComfyUI routes the JSON through its workflow loader.
     * This reproduces that behavior as closely as possible.
     */
    const attempts = [
        async () => {
            if (typeof app.loadGraphData === "function") {
                await app.loadGraphData(workflowJson);
                return true;
            }
            return false;
        },
        async () => {
            if (typeof app.loadGraphData === "function") {
                await app.loadGraphData(workflowJson, true);
                return true;
            }
            return false;
        },
        async () => {
            clearGraph();
            app.graph.configure(workflowJson);
            return true;
        },
    ];

    let lastError = null;

    for (const attempt of attempts) {
        try {
            const ok = await attempt();
            if (ok) {
                markGraphDirty();
                showNotice("Loaded workflow JSON onto the canvas.");
                return true;
            }
        } catch (error) {
            lastError = error;
            console.warn("[ZMongo] Workflow load attempt failed.", error);
        }
    }

    showNotice(`Failed to load workflow JSON: ${lastError?.message || lastError || "unknown error"}`, "error");
    return false;
}

function findInputIndex(node, inputName) {
    const inputs = node?.inputs || [];
    return inputs.findIndex((input) => input?.name === inputName);
}

function ensureInputSlot(node, inputName, inputType = "*") {
    let index = findInputIndex(node, inputName);
    if (index >= 0) return index;

    try {
        node.addInput(inputName, inputType);
        return findInputIndex(node, inputName);
    } catch {
        return -1;
    }
}

function firstImageAlias(contentPack) {
    const fields = Array.isArray(contentPack?.fields) ? contentPack.fields : [];
    const imageField = fields.find((field) => {
        return String(field?.comfy_type || "").toUpperCase() === "IMAGE";
    });

    return imageField?.alias || "hero_image";
}

function createPortableContentPackNodes(contentPack, rawText, event) {
    if (!nodeTypeExists(JSON_TEXT_LOADER_NODE_TYPE)) {
        showNotice(`Missing node type: ${JSON_TEXT_LOADER_NODE_TYPE}`, "error");
        return false;
    }

    const basePos = getCanvasGraphPosition(event);

    clearGraph();

    const loader = window.LiteGraph.createNode(JSON_TEXT_LOADER_NODE_TYPE);
    loader.title = "09 ZMongo Content Pack JSON Text Loader";
    loader.pos = [basePos[0], basePos[1]];
    app.graph.add(loader);

    setWidgetValue(loader, "content_pack_json", prettyJsonText(rawText));
    setWidgetValue(loader, "validate_schema", true);
    setWidgetValue(loader, "refresh_token", "");

    let getter = null;

    if (nodeTypeExists(GET_IMAGE_NODE_TYPE)) {
        getter = window.LiteGraph.createNode(GET_IMAGE_NODE_TYPE);
        getter.title = "09 ZMongo Content Pack Get Image";
        getter.pos = [basePos[0] + 560, basePos[1]];
        app.graph.add(getter);

        setWidgetValue(getter, "field_alias", firstImageAlias(contentPack));
        setWidgetValue(getter, "strict_type", true);
        setWidgetValue(getter, "master_key_hex", "");

        try {
            loader.connect(0, getter, ensureInputSlot(getter, "content_pack", "ZMONGO_CONTENT_PACK"));
        } catch {}
    }

    if (getter && nodeTypeExists(PREVIEW_IMAGE_NODE_TYPE)) {
        const preview = window.LiteGraph.createNode(PREVIEW_IMAGE_NODE_TYPE);
        preview.title = "Preview Image";
        preview.pos = [basePos[0] + 1040, basePos[1]];
        app.graph.add(preview);

        try {
            getter.connect(0, preview, ensureInputSlot(preview, "images", "IMAGE"));
        } catch {}
    }

    markGraphDirty();
    showNotice("Loaded raw portable ZMongo content pack onto the canvas.");
    return true;
}

async function handleDroppedJsonFile(file, event) {
    const text = await file.text();
    const parsed = parseJsonMaybe(text);

    if (!parsed) {
        showNotice(`Dropped file is not valid JSON: ${file.name}`, "warn");
        return false;
    }

    if (isComfyWorkflow(parsed)) {
        return await loadWorkflowJson(parsed);
    }

    if (isPortableContentPack(parsed)) {
        return createPortableContentPackNodes(parsed, text, event);
    }

    showNotice(`JSON file is not a ComfyUI workflow or ZMongo content pack: ${file.name}`, "warn");
    return false;
}

async function handleDrop(event) {
    const files = Array.from(event.dataTransfer?.files || []);
    const jsonFiles = files.filter((file) => file.name.toLowerCase().endsWith(".json"));

    if (!jsonFiles.length) return false;

    /*
     * We only prevent default after confirming this is a JSON file drop.
     * This avoids breaking non-JSON browser/ComfyUI drag behavior.
     */
    event.preventDefault();
    event.stopPropagation();

    let loadedAny = false;

    for (const file of jsonFiles) {
        try {
            const loaded = await handleDroppedJsonFile(file, event);
            loadedAny = loadedAny || loaded;
        } catch (error) {
            console.error("[ZMongo] Failed to load dropped JSON file.", error);
            showNotice(`Failed to load ${file.name}: ${error?.message || error}`, "error");
        }
    }

    return loadedAny;
}

function handleDragOver(event) {
    const items = Array.from(event.dataTransfer?.items || []);
    const hasJson = items.some((item) => {
        return (
            item.kind === "file" &&
            (
                String(item.type || "").includes("json") ||
                item.type === "" ||
                item.type === "application/json"
            )
        );
    });

    if (hasJson) {
        event.preventDefault();
        event.dataTransfer.dropEffect = "copy";
    }
}

function installDropHandlers() {
    if (window.__zmongo_content_pack_global_drop_installed) return;
    window.__zmongo_content_pack_global_drop_installed = true;

    /*
     * Use document/window capture, not only canvas.
     * Newer ComfyUI frontends can route drag events through overlays or layout panes,
     * so canvas-only handlers may never see the drop.
     */
    document.addEventListener("dragover", handleDragOver, true);
    document.addEventListener("drop", (event) => {
        handleDrop(event).catch((error) => {
            console.error("[ZMongo] Document drop handler failed.", error);
            showNotice(`ZMongo drop failed: ${error?.message || error}`, "error");
        });
    }, true);

    window.addEventListener("dragover", handleDragOver, true);
    window.addEventListener("drop", (event) => {
        handleDrop(event).catch((error) => {
            console.error("[ZMongo] Window drop handler failed.", error);
            showNotice(`ZMongo drop failed: ${error?.message || error}`, "error");
        });
    }, true);

    console.info("[ZMongo] Content-pack JSON browser-download/drop handlers installed.");
}

function installExportNodeHelpers(node) {
    if (!node) return;

    addToggleOnce(
        node,
        "auto_download_after_export",
        "Auto-download export to browser Downloads",
        true
    );

    addButtonOnce(
        node,
        "download_last_export",
        "⬇️ Download Last Export to Browser Downloads",
        () => {
            const jsonText = node.__zmongo_last_export_json || "";
            if (!jsonText) {
                showNotice("No export JSON is cached yet. Run the export node first.", "warn");
                return;
            }

            const filename =
                node.__zmongo_last_export_filename ||
                filenameFromJsonText(jsonText, "content_pack");

            downloadTextToBrowserDownloads(jsonText, filename);
        }
    );
}

app.registerExtension({
    name: EXTENSION_NAME,

    async setup() {
        window.setTimeout(installDropHandlers, 250);
        window.setTimeout(installDropHandlers, 1000);
        window.setTimeout(installDropHandlers, 2500);
    },

    async beforeRegisterNodeDef(nodeType, nodeData) {
        const nodeName = nodeData?.name;

        if (nodeName !== JSON_FILE_EXPORT_NODE_TYPE) return;

        const originalOnNodeCreated = nodeType.prototype.onNodeCreated;
        nodeType.prototype.onNodeCreated = function () {
            const result = originalOnNodeCreated?.apply(this, arguments);
            installExportNodeHelpers(this);
            return result;
        };

        const originalOnExecuted = nodeType.prototype.onExecuted;
        nodeType.prototype.onExecuted = function (message) {
            const result = originalOnExecuted?.apply(this, arguments);

            const exportJson = deepFindExportJson(message);
            const filename = deepFindFilename(message);

            if (exportJson) {
                this.__zmongo_last_export_json = exportJson;
                this.__zmongo_last_export_filename =
                    filename ||
                    filenameFromJsonText(exportJson, "content_pack");

                if (this.__zmongo_auto_download_after_export_value !== false) {
                    downloadTextToBrowserDownloads(
                        this.__zmongo_last_export_json,
                        this.__zmongo_last_export_filename
                    );
                }
            } else {
                console.warn("[ZMongo] Export node executed, but no export JSON was found in the UI payload.", message);
                showNotice("Export completed, but browser-download JSON was not found in the node UI payload.", "warn");
            }

            return result;
        };
    },

    async nodeCreated(node) {
        installDropHandlers();

        if (node?.comfyClass === JSON_FILE_EXPORT_NODE_TYPE || node?.type === JSON_FILE_EXPORT_NODE_TYPE) {
            installExportNodeHelpers(node);
        }
    },
});