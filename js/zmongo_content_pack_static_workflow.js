import { app } from "../../scripts/app.js";

const EXTENSION_NAME = "BusinessProcessApplications.ZMongo.ContentPackStaticWorkflow.ValuesOnlySequence";
const STATIC_NODE_TYPE = "ZMongoContentPackStaticOutputsV3";
const EXPORT_NODE_TYPE = "ZMongoContentPackExportStaticWorkflowV3";
const MAX_STATIC_OUTPUTS = 64;
const FIELD_TYPES = new Set(["STRING", "INT", "FLOAT", "BOOLEAN", "IMAGE", "JSON", "ANY", "*"]);

function parseJsonMaybe(value) {
    if (value === null || value === undefined) return null;
    if (typeof value === "object") return value;
    if (typeof value !== "string") return null;
    const text = value.trim();
    if (!text) return null;
    try { return JSON.parse(text); } catch { return null; }
}

function jsonPretty(value) {
    const parsed = parseJsonMaybe(value);
    try { return JSON.stringify(parsed ?? value, null, 2); }
    catch { return String(value ?? ""); }
}

function normalizeType(value) {
    const raw = String(value || "ANY").trim().toUpperCase();
    const aliases = {
        STR: "STRING",
        TEXT: "STRING",
        INTEGER: "INT",
        BOOL: "BOOLEAN",
        IMAGE_ASSET: "IMAGE",
        IMAGE_SEQUENCE_ASSET: "IMAGE",
        OBJECT: "JSON",
        DICT: "JSON",
        LIST: "JSON",
    };
    const type = aliases[raw] || raw;
    if (type === "ANY") return "*";
    return FIELD_TYPES.has(type) ? type : "*";
}

function manifestType(value) {
    const type = normalizeType(value);
    return type === "*" ? "ANY" : type;
}

function safeName(value, fallback = "field") {
    let text = String(value || fallback).trim();
    text = text.replace(/[^A-Za-z0-9_]+/g, "_").replace(/^_+|_+$/g, "");
    if (!text) text = fallback;
    if (/^[0-9]/.test(text)) text = `field_${text}`;
    return text;
}

function safeFilenameStem(value, fallback = "content_pack_static_workflow") {
    let text = String(value || fallback).trim();
    text = text.replace(/[^A-Za-z0-9_.-]+/g, "_").replace(/^[._-]+|[._-]+$/g, "");
    return text || fallback;
}

function timestampForFilename() {
    const now = new Date();
    const pad = (n) => String(n).padStart(2, "0");
    return `${now.getFullYear()}${pad(now.getMonth() + 1)}${pad(now.getDate())}_${pad(now.getHours())}${pad(now.getMinutes())}${pad(now.getSeconds())}`;
}

function getWidget(node, widgetName) {
    return node?.widgets?.find((widget) => widget.name === widgetName) || null;
}

function getWidgetValue(node, widgetName, fallback = "") {
    const widget = getWidget(node, widgetName);
    return widget ? widget.value : fallback;
}

function addButtonOnce(node, key, label, callback) {
    if (!node || node[`__zmongo_${key}_installed`]) return;
    node[`__zmongo_${key}_installed`] = true;
    node.addWidget("button", label, null, () => {
        try { callback(node); }
        catch (error) {
            console.warn(`[ZMongo] ${label} failed`, error);
            alert(`ZMongo button failed: ${error?.message || error}`);
        }
    });
}

function addToggleOnce(node, key, label, defaultValue = true) {
    if (!node || node[`__zmongo_${key}_toggle_installed`]) return;
    node[`__zmongo_${key}_toggle_installed`] = true;
    node[`__zmongo_${key}_toggle_value`] = !!defaultValue;
    node.addWidget("toggle", label, defaultValue, (value) => {
        node[`__zmongo_${key}_toggle_value`] = !!value;
    });
}

function downloadText(text, filename, mime = "application/json;charset=utf-8") {
    const finalText = String(text || "");
    if (!finalText.trim()) {
        alert("Nothing is available to download yet. Run the export node first.");
        return false;
    }
    const blob = new Blob([finalText], { type: mime });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = filename || `content_pack_static_workflow_${timestampForFilename()}.workflow.json`;
    anchor.style.display = "none";
    document.body.appendChild(anchor);
    anchor.click();
    window.setTimeout(() => {
        try { URL.revokeObjectURL(url); } catch {}
        try { anchor.remove(); } catch {}
    }, 1000);
    return true;
}

function staticFieldsFromNode(node) {
    const text = getWidgetValue(node, "content_pack_json", "{}");
    const parsed = parseJsonMaybe(text) || {};
    const fields = Array.isArray(parsed.fields) ? parsed.fields : [];
    const used = new Set();

    return fields.slice(0, MAX_STATIC_OUTPUTS).map((field, index) => {
        let name = safeName(field?.alias || field?.source_path || `field_${index}`, `field_${index}`);
        if (used.has(name)) {
            const base = name;
            let suffix = 2;
            while (used.has(`${base}_${suffix}`)) suffix += 1;
            name = `${base}_${suffix}`;
        }
        used.add(name);
        const type = normalizeType(field?.comfy_type || field?.data_type || field?.json_type || "ANY");
        return { index, name, type, comfy_type: manifestType(type) };
    });
}

function disconnectRemovedLinks(node, keepCount) {
    if (!node?.outputs) return;
    const graph = node.graph || app.graph;
    for (let i = keepCount; i < node.outputs.length; i++) {
        const output = node.outputs[i];
        const links = Array.isArray(output?.links) ? [...output.links] : [];
        for (const linkId of links) {
            try { graph?.removeLink?.(linkId); } catch {}
        }
    }
}

function ensureOutput(node, slot, name, type) {
    if (!node.outputs) node.outputs = [];
    while (node.outputs.length <= slot) node.addOutput(`out_${node.outputs.length}`, "*");
    const output = node.outputs[slot];
    output.name = name;
    output.localized_name = name;
    output.type = type;
    delete output.shape;
    return output;
}

function rehydrateStaticOutputs(node) {
    if (!node) return;
    const fields = staticFieldsFromNode(node);
    disconnectRemovedLinks(node, fields.length);
    for (const field of fields) ensureOutput(node, field.index, field.name, field.type);
    node.outputs.length = fields.length;
    node.__zmongo_static_fields = fields;
    try { node.size = node.computeSize?.() || node.size; } catch {}
    try { app.graph?.setDirtyCanvas?.(true, true); } catch {}
    try { app.canvas?.setDirty?.(true, true); } catch {}
}

function flattenContentPackFromText(text) {
    const parsed = parseJsonMaybe(text);
    if (!parsed || typeof parsed !== "object") return null;

    const sourceFields = Array.isArray(parsed.fields) ? parsed.fields : [];
    const used = new Set();
    const fields = sourceFields.slice(0, MAX_STATIC_OUTPUTS).map((field, index) => {
        let name = safeName(field?.alias || field?.source_path || `field_${index}`, `field_${index}`);
        if (used.has(name)) {
            const base = name;
            let suffix = 2;
            while (used.has(`${base}_${suffix}`)) suffix += 1;
            name = `${base}_${suffix}`;
        }
        used.add(name);
        const socketType = normalizeType(field?.comfy_type || field?.data_type || field?.json_type || "ANY");
        const comfyType = manifestType(socketType);
        const out = {
            index,
            alias: name,
            label: field?.label || name,
            source_path: field?.source_path || name,
            comfy_type: comfyType,
            json_type: field?.json_type || "",
            storage: field?.storage || "inline",
            summary: field?.summary || {},
        };
        for (const key of ["value", "asset_ref", "json_ref", "content_type", "filename", "sequence"]) {
            if (Object.prototype.hasOwnProperty.call(field || {}, key)) out[key] = field[key];
        }
        return out;
    });

    return {
        schema_kind: "zmongo_static_content_pack_workflow",
        schema_version: parsed.schema_version || "3.0.0",
        created_at: parsed.created_at || new Date().toISOString(),
        updated_at: new Date().toISOString(),
        content_pack_name: parsed.content_pack_name || "content_pack",
        project_name: parsed.project_name || "default",
        source_schema_kind: parsed.schema_kind || parsed.source_schema_kind || "zmongo_content_pack",
        source_manifest_hash: parsed.manifest_hash || parsed.source_manifest_hash || parsed?.source?.original_manifest_hash || "",
        field_count: fields.length,
        fields,
        content: {
            summary: `Static workflow pack '${parsed.content_pack_name || "content_pack"}' with ${fields.length} restored value output(s).`,
        },
    };
}

function makeWorkflowFromFlatPack(flatPack, nodeTitle = "📦 Static Content Pack Outputs") {
    const outputs = (flatPack.fields || []).map((field) => ({
        localized_name: field.alias,
        name: field.alias,
        type: normalizeType(field.comfy_type),
        links: null,
    }));

    return {
        id: `zmongo-static-content-pack-${Date.now()}`,
        revision: 0,
        last_node_id: 1,
        last_link_id: 0,
        nodes: [{
            id: 1,
            type: STATIC_NODE_TYPE,
            pos: [640, 360],
            size: [520, Math.max(220, 100 + 28 * Math.min((flatPack.fields || []).length, MAX_STATIC_OUTPUTS))],
            flags: {},
            order: 0,
            mode: 0,
            inputs: [
                { localized_name: "content_pack_json", name: "content_pack_json", type: "STRING", widget: { name: "content_pack_json" }, link: null },
                { localized_name: "refresh_token", name: "refresh_token", shape: 7, type: "STRING", widget: { name: "refresh_token" }, link: null },
                { localized_name: "session", name: "session", shape: 7, type: "ZMONGO_API_SESSION", link: null },
                { localized_name: "master_key_hex", name: "master_key_hex", shape: 7, type: "STRING", widget: { name: "master_key_hex" }, link: null },
            ],
            outputs,
            title: nodeTitle,
            properties: { "Node name for S&R": STATIC_NODE_TYPE },
            widgets_values: [jsonPretty(flatPack), "", ""],
        }],
        links: [],
        groups: [],
        config: {},
        extra: {
            ds: { scale: 1.0, offset: [0, 0] },
            zmongo_static_content_pack: {
                content_pack_name: flatPack.content_pack_name,
                field_count: flatPack.field_count,
            },
        },
        version: 0.4,
    };
}

function deepFindWorkflowJson(value, depth = 0) {
    if (depth > 8 || value === null || value === undefined) return "";
    if (typeof value === "string") {
        const parsed = parseJsonMaybe(value);
        if (parsed && Array.isArray(parsed.nodes) && Array.isArray(parsed.links)) return value;
        return "";
    }
    if (Array.isArray(value)) {
        for (const item of value) {
            const found = deepFindWorkflowJson(item, depth + 1);
            if (found) return found;
        }
        return "";
    }
    if (typeof value === "object") {
        if (Array.isArray(value.nodes) && Array.isArray(value.links)) return jsonPretty(value);
        for (const key of ["workflow_json", "workflow", "ui", "output", "outputs", "data", "result"]) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const found = deepFindWorkflowJson(value[key], depth + 1);
                if (found) return found;
            }
        }
        for (const key of Object.keys(value)) {
            const found = deepFindWorkflowJson(value[key], depth + 1);
            if (found) return found;
        }
    }
    return "";
}

function deepFindFilename(value, depth = 0) {
    if (depth > 8 || value === null || value === undefined) return "";
    if (typeof value === "string") return value.toLowerCase().endsWith(".json") ? value : "";
    if (Array.isArray(value)) {
        for (const item of value) {
            const found = deepFindFilename(item, depth + 1);
            if (found) return found;
        }
        return "";
    }
    if (typeof value === "object") {
        for (const key of ["filename", "file_name", "download_filename", "ui", "data"]) {
            if (Object.prototype.hasOwnProperty.call(value, key)) {
                const found = deepFindFilename(value[key], depth + 1);
                if (found) return found;
            }
        }
    }
    return "";
}

function installStaticNodeButtons(node) {
    addButtonOnce(node, "rehydrate_static_outputs", "🔁 Rehydrate Value Outputs", () => rehydrateStaticOutputs(node));
    addButtonOnce(node, "download_static_workflow_from_embedded", "⬇️ Download This Static Workflow", () => {
        const text = getWidgetValue(node, "content_pack_json", "{}");
        const flatPack = flattenContentPackFromText(text);
        if (!flatPack) {
            alert("This static node does not contain a valid embedded content pack JSON.");
            return;
        }
        const workflow = makeWorkflowFromFlatPack(flatPack, node.title || "📦 Static Content Pack Outputs");
        const filename = `${safeFilenameStem(flatPack.content_pack_name || "content_pack_static_workflow")}_${timestampForFilename()}.workflow.json`;
        downloadText(jsonPretty(workflow), filename);
    });
}

function installExportNodeButtons(node) {
    addToggleOnce(node, "auto_download_static_workflow", "Auto-download workflow JSON after run", true);
    addButtonOnce(node, "download_last_static_workflow", "⬇️ Download Last Workflow JSON", () => {
        const workflowJson = node.__zmongo_last_static_workflow_json || "";
        if (!workflowJson) {
            alert("No static workflow JSON is available yet. Run the export node first.");
            return;
        }
        const filename = node.__zmongo_last_static_workflow_filename || `content_pack_static_workflow_${timestampForFilename()}.workflow.json`;
        downloadText(workflowJson, filename);
    });
}

app.registerExtension({
    name: EXTENSION_NAME,

    async beforeRegisterNodeDef(nodeType, nodeData) {
        const nodeName = nodeData?.name;

        if (nodeName === STATIC_NODE_TYPE) {
            const originalOnNodeCreated = nodeType.prototype.onNodeCreated;
            nodeType.prototype.onNodeCreated = function () {
                const result = originalOnNodeCreated?.apply(this, arguments);
                installStaticNodeButtons(this);
                setTimeout(() => rehydrateStaticOutputs(this), 0);
                setTimeout(() => rehydrateStaticOutputs(this), 250);
                setTimeout(() => rehydrateStaticOutputs(this), 1000);
                return result;
            };

            const originalOnConfigure = nodeType.prototype.onConfigure;
            nodeType.prototype.onConfigure = function () {
                const result = originalOnConfigure?.apply(this, arguments);
                installStaticNodeButtons(this);
                setTimeout(() => rehydrateStaticOutputs(this), 0);
                setTimeout(() => rehydrateStaticOutputs(this), 250);
                setTimeout(() => rehydrateStaticOutputs(this), 1000);
                return result;
            };

            const originalOnPropertyChanged = nodeType.prototype.onPropertyChanged;
            nodeType.prototype.onPropertyChanged = function () {
                const result = originalOnPropertyChanged?.apply(this, arguments);
                setTimeout(() => rehydrateStaticOutputs(this), 0);
                return result;
            };
        }

        if (nodeName === EXPORT_NODE_TYPE) {
            const originalOnNodeCreated = nodeType.prototype.onNodeCreated;
            nodeType.prototype.onNodeCreated = function () {
                const result = originalOnNodeCreated?.apply(this, arguments);
                installExportNodeButtons(this);
                return result;
            };

            const originalOnExecuted = nodeType.prototype.onExecuted;
            nodeType.prototype.onExecuted = function (message) {
                const result = originalOnExecuted?.apply(this, arguments);
                const workflowJson = deepFindWorkflowJson(message);
                const filename = deepFindFilename(message) || `content_pack_static_workflow_${timestampForFilename()}.workflow.json`;
                const browserDownload = message?.browser_download?.[0] ?? message?.ui?.browser_download?.[0] ?? true;
                if (workflowJson) {
                    this.__zmongo_last_static_workflow_json = jsonPretty(workflowJson);
                    this.__zmongo_last_static_workflow_filename = filename;
                    if (browserDownload !== false && this.__zmongo_auto_download_static_workflow_toggle_value !== false) {
                        downloadText(this.__zmongo_last_static_workflow_json, filename);
                    }
                } else {
                    console.warn("[ZMongo] Export node executed but no workflow_json UI payload was found.", message);
                }
                return result;
            };
        }
    },
});

console.log("[ZMongo] Static content-pack workflow helper loaded: values-only image-sequence mode.");