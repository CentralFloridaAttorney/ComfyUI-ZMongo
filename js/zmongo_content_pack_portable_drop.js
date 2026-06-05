import { app } from "../../scripts/app.js";

/*
 * ComfyUI-ZMongo Content Pack Browser Download Helper
 * ---------------------------------------------------
 *
 * Safe version:
 * - DOES NOT register dragover/drop handlers.
 * - DOES NOT block ComfyUI native workflow drag/drop.
 * - Keeps browser Downloads support for ZMongoContentPackExportJSONFileV3.
 *
 * Python exporter:
 * - Saves to ComfyUI/output/content_packs.
 *
 * Browser JS:
 * - Downloads the same exported JSON to the user's browser Downloads folder.
 */

const PORTABLE_SCHEMA_KIND = "zmongo_portable_content_pack";
const CONTENT_PACK_SCHEMA_KIND = "zmongo_content_pack";

const JSON_TEXT_LOADER_NODE_TYPE = "ZMongoContentPackJSONTextLoaderV3";
const JSON_FILE_EXPORT_NODE_TYPE = "ZMongoContentPackExportJSONFileV3";
const GET_IMAGE_NODE_TYPE = "ZMongoContentPackGetImageV3";

const EXTENSION_NAME = "BusinessProcessApplications.ZMongo.ContentPackDownloadsOnly";

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

function isDownloadableJson(value) {
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

    if (!isDownloadableJson(parsed)) {
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
        return isDownloadableJson(parsed) ? value : "";
    }

    if (Array.isArray(value)) {
        for (const item of value) {
            const found = deepFindExportJson(item, depth + 1);
            if (found) return found;
        }
        return "";
    }

    if (typeof value === "object") {
        if (isDownloadableJson(value)) {
            try {
                return JSON.stringify(value, null, 2);
            } catch {
                return "";
            }
        }

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
        const preferredKeys = [
            "filename",
            "file_name",
            "download_filename",
        ];

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
        /*
         * Intentionally no dragover/drop listeners.
         * This preserves ComfyUI's native workflow drag/drop behavior.
         */
        console.info("[ZMongo] Content-pack browser Downloads helper loaded. Native ComfyUI drop behavior preserved.");
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
        if (
            node?.comfyClass === JSON_FILE_EXPORT_NODE_TYPE ||
            node?.type === JSON_FILE_EXPORT_NODE_TYPE
        ) {
            installExportNodeHelpers(node);
        }
    },
});