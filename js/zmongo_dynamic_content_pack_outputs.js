import { app } from "../../scripts/app.js";

const BUILD_NODE_TYPE = "ZMongoContentPackFromDocumentJSONNode";
const SAVE_NODE_TYPE = "ZMongoContentPackSaveNode";
const LOAD_NODE_TYPE = "ZMongoContentPackLoadNode";
const DYNAMIC_NODE_TYPE = "ZMongoDynamicContentPackOutputs";

const PRODUCER_TYPES = new Set([BUILD_NODE_TYPE, SAVE_NODE_TYPE, LOAD_NODE_TYPE]);
const MAX_DYNAMIC_OUTPUTS = 64;
const MAX_VALUE_LABEL_CHARS = 84;
const TEXTAREA_HEIGHT = 198;

function markDirty(node) {
    try { node?.setDirtyCanvas?.(true, true); } catch {}
    try { app.graph?.setDirtyCanvas?.(true, true); } catch {}
    try { app.canvas?.setDirty?.(true, true); } catch {}
}

function parseJsonMaybe(value) {
    if (value === null || value === undefined) return null;
    if (Array.isArray(value)) {
        if (value.length === 1) return parseJsonMaybe(value[0]);
        return value;
    }
    if (typeof value === "object") return value;
    if (typeof value !== "string") return null;

    const text = value.trim();
    if (!text || text === "{}") return null;

    try { return JSON.parse(text); } catch { return null; }
}

function toPrettyJson(value) {
    try { return JSON.stringify(value || {}, null, 2); } catch { return String(value ?? "{}"); }
}

function isObject(value) {
    return !!value && typeof value === "object" && !Array.isArray(value);
}

function isContentPackDocument(value) {
    return !!(
        isObject(value) &&
        (
            value.schema_kind === "zmongo_content_pack" ||
            value.schema_kind === "zmongo_content_pack_source" ||
            Array.isArray(value.field_index) ||
            isObject(value.outputs) ||
            value.content_pack_name
        )
    );
}

function fieldCount(doc) {
    if (!isObject(doc)) return 0;

    if (Array.isArray(doc.field_index)) return doc.field_index.length;

    if (isObject(doc.outputs)) {
        let total = 0;
        for (const key of ["images", "texts", "numbers", "booleans", "any"]) {
            if (Array.isArray(doc.outputs[key])) total += doc.outputs[key].length;
        }
        return total;
    }

    const total = Number(doc.field_report?.counts?.total || doc.field_count || 0);
    return Number.isFinite(total) ? total : 0;
}

function updatedStamp(doc) {
    if (!isObject(doc)) return 0;
    const numeric = Number(doc.updated_at_unix || doc.created_at_unix || 0);
    if (Number.isFinite(numeric) && numeric > 0) return numeric;
    const parsed = Date.parse(doc.updated_at || doc.created_at || "");
    return Number.isFinite(parsed) ? parsed / 1000 : 0;
}

function scoreContentPack(doc) {
    if (!isContentPackDocument(doc)) return [-1, -1, -1];

    let score = 0;
    if (doc.schema_kind === "zmongo_content_pack") score += 1000;
    if (doc.schema_kind === "zmongo_content_pack_source") score += 900;

    const count = fieldCount(doc);
    if (count > 0) score += 500;

    return [score, count, updatedStamp(doc)];
}

function compareDocDesc(a, b) {
    const as = scoreContentPack(a);
    const bs = scoreContentPack(b);
    for (let i = 0; i < as.length; i++) {
        if (as[i] !== bs[i]) return bs[i] - as[i];
    }
    return 0;
}

function selectBestContentPack(values) {
    const docs = [];

    function collect(value) {
        const parsed = parseJsonMaybe(value);
        if (!parsed) return;

        if (Array.isArray(parsed)) {
            for (const item of parsed) collect(item);
            return;
        }

        if (isContentPackDocument(parsed)) docs.push(parsed);
    }

    collect(values);
    if (!docs.length) return null;

    docs.sort(compareDocDesc);
    return docs[0];
}

function addContainerCandidates(container, candidates) {
    if (!isObject(container)) return;

    for (const key of ["documents", "docs", "results", "items", "records"]) {
        if (Array.isArray(container[key])) {
            const best = selectBestContentPack(container[key]);
            if (best) candidates.push(best);
        }
    }

    for (const key of ["document", "doc", "result", "item", "record", "content_pack", "content_pack_source", "content_pack_document"]) {
        if (container[key]) candidates.push(container[key]);
    }
}

function extractContentPackDocument(value) {
    const parsed = parseJsonMaybe(value);
    if (!parsed) return null;

    if (Array.isArray(parsed)) return selectBestContentPack(parsed);
    if (isContentPackDocument(parsed)) return parsed;
    if (!isObject(parsed)) return null;

    const candidates = [];
    addContainerCandidates(parsed, candidates);

    const data = parsed.data;
    if (isObject(data)) {
        if (isContentPackDocument(data)) candidates.push(data);
        addContainerCandidates(data, candidates);
        addContainerCandidates(data.save_payload?.data, candidates);
        addContainerCandidates(data.query_payload?.data, candidates);
    }

    const bestDirect = selectBestContentPack(candidates);
    if (bestDirect) return bestDirect;

    for (const candidate of candidates) {
        const found = extractContentPackDocument(candidate);
        if (found) return found;
    }

    return null;
}

function recursiveContentPackSearch(value, depth = 0, seen = new WeakSet()) {
    if (value === null || value === undefined || depth > 10) return null;

    const direct = extractContentPackDocument(value);
    if (direct) return direct;

    if (typeof value === "string") {
        const text = value.trim();
        if (!text) return null;
        if (!text.includes("zmongo_content_pack") && !text.includes("field_index") && !text.includes("content_pack")) return null;
        return recursiveContentPackSearch(parseJsonMaybe(text), depth + 1, seen);
    }

    if (Array.isArray(value)) {
        const found = [];
        for (const item of value) {
            const doc = recursiveContentPackSearch(item, depth + 1, seen);
            if (doc) found.push(doc);
        }
        return selectBestContentPack(found);
    }

    if (isObject(value)) {
        if (seen.has(value)) return null;
        seen.add(value);

        const found = [];
        for (const item of Object.values(value)) {
            const doc = recursiveContentPackSearch(item, depth + 1, seen);
            if (doc) found.push(doc);
        }
        return selectBestContentPack(found);
    }

    return null;
}

function extractContentPackCandidate(message) {
    const candidates = [
        message?.content_pack_json,
        message?.content_pack_source_json,
        message?.content_pack_document,
        message?.ui?.content_pack_json,
        message?.ui?.content_pack_source_json,
        message?.ui?.content_pack_document,
        message?.output,
        message?.outputs,
        message?.result,
        message?.ui,
        message,
    ];

    const found = [];
    for (const candidate of candidates) {
        const doc = recursiveContentPackSearch(candidate);
        if (doc) found.push(doc);
    }
    return selectBestContentPack(found);
}

function getWidget(node, name) {
    return node.widgets?.find((widget) => widget.name === name) || null;
}

function getWidgetValue(node, name, fallback = "") {
    const widget = getWidget(node, name);
    return widget ? (widget.value ?? fallback) : fallback;
}

function setWidgetValue(node, name, value, silent = false) {
    const widget = getWidget(node, name);
    if (!widget || widget.value === value) return;

    if (silent) {
        node.zmongoSuppressContentPackCallbacks = true;
        try { widget.value = value; } finally { node.zmongoSuppressContentPackCallbacks = false; }
    } else {
        widget.value = value;
    }
}

function ensureProperties(node) {
    if (!node.properties || typeof node.properties !== "object") node.properties = {};
    return node.properties;
}

function cacheContentPackProperty(node, contentPack) {
    if (!contentPack) return;
    const text = toPrettyJson(contentPack);
    const props = ensureProperties(node);
    props.zmongo_content_pack_json = text;
    props.zmongo_content_pack_updated_at = Date.now();

    node.zmongoLastContentPackJson = text;
    node.zmongoLastUiContentPackJson = text;
    node.zmongoLastContentPackSourceJson = text;
}

function getInput(node, name) {
    return node.inputs?.find((input) => input.name === name) || null;
}

function isInputLinked(node, name) {
    const input = getInput(node, name);
    return !!input && input.link !== null && input.link !== undefined;
}

function getLinkedOriginNode(node, inputName) {
    const input = getInput(node, inputName);
    if (!input || input.link === null || input.link === undefined || !app.graph) return null;

    const link = app.graph.links?.[input.link];
    if (!link) return null;

    return app.graph.getNodeById(link.origin_id);
}

function getContentPackFromLinkedOrigin(node) {
    const origin = getLinkedOriginNode(node, "content_pack_json");
    if (!origin) return null;

    return selectBestContentPack([
        extractContentPackDocument(origin.zmongoLastContentPackJson),
        extractContentPackDocument(origin.zmongoLastUiContentPackJson),
        extractContentPackDocument(origin.zmongoLastContentPackSourceJson),
        extractContentPackDocument(origin.properties?.zmongo_content_pack_json),
        extractContentPackDocument(getWidgetValue(origin, "content_pack_json")),
        extractContentPackDocument(getWidgetValue(origin, "content_pack_source_json")),
        extractContentPackDocument(getWidgetValue(origin, "cached_content_pack_json")),
        extractContentPackDocument(getWidgetValue(origin, "resolved_content_pack_json")),
        extractContentPackDocument(getWidgetValue(origin, "runtime_content_pack_json")),
    ]);
}

function getContentPackFromDynamicNode(node) {
    const linked = getContentPackFromLinkedOrigin(node);
    if (linked) return linked;

    return selectBestContentPack([
        extractContentPackDocument(node.zmongoLastContentPackJson),
        extractContentPackDocument(node.properties?.zmongo_content_pack_json),
        extractContentPackDocument(getWidgetValue(node, "content_pack_json")),
        extractContentPackDocument(getWidgetValue(node, "cached_content_pack_json")),
        extractContentPackDocument(getWidgetValue(node, "resolved_content_pack_json")),
        extractContentPackDocument(getWidgetValue(node, "runtime_content_pack_json")),
    ]);
}

function widgetDomElements(widget) {
    return [widget?.inputEl, widget?.element, widget?.textarea, widget?.domElement, widget?.input].filter(Boolean);
}

function constrainTextareaWidget(node, name, height = TEXTAREA_HEIGHT) {
    const widget = getWidget(node, name);
    if (!widget) return;

    if (widget.type === "hidden") widget.type = "text";
    widget.options = { ...(widget.options || {}), multiline: true, serialize: true };
    widget.computeSize = function (width) {
        const nodeWidth = node?.size?.[0] || width || 460;
        return [Math.max(280, nodeWidth - 20), height];
    };

    for (const element of widgetDomElements(widget)) {
        try {
            Object.assign(element.style, {
                height: `${height}px`,
                maxHeight: `${height}px`,
                minHeight: `${height}px`,
                overflowY: "auto",
                overflowX: "auto",
                resize: "none",
                whiteSpace: "pre",
                boxSizing: "border-box",
            });
        } catch {}
    }
}

function hideLegacyCacheWidget(node, name) {
    const widget = getWidget(node, name);
    if (!widget) return;

    widget.type = "hidden";
    widget.options = { ...(widget.options || {}), hidden: true, serialize: true, multiline: false };
    widget.computeSize = () => [0, 0];
    widget.draw = () => {};

    for (const element of widgetDomElements(widget)) {
        try {
            Object.assign(element.style, {
                display: "none",
                visibility: "hidden",
                height: "0px",
                maxHeight: "0px",
                minHeight: "0px",
                overflow: "hidden",
                padding: "0px",
                margin: "0px",
                border: "0px",
            });
        } catch {}
    }
}

function applyDynamicLayout(node) {
    constrainTextareaWidget(node, "content_pack_json", 10 * 18 + 18);
    constrainTextareaWidget(node, "dynamic_status", TEXTAREA_HEIGHT);

    hideLegacyCacheWidget(node, "cached_content_pack_json");
    hideLegacyCacheWidget(node, "resolved_content_pack_json");
    hideLegacyCacheWidget(node, "runtime_content_pack_json");

    markDirty(node);
}

function setStatus(node, text) {
    let widget = getWidget(node, "dynamic_status");
    if (!widget) {
        node.addWidget("text", "dynamic_status", text, () => {}, { multiline: true, serialize: true });
        widget = getWidget(node, "dynamic_status");
    } else {
        widget.value = text;
    }
    constrainTextareaWidget(node, "dynamic_status", TEXTAREA_HEIGHT);
}

function valueForItem(item) {
    if (!isObject(item)) return item;
    if ("value" in item) return item.value;
    if ("ref" in item) return item.ref;
    if ("summary" in item) return item.summary;
    return item;
}

function formatValue(value) {
    let text;
    if (value === null || value === undefined) text = "null";
    else if (typeof value === "string") text = value;
    else if (typeof value === "number" || typeof value === "boolean") text = String(value);
    else {
        try { text = JSON.stringify(value); } catch { text = String(value); }
    }
    text = text.replace(/\s+/g, " ").trim();
    if (text.length > MAX_VALUE_LABEL_CHARS) text = `${text.slice(0, MAX_VALUE_LABEL_CHARS - 1)}…`;
    return text;
}

function groupsFromFieldIndex(contentPack) {
    const fieldIndex = Array.isArray(contentPack?.field_index) ? contentPack.field_index : [];
    const groups = { images: [], texts: [], numbers: [], booleans: [], any: [] };

    for (const field of fieldIndex) {
        if (!isObject(field)) continue;

        const kind = String(field.kind || "");
        const valueType = String(field.value_type || "").toLowerCase();
        const path = String(field.path || field.name || "");

        if (kind === "image_asset") {
            groups.images.push({ ...field, name: path, path, value_type: "image" });
        } else if ((kind === "text" || kind === "metadata") && ["string", "str", ""].includes(valueType)) {
            groups.texts.push({ ...field, name: path, path, value_type: "string" });
        } else if (kind === "number") {
            groups.numbers.push({ ...field, name: path, path });
        } else if (kind === "boolean") {
            groups.booleans.push({ ...field, name: path, path, value_type: "boolean" });
        } else if (kind === "json" || kind === "large_json") {
            groups.any.push({ ...field, name: path, path, value: field.inline ? field.value : field.ref });
        }
    }

    return groups;
}

function getFieldsFromContentPack(contentPack, node) {
    if (!contentPack) return [];

    const includeImages = getWidgetValue(node, "include_images", true) !== false;
    const includeMetadata = getWidgetValue(node, "include_metadata", true) !== false;
    const groups = groupsFromFieldIndex(contentPack);
    const fields = [];

    function pushGroup(groupName, items) {
        if (groupName === "images" && !includeImages) return;
        for (const item of items || []) {
            if (fields.length >= MAX_DYNAMIC_OUTPUTS) return;
            const path = String(item.path || item.name || "");
            if (!includeMetadata && (path.startsWith("metadata.") || path.startsWith("_"))) continue;
            fields.push({ groupName, item });
        }
    }

    pushGroup("images", groups.images);
    pushGroup("texts", groups.texts);
    pushGroup("numbers", groups.numbers);
    pushGroup("booleans", groups.booleans);
    pushGroup("any", groups.any);

    return fields.slice(0, MAX_DYNAMIC_OUTPUTS);
}

function outputTypeForField(field) {
    if (field.groupName === "images") return "IMAGE";
    if (field.groupName === "texts") return "STRING";
    if (field.groupName === "booleans") return "BOOLEAN";
    if (field.groupName === "numbers") {
        const valueType = String(field.item?.value_type || "").toLowerCase();
        const value = field.item?.value;
        if (valueType === "int" || (Number.isInteger(value) && typeof value !== "boolean")) return "INT";
        return "FLOAT";
    }
    return "*";
}

function outputNameForField(field, index) {
    const item = field.item || {};
    const path = String(item.path || item.name || `${field.groupName}_${String(index).padStart(2, "0")}`);
    return `${path} = ${formatValue(valueForItem(item))}`;
}

function outputSignature(node) {
    return (node.outputs || []).map((output) => {
        return `${output.name || ""}:${output.type || "*"}:${output.zmongoContentPackPath || ""}:${output.zmongoContentPackValueDisplay || ""}`;
    }).join("|");
}

function desiredSignature(fields) {
    return fields.map((field, index) => {
        return `${outputNameForField(field, index)}:${outputTypeForField(field)}:${field.item?.path || field.item?.name || ""}:${formatValue(valueForItem(field.item))}`;
    }).join("|");
}

function disconnectOutputLinks(node, outputIndex) {
    const output = node.outputs?.[outputIndex];
    if (!output) return;

    const links = Array.isArray(output.links) ? [...output.links] : [];
    for (const linkId of links) {
        try { app.graph?.removeLink(linkId); }
        catch { try { node.disconnectOutput(outputIndex, linkId); } catch {} }
    }
    output.links = [];
}

function removeOutputForce(node, index) {
    if (!node.outputs || index < 0 || index >= node.outputs.length) return;
    disconnectOutputLinks(node, index);
    try { node.removeOutput(index); }
    catch { node.outputs.splice(index, 1); }
}

function shrinkOutputs(node, targetLength) {
    if (!node.outputs) return;
    for (let i = node.outputs.length - 1; i >= targetLength; i--) removeOutputForce(node, i);
}

function setOutput(node, index, field) {
    const name = outputNameForField(field, index);
    const type = outputTypeForField(field);
    const value = valueForItem(field.item);
    const valueDisplay = formatValue(value);
    const path = String(field.item?.path || field.item?.name || "");

    if (!node.outputs) node.outputs = [];
    if (!node.outputs[index]) node.addOutput(name, type);

    const output = node.outputs[index];
    output.name = name;
    output.label = name;
    output.localized_name = name;
    output.type = type;
    output.zmongoContentPackPath = path;
    output.zmongoContentPackValue = value;
    output.zmongoContentPackValueDisplay = valueDisplay;
    output.zmongoContentPackGroup = field.groupName;
    output.zmongoContentPackField = {
        path,
        group_name: field.groupName,
        value,
        value_display: valueDisplay,
        value_type: field.item?.value_type || "",
    };
}

function rebuildOutputs(node, fields, force = false) {
    if (node.zmongoRebuildingContentPackOutputs) return false;
    node.zmongoRebuildingContentPackOutputs = true;

    try {
        const current = outputSignature(node);
        const desired = desiredSignature(fields);
        if (!force && current === desired) return false;

        shrinkOutputs(node, fields.length);
        for (let i = 0; i < fields.length; i++) setOutput(node, i, fields[i]);
        shrinkOutputs(node, fields.length);
    } finally {
        node.zmongoRebuildingContentPackOutputs = false;
    }

    markDirty(node);
    return true;
}

function statusText(contentPack, fields) {
    const name = contentPack?.content_pack_name || "unnamed";
    const project = contentPack?.project_name || "default";
    const lines = fields.map((field, index) => `${index}: ${outputNameForField(field, index)} [${outputTypeForField(field)}]`);
    return `Content Pack: ${name}\nProject: ${project}\nDocument fields: ${fieldCount(contentPack)}\nVisible outputs: ${fields.length}\n\n${lines.join("\n")}`;
}

function rebuildDynamicContentPackOutputs(node, options = {}) {
    if (!node || node.zmongoRebuildingContentPackOutputs) return false;

    applyDynamicLayout(node);

    const contentPack = getContentPackFromDynamicNode(node);
    if (!contentPack) {
        shrinkOutputs(node, 0);
        setStatus(node, "No valid content pack JSON found. Execute the connected Build/Save/Load Content Pack node, or paste content_pack_json into this node.");
        markDirty(node);
        return false;
    }

    const fields = getFieldsFromContentPack(contentPack, node);
    cacheContentPackProperty(node, contentPack);

    if (!isInputLinked(node, "content_pack_json")) {
        setWidgetValue(node, "content_pack_json", toPrettyJson(contentPack), true);
    }

    if (!fields.length) {
        shrinkOutputs(node, 0);
        setStatus(node, `Content pack loaded, but no visible fields matched include_images/include_metadata.\nDocument fields: ${fieldCount(contentPack)}`);
        markDirty(node);
        return false;
    }

    rebuildOutputs(node, fields, !!options.forceRebuild);
    setStatus(node, statusText(contentPack, fields));

    try { node.size = node.computeSize?.() || node.size; } catch {}
    markDirty(node);
    return true;
}

function cacheProducerFromMessage(node, message) {
    const contentPack = extractContentPackCandidate(message);
    if (!contentPack) {
        console.warn("[ComfyUI-ZMongo] No content pack JSON found in executed message.", message);
        return false;
    }

    cacheContentPackProperty(node, contentPack);

    setWidgetValue(node, "content_pack_json", toPrettyJson(contentPack), true);
    setWidgetValue(node, "content_pack_source_json", toPrettyJson(contentPack), true);
    setWidgetValue(node, "cached_content_pack_json", toPrettyJson(contentPack), true);
    return true;
}

function rebuildDownstreamDynamicNodes(originNode) {
    if (!originNode?.outputs || !app.graph) return;

    for (const output of originNode.outputs) {
        const links = Array.isArray(output.links) ? output.links : [];
        for (const linkId of links) {
            const link = app.graph.links?.[linkId];
            if (!link) continue;

            const target = app.graph.getNodeById(link.target_id);
            if (!target || target.comfyClass !== DYNAMIC_NODE_TYPE) continue;

            rebuildDynamicContentPackOutputs(target, { forceRebuild: true });
            setTimeout(() => rebuildDynamicContentPackOutputs(target, { forceRebuild: true }), 100);
            setTimeout(() => rebuildDynamicContentPackOutputs(target, { forceRebuild: true }), 500);
        }
    }
}

function isInputConnectionChange(type, inputOrOutput) {
    if (inputOrOutput?.name === "content_pack_json") return true;
    if (typeof LiteGraph !== "undefined" && type === LiteGraph.INPUT) return true;
    return type === 1;
}

function isContentPackInputConnectionChange(node, slotIndex, inputOrOutput) {
    if (inputOrOutput?.name === "content_pack_json") return true;
    if (!node.inputs || slotIndex == null || slotIndex < 0) return false;
    return node.inputs[slotIndex]?.name === "content_pack_json";
}

function hookDynamicWidgetCallbacks(node) {
    for (const widgetName of ["content_pack_json", "include_images", "include_metadata", "cached_content_pack_json", "resolved_content_pack_json", "runtime_content_pack_json"]) {
        const widget = getWidget(node, widgetName);
        if (!widget || widget.zmongoContentPackHooked) continue;

        widget.zmongoContentPackHooked = true;
        const originalCallback = widget.callback;
        widget.callback = function () {
            if (node.zmongoSuppressContentPackCallbacks) return;
            originalCallback?.apply(this, arguments);
            setTimeout(() => rebuildDynamicContentPackOutputs(node, { forceRebuild: true }), 50);
        };
    }
}

function installRebuildButton(node) {
    const existing = getWidget(node, "Rebuild Content Pack Outputs");
    if (existing) {
        existing.callback = () => rebuildDynamicContentPackOutputs(node, { forceRebuild: true });
        return;
    }

    node.addWidget("button", "Rebuild Content Pack Outputs", null, () => {
        rebuildDynamicContentPackOutputs(node, { forceRebuild: true });
    });
}

function scheduleDynamicRebuild(node) {
    setTimeout(() => rebuildDynamicContentPackOutputs(node, { forceRebuild: true }), 100);
    setTimeout(() => rebuildDynamicContentPackOutputs(node, { forceRebuild: true }), 500);
    setTimeout(() => rebuildDynamicContentPackOutputs(node, { forceRebuild: true }), 1500);
}

function installProducerHooks(nodeType) {
    if (nodeType.prototype.zmongoContentPackProducerHooksInstalled) return;
    nodeType.prototype.zmongoContentPackProducerHooksInstalled = true;

    const originalOnExecuted = nodeType.prototype.onExecuted;
    nodeType.prototype.onExecuted = function (message) {
        originalOnExecuted?.apply(this, arguments);
        if (cacheProducerFromMessage(this, message)) rebuildDownstreamDynamicNodes(this);
    };
}

function installDynamicHooks(nodeType) {
    if (nodeType.prototype.zmongoDynamicContentPackHooksInstalled) return;
    nodeType.prototype.zmongoDynamicContentPackHooksInstalled = true;

    const originalOnNodeCreated = nodeType.prototype.onNodeCreated;
    nodeType.prototype.onNodeCreated = function () {
        originalOnNodeCreated?.apply(this, arguments);
        applyDynamicLayout(this);
        hookDynamicWidgetCallbacks(this);
        installRebuildButton(this);
        scheduleDynamicRebuild(this);
    };

    const originalOnConfigure = nodeType.prototype.onConfigure;
    nodeType.prototype.onConfigure = function () {
        originalOnConfigure?.apply(this, arguments);
        applyDynamicLayout(this);
        hookDynamicWidgetCallbacks(this);
        installRebuildButton(this);
        scheduleDynamicRebuild(this);
    };

    const originalOnConnectionsChange = nodeType.prototype.onConnectionsChange;
    nodeType.prototype.onConnectionsChange = function (type, slotIndex, isConnected, linkInfo, inputOrOutput) {
        originalOnConnectionsChange?.apply(this, arguments);
        if (this.zmongoRebuildingContentPackOutputs) return;
        if (!isInputConnectionChange(type, inputOrOutput)) return;
        if (!isContentPackInputConnectionChange(this, slotIndex, inputOrOutput)) return;
        scheduleDynamicRebuild(this);
    };

    const originalOnExecuted = nodeType.prototype.onExecuted;
    nodeType.prototype.onExecuted = function (message) {
        originalOnExecuted?.apply(this, arguments);
        const contentPack = extractContentPackCandidate(message);
        if (contentPack) cacheContentPackProperty(this, contentPack);
        scheduleDynamicRebuild(this);
    };
}

app.registerExtension({
    name: "BusinessProcessApplications.ZMongo.DynamicContentPackOutputs.Fixed",

    async beforeRegisterNodeDef(nodeType, nodeData) {
        const nodeName = nodeData.name;

        if (PRODUCER_TYPES.has(nodeName)) {
            installProducerHooks(nodeType);
            return;
        }

        if (nodeName === DYNAMIC_NODE_TYPE) installDynamicHooks(nodeType);
    },
});