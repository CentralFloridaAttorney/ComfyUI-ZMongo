import { app } from "../../scripts/app.js";

const SAVE_NODE_TYPE = "ZMongoSavePresetByNodeID";
const LOAD_NODE_TYPE = "ZMongoLoadPreset";
const DYNAMIC_NODE_TYPE = "ZMongoDynamicPresetOutputs";
const MAX_DYNAMIC_OUTPUTS = 64;
const MAX_DISPLAY_VALUE_LENGTH = 48;
const TEXTAREA_LINE_HEIGHT = 18;
const TEXTAREA_PADDING = 18;
const TEXTAREA_LINES = 10;
const TEXTAREA_HEIGHT = TEXTAREA_LINE_HEIGHT * TEXTAREA_LINES + TEXTAREA_PADDING;

function parsePresetJson(value) {
    if (!value) return null;

    if (Array.isArray(value)) value = value[0];

    if (typeof value === "object") {
        return Array.isArray(value.fields) ? value : null;
    }

    if (typeof value !== "string") return null;

    const text = value.trim();
    if (!text || text === "{}") return null;

    try {
        const parsed = JSON.parse(text);
        return parsed && typeof parsed === "object" && Array.isArray(parsed.fields)
            ? parsed
            : null;
    } catch {
        return null;
    }
}

function presetToText(preset) {
    return JSON.stringify(preset || {}, null, 2);
}

function extractPresetJsonCandidate(message) {
    const candidates = [
        message?.preset_json,
        message?.ui?.preset_json,
        message?.output?.preset_json,
        message?.outputs?.preset_json,
        message?.result?.preset_json,
    ];

    for (const candidate of candidates) {
        const parsed = parsePresetJson(candidate);
        if (parsed) return parsed;
    }

    const resultCandidates = [
        message?.result,
        message?.output?.result,
        message?.outputs?.result,
    ];

    for (const candidate of resultCandidates) {
        if (Array.isArray(candidate) && candidate.length > 0) {
            const parsed = parsePresetJson(candidate[0]);
            if (parsed) return parsed;
        }
    }

    if (message && typeof message === "object") {
        for (const value of Object.values(message)) {
            const parsed = parsePresetJson(value);
            if (parsed) return parsed;

            if (value && typeof value === "object") {
                for (const innerValue of Object.values(value)) {
                    const innerParsed = parsePresetJson(innerValue);
                    if (innerParsed) return innerParsed;
                }
            }
        }
    }

    return null;
}

function getFieldsFromPreset(preset) {
    if (!preset || !Array.isArray(preset.fields)) return [];

    return preset.fields
        .filter((field) => field && typeof field === "object")
        .filter((field) => field.input_name)
        .slice(0, MAX_DYNAMIC_OUTPUTS);
}

function normalizeOutputType(field) {
    const declaredType = String(field?.declared_type || "").trim();

    /*
     * Critical ComfyUI compatibility fix:
     * Dropdown widget values are saved as declared_type COMBO, but many native
     * ComfyUI combo inputs do not accept an output socket typed literally as
     * "COMBO". They expect a normal runtime value and frontend compatibility
     * is safest when the output socket is flexible.
     *
     * This fixes presets for:
     * - CheckpointLoaderSimple.ckpt_name
     * - KSampler.sampler_name
     * - KSampler.scheduler
     * - Any runtime dropdown ending in _name
     */
    if (declaredType === "COMBO") return "*";

    if (declaredType === "INT") return "INT";
    if (declaredType === "FLOAT") return "FLOAT";
    if (declaredType === "BOOLEAN") return "BOOLEAN";
    if (declaredType === "STRING") return "STRING";
    if (!declaredType || declaredType === "UNKNOWN") return "*";

    return declaredType;
}

function formatDisplayValue(value) {
    if (value === null || value === undefined) return "null";

    let text;

    if (typeof value === "string") {
        text = value;
    } else if (typeof value === "number" || typeof value === "boolean") {
        text = String(value);
    } else {
        try {
            text = JSON.stringify(value);
        } catch {
            text = String(value);
        }
    }

    text = text.replace(/\s+/g, " ").trim();

    if (text.length > MAX_DISPLAY_VALUE_LENGTH) {
        text = `${text.slice(0, MAX_DISPLAY_VALUE_LENGTH - 1)}…`;
    }

    return text;
}

function getDisplayOutputName(field) {
    return `${String(field?.input_name || "output")} = ${formatDisplayValue(field?.value)}`;
}

function getWidget(node, widgetName) {
    return node.widgets?.find((widget) => widget.name === widgetName) || null;
}

function getWidgetValue(node, widgetName) {
    const widget = getWidget(node, widgetName);
    return widget?.value ?? "";
}

function setWidgetValue(node, widgetName, value, options = {}) {
    const widget = getWidget(node, widgetName);
    if (!widget) return;

    if (widget.value === value) return;

    if (options.silent) {
        node.zmongoSuppressWidgetCallbacks = true;
        try {
            widget.value = value;
        } finally {
            node.zmongoSuppressWidgetCallbacks = false;
        }
        return;
    }

    widget.value = value;
}

function getWidgetDomCandidates(widget) {
    return [
        widget?.inputEl,
        widget?.element,
        widget?.textarea,
        widget?.domElement,
        widget?.input,
    ].filter(Boolean);
}

function forceWidgetDomStyle(widget, styles = {}) {
    for (const element of getWidgetDomCandidates(widget)) {
        try {
            Object.assign(element.style, styles);
        } catch {
            // Different ComfyUI frontend builds expose different widget DOM fields.
        }
    }
}

function hideInternalWidget(node, widgetName) {
    const widget = getWidget(node, widgetName);
    if (!widget) return;

    widget.zmongoHidden = true;
    widget.type = "hidden";
    widget.options = {
        ...(widget.options || {}),
        hidden: true,
        serialize: true,
        multiline: false,
    };

    widget.computeSize = () => [0, 0];
    widget.draw = () => {};

    forceWidgetDomStyle(widget, {
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
}

function constrainMultilineWidget(node, widgetName, lines = TEXTAREA_LINES) {
    const widget = getWidget(node, widgetName);
    if (!widget) return;

    const height = TEXTAREA_LINE_HEIGHT * lines + TEXTAREA_PADDING;

    if (widget.type === "hidden") {
        widget.type = "text";
    }

    widget.options = {
        ...(widget.options || {}),
        multiline: true,
    };

    widget.computeSize = function (width) {
        const nodeWidth = node?.size?.[0] || width || 420;
        return [Math.max(260, nodeWidth - 20), height];
    };

    forceWidgetDomStyle(widget, {
        height: `${height}px`,
        maxHeight: `${height}px`,
        minHeight: `${height}px`,
        overflowY: "auto",
        overflowX: "auto",
        resize: "none",
        whiteSpace: "pre",
        boxSizing: "border-box",
    });

    for (const element of getWidgetDomCandidates(widget)) {
        try {
            element.rows = lines;
        } catch {
            // Ignore.
        }
    }
}

function applyDynamicNodeWidgetLayout(node) {
    /*
     * Root cause fix:
     * cached_preset_json contains the huge full JSON cache. It must never
     * render. preset_json is allowed to render, but only as a 10-line scroller.
     */
    constrainMultilineWidget(node, "preset_json", 10);
    constrainMultilineWidget(node, "dynamic_status", 10);

    hideInternalWidget(node, "cached_preset_json");
    hideInternalWidget(node, "resolved_preset_json");
    hideInternalWidget(node, "runtime_preset_json");

    node.setDirtyCanvas?.(true, true);
}

function getPresetJsonInput(node) {
    return node.inputs?.find((input) => input.name === "preset_json") || null;
}

function isPresetJsonLinked(node) {
    const input = getPresetJsonInput(node);
    return !!input && input.link != null;
}

function getPresetFromOwnCachedWidget(node) {
    return (
        parsePresetJson(getWidgetValue(node, "cached_preset_json")) ||
        parsePresetJson(getWidgetValue(node, "resolved_preset_json")) ||
        parsePresetJson(getWidgetValue(node, "runtime_preset_json"))
    );
}

function getPresetFromOwnPresetWidget(node) {
    return parsePresetJson(getWidgetValue(node, "preset_json"));
}

function getLinkedOriginNode(node, inputName) {
    if (!node.inputs || !app.graph) return null;

    const input = node.inputs.find((item) => item.name === inputName);
    if (!input || input.link == null) return null;

    const link = app.graph.links?.[input.link];
    if (!link) return null;

    return app.graph.getNodeById(link.origin_id);
}

function getPresetFromLinkedUpstream(node) {
    const originNode = getLinkedOriginNode(node, "preset_json");
    if (!originNode) return null;

    return (
        parsePresetJson(originNode.zmongoLastPresetJson) ||
        parsePresetJson(originNode.zmongoLastUiPresetJson) ||
        parsePresetJson(getWidgetValue(originNode, "cached_preset_json")) ||
        parsePresetJson(getWidgetValue(originNode, "preset_json"))
    );
}

function getPresetForDynamicNode(node) {
    if (isPresetJsonLinked(node)) {
        return (
            getPresetFromLinkedUpstream(node) ||
            getPresetFromOwnCachedWidget(node) ||
            getPresetFromOwnPresetWidget(node) ||
            null
        );
    }

    return (
        getPresetFromOwnPresetWidget(node) ||
        getPresetFromOwnCachedWidget(node) ||
        getPresetFromLinkedUpstream(node) ||
        null
    );
}

function outputTypeSignature(typeValue) {
    if (Array.isArray(typeValue)) return JSON.stringify(typeValue);
    return String(typeValue || "*");
}

function getDesiredSignature(fields) {
    return fields
        .map((field) => {
            const rawName = String(field.input_name || "");
            const displayName = getDisplayOutputName(field);
            const type = outputTypeSignature(normalizeOutputType(field));
            const value = formatDisplayValue(field.value);
            return `${rawName}:${displayName}:${type}:${value}`;
        })
        .join("|");
}

function getCurrentSignature(node) {
    if (!node.outputs || node.outputs.length === 0) return "";

    return node.outputs
        .map((output) => {
            const rawName = String(output.zmongoPresetField?.input_name || output.name || "");
            const displayName = String(output.name || "");
            const type = outputTypeSignature(output.type || "*");
            const value = formatDisplayValue(output.zmongoPresetField?.value);
            return `${rawName}:${displayName}:${type}:${value}`;
        })
        .join("|");
}

function setOutputMetadata(output, field) {
    const rawName = String(field.input_name);
    const displayName = getDisplayOutputName(field);
    const type = normalizeOutputType(field);

    output.name = displayName;
    output.label = displayName;
    output.localized_name = displayName;
    output.type = type;

    output.zmongoPresetField = {
        input_name: rawName,
        display_name: displayName,
        declared_type: field.declared_type || "",
        widget_kind: field.widget_kind || "",
        value: field.value,
        value_display: formatDisplayValue(field.value),
        options_snapshot: Array.isArray(field.options_snapshot) ? field.options_snapshot : [],
        options_source: field.options_source || null,
        source_node_class: field.source_node_class || null,
    };
}

function rebuildOutputsPreservingLinks(node, fields) {
    node.zmongoRebuildingOutputs = true;

    try {
        for (let index = 0; index < fields.length; index++) {
            const field = fields[index];
            const displayName = getDisplayOutputName(field);
            const type = normalizeOutputType(field);

            if (!node.outputs || !node.outputs[index]) {
                node.addOutput(displayName, type);
            }

            setOutputMetadata(node.outputs[index], field);
        }

        while (node.outputs && node.outputs.length > fields.length) {
            const lastOutput = node.outputs[node.outputs.length - 1];

            if (Array.isArray(lastOutput.links) && lastOutput.links.length > 0) {
                break;
            }

            node.removeOutput(node.outputs.length - 1);
        }
    } finally {
        node.zmongoRebuildingOutputs = false;
    }
}

function setNodeStatusWidget(node, text) {
    let widget = getWidget(node, "dynamic_status");

    if (!widget) {
        node.addWidget("text", "dynamic_status", text, () => {}, { multiline: true });
        widget = getWidget(node, "dynamic_status");
    } else {
        widget.value = text;
    }

    constrainMultilineWidget(node, "dynamic_status", 10);
}

function copyPresetIntoCache(node, preset) {
    if (!preset) return;

    const text = presetToText(preset);

    setWidgetValue(node, "cached_preset_json", text, { silent: true });
    setWidgetValue(node, "resolved_preset_json", text, { silent: true });
    setWidgetValue(node, "runtime_preset_json", text, { silent: true });

    if (!isPresetJsonLinked(node)) {
        setWidgetValue(node, "preset_json", text, { silent: true });
    }

    node.zmongoLastPresetJson = text;
    node.zmongoLastUiPresetJson = text;

    applyDynamicNodeWidgetLayout(node);
}

function makeStatusFieldList(fields) {
    return fields
        .map((field) => `${String(field.input_name || "")} = ${formatDisplayValue(field.value)}`)
        .join("\n");
}

function updateStatus(node, preset, fields) {
    const sourceClass = preset?.source_node_class || "unknown";
    const presetName = preset?.preset_name || "unnamed";
    const fieldList = makeStatusFieldList(fields);

    setNodeStatusWidget(
        node,
        `Preset: ${presetName}\nSource node: ${sourceClass}\nOutputs: ${fields.length}\n\n${fieldList}`
    );
}

function rebuildDynamicPresetOutputs(node, options = {}) {
    if (node.zmongoRebuildingOutputs) return false;

    applyDynamicNodeWidgetLayout(node);

    const preset = getPresetForDynamicNode(node);
    const fields = getFieldsFromPreset(preset);

    if (!preset) {
        setNodeStatusWidget(
            node,
            "No valid preset JSON found. Execute the connected Save/Load Preset node first, or paste preset JSON into this node."
        );
        applyDynamicNodeWidgetLayout(node);
        return false;
    }

    if (fields.length === 0) {
        setNodeStatusWidget(node, "Preset loaded, but it contains no fields.");
        copyPresetIntoCache(node, preset);
        applyDynamicNodeWidgetLayout(node);
        return false;
    }

    const desiredSignature = getDesiredSignature(fields);
    const currentSignature = getCurrentSignature(node);

    if (desiredSignature !== currentSignature || options.forceRebuild) {
        rebuildOutputsPreservingLinks(node, fields);
    }

    copyPresetIntoCache(node, preset);
    updateStatus(node, preset, fields);
    applyDynamicNodeWidgetLayout(node);

    node.size = node.computeSize();
    node.setDirtyCanvas?.(true, true);
    return true;
}

function rebuildDownstreamDynamicNodes(originNode) {
    if (!originNode?.outputs || !app.graph) return;

    for (const output of originNode.outputs) {
        if (!output?.links) continue;

        for (const linkId of output.links) {
            const link = app.graph.links?.[linkId];
            if (!link) continue;

            const targetNode = app.graph.getNodeById(link.target_id);
            if (!targetNode || targetNode.comfyClass !== DYNAMIC_NODE_TYPE) continue;

            rebuildDynamicPresetOutputs(targetNode, { forceRebuild: true });
        }
    }
}

function cachePresetFromExecutedMessage(node, message) {
    const parsed = extractPresetJsonCandidate(message);

    if (!parsed) {
        console.warn("[ComfyUI-ZMongo] No preset_json found in executed message.", message);
        return false;
    }

    const text = presetToText(parsed);
    node.zmongoLastPresetJson = text;
    node.zmongoLastUiPresetJson = text;

    setWidgetValue(node, "cached_preset_json", text, { silent: true });
    setWidgetValue(node, "preset_json", text, { silent: true });

    return true;
}

function isInputConnectionChange(type, inputOrOutput) {
    if (inputOrOutput?.name === "preset_json") return true;

    if (typeof LiteGraph !== "undefined" && type === LiteGraph.INPUT) {
        return true;
    }

    return type === 1;
}

function isPresetJsonInputConnectionChange(node, slotIndex, inputOrOutput) {
    if (inputOrOutput?.name === "preset_json") return true;

    if (!node.inputs || slotIndex == null || slotIndex < 0) return false;

    return node.inputs[slotIndex]?.name === "preset_json";
}

function installPresetProducerHooks(nodeType) {
    if (nodeType.prototype.zmongoPresetProducerHooksInstalled) return;

    nodeType.prototype.zmongoPresetProducerHooksInstalled = true;

    const originalOnExecuted = nodeType.prototype.onExecuted;

    nodeType.prototype.onExecuted = function (message) {
        originalOnExecuted?.apply(this, arguments);

        if (cachePresetFromExecutedMessage(this, message)) {
            rebuildDownstreamDynamicNodes(this);
        }
    };
}

function installDynamicNodeHooks(nodeType) {
    if (nodeType.prototype.zmongoDynamicHooksInstalled) return;

    nodeType.prototype.zmongoDynamicHooksInstalled = true;

    const originalOnNodeCreated = nodeType.prototype.onNodeCreated;

    nodeType.prototype.onNodeCreated = function () {
        originalOnNodeCreated?.apply(this, arguments);

        const node = this;

        applyDynamicNodeWidgetLayout(node);

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(node);
            rebuildDynamicPresetOutputs(node, { forceRebuild: true });
        }, 100);

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(node);
        }, 500);

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(node);
        }, 1500);

        for (const widgetName of ["preset_json", "cached_preset_json", "dynamic_status"]) {
            const widget = getWidget(node, widgetName);
            if (!widget) continue;

            const originalCallback = widget.callback;

            widget.callback = function () {
                if (node.zmongoSuppressWidgetCallbacks) return;

                originalCallback?.apply(this, arguments);

                applyDynamicNodeWidgetLayout(node);

                if (widgetName === "preset_json" || widgetName === "cached_preset_json") {
                    rebuildDynamicPresetOutputs(node, { forceRebuild: true });
                }
            };
        }

        if (!getWidget(node, "Rebuild Preset Outputs")) {
            node.addWidget("button", "Rebuild Preset Outputs", null, () => {
                applyDynamicNodeWidgetLayout(node);
                rebuildDynamicPresetOutputs(node, { forceRebuild: true });
            });
        }
    };

    const originalOnConfigure = nodeType.prototype.onConfigure;

    nodeType.prototype.onConfigure = function () {
        originalOnConfigure?.apply(this, arguments);

        applyDynamicNodeWidgetLayout(this);

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(this);
            rebuildDynamicPresetOutputs(this, { forceRebuild: true });
        }, 100);

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(this);
        }, 500);

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(this);
        }, 1500);
    };

    const originalOnConnectionsChange = nodeType.prototype.onConnectionsChange;

    nodeType.prototype.onConnectionsChange = function (
        type,
        slotIndex,
        isConnected,
        linkInfo,
        inputOrOutput
    ) {
        originalOnConnectionsChange?.apply(this, arguments);

        if (this.zmongoRebuildingOutputs) return;
        if (!isInputConnectionChange(type, inputOrOutput)) return;
        if (!isPresetJsonInputConnectionChange(this, slotIndex, inputOrOutput)) return;

        setTimeout(() => {
            applyDynamicNodeWidgetLayout(this);
            rebuildDynamicPresetOutputs(this, { forceRebuild: true });
        }, 100);
    };
}

app.registerExtension({
    name: "BusinessProcessApplications.ZMongo.DynamicPresetOutputs",

    async beforeRegisterNodeDef(nodeType, nodeData) {
        const nodeName = nodeData.name;

        if (nodeName === SAVE_NODE_TYPE || nodeName === LOAD_NODE_TYPE) {
            installPresetProducerHooks(nodeType);
            return;
        }

        if (nodeName === DYNAMIC_NODE_TYPE) {
            installDynamicNodeHooks(nodeType);
        }
    },
});
