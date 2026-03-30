import { app } from "../../../scripts/app.js";

function normalizeChoices(rawChoices) {
    if (!rawChoices) {
        return ["text"];
    }

    // If backend sent a plain list of strings:
    // ["text", "_id", "file_name"]
    if (Array.isArray(rawChoices) && rawChoices.length > 0 && typeof rawChoices[0] === "string") {
        return rawChoices.map(String);
    }

    // If backend sent nested list:
    // [["text", "_id", "file_name"]]
    if (
        Array.isArray(rawChoices) &&
        rawChoices.length > 0 &&
        Array.isArray(rawChoices[0])
    ) {
        return rawChoices[0].map(String);
    }

    // If backend sent a newline string
    if (typeof rawChoices === "string") {
        const lines = rawChoices
            .split("\n")
            .map((s) => s.trim())
            .filter(Boolean);
        return lines.length ? lines : ["text"];
    }

    return ["text"];
}

function normalizeSelectedPath(rawSelectedPath, choices) {
    if (typeof rawSelectedPath === "string" && choices.includes(rawSelectedPath)) {
        return rawSelectedPath;
    }

    if (
        Array.isArray(rawSelectedPath) &&
        rawSelectedPath.length > 0 &&
        typeof rawSelectedPath[0] === "string" &&
        choices.includes(rawSelectedPath[0])
    ) {
        return rawSelectedPath[0];
    }

    return choices[0] || "text";
}

function updateFieldPathWidget(node, rawChoices, rawSelectedPath) {
    if (!node || !node.widgets) return;

    const widget = node.widgets.find((w) => w && w.name === "field_path");
    if (!widget) return;

    const choices = normalizeChoices(rawChoices);
    const selectedPath = normalizeSelectedPath(rawSelectedPath, choices);

    if (!widget.options) {
        widget.options = {};
    }

    widget.options.values = choices;
    widget.value = selectedPath;

    if (typeof widget.callback === "function") {
        try {
            widget.callback(widget.value);
        } catch (err) {
            console.warn("ZMongoFieldSelector widget callback failed:", err);
        }
    }

    if (node.graph) {
        node.graph.setDirtyCanvas(true, true);
    }
}

app.registerExtension({
    name: "ComfyUI_ZMongo.FieldSelectorDropdown",

    async beforeRegisterNodeDef(nodeType, nodeData, appInstance) {
        if (nodeData.name !== "ZMongoFieldSelector") {
            return;
        }

        const originalOnExecuted = nodeType.prototype.onExecuted;

        nodeType.prototype.onExecuted = function (message) {
            if (originalOnExecuted) {
                originalOnExecuted.apply(this, arguments);
            }

            try {
                if (!message) return;

                // Support either ui payload shape or flat payload shape
                const rawChoices =
                    message.field_choices ??
                    message.ui?.field_choices ??
                    null;

                const rawSelectedPath =
                    message.selected_path ??
                    message.ui?.selected_path ??
                    null;

                updateFieldPathWidget(this, rawChoices, rawSelectedPath);
            } catch (err) {
                console.warn("ZMongoFieldSelector dropdown update failed:", err);
            }
        };
    },

    async nodeCreated(node) {
        if (node.comfyClass !== "ZMongoFieldSelector") {
            return;
        }

        const widget = node.widgets?.find((w) => w && w.name === "field_path");
        if (!widget) return;

        if (!widget.options) {
            widget.options = {};
        }

        if (!Array.isArray(widget.options.values) || widget.options.values.length === 0) {
            widget.options.values = ["text"];
        }

        if (!widget.value || !widget.options.values.includes(widget.value)) {
            widget.value = widget.options.values[0];
        }

        if (node.graph) {
            node.graph.setDirtyCanvas(true, true);
        }
    },
});