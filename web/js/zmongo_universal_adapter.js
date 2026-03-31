import { app } from "../../../scripts/app.js";

app.registerExtension({
    name: "ZMongo.UniversalModelAdapter",

    async beforeRegisterNodeDef(nodeType, nodeData) {
        if (nodeData.name !== "ZMongoUniversalModelAdapterNode") {
            return;
        }

        const onNodeCreated = nodeType.prototype.onNodeCreated;
        nodeType.prototype.onNodeCreated = function () {
            const result = onNodeCreated ? onNodeCreated.apply(this, arguments) : undefined;

            this.zmongoActiveOutputs = [];
            this.zmongoCompatible = false;

            const statusWidget = this.addWidget(
                "text",
                "adapter_status",
                "No model metadata yet",
                () => {},
                { multiline: true }
            );
            statusWidget.readOnly = true;

            const updateVisualState = () => {
                const active = new Set(this.zmongoActiveOutputs || []);
                const outputs = this.outputs || [];

                for (const output of outputs) {
                    if (!output) continue;
                    const isActive = active.has(output.name);

                    // Visual hint only
                    output.label = isActive
                        ? `${output.name} ✓`
                        : `${output.name} ✗`;

                    output.color_on = isActive ? "#6aa84f" : "#666666";
                    output.color_off = isActive ? "#38761d" : "#333333";
                }

                this.bgcolor = this.zmongoCompatible ? "#1f3b24" : "#3b1f1f";
                this.setDirtyCanvas(true, true);
            };

            const onExecuted = this.onExecuted;
            this.onExecuted = function (message) {
                if (onExecuted) {
                    onExecuted.apply(this, arguments);
                }

                try {
                    // RETURN_NAMES order:
                    // model, clip, vae, status_text, active_outputs_json, is_compatible, model_type
                    const statusText = message?.text?.[0] ?? "";
                    const activeOutputsJson = message?.text?.[1] ?? "";
                    const compatible = message?.text?.[2];

                    const widget = this.widgets?.find((w) => w.name === "adapter_status");
                    if (widget) {
                        widget.value = statusText || "No status";
                    }

                    let parsed = {};
                    try {
                        parsed = activeOutputsJson ? JSON.parse(activeOutputsJson) : {};
                    } catch {
                        parsed = {};
                    }

                    this.zmongoActiveOutputs = parsed.active_outputs || [];
                    this.zmongoCompatible = Boolean(
                        parsed.compatible ?? compatible ?? false
                    );

                    updateVisualState();
                } catch (err) {
                    console.warn("ZMongo adapter UI update failed:", err);
                }
            };

            updateVisualState();
            return result;
        };
    },
});