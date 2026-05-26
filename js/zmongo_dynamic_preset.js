import { app } from "../../scripts/app.js";
import { api } from "../../scripts/api.js";

app.registerExtension({
    name: "ComfyUI.ZMongo.DynamicPreset",
    async beforeRegisterNodeDef(nodeType, nodeData, app) {
        if (nodeData.name === "ZMongoDynamicNodePreset") {
            const onNodeCreated = nodeType.prototype.onNodeCreated;

            nodeType.prototype.onNodeCreated = function () {
                if (onNodeCreated) onNodeCreated.apply(this, arguments);

                // Add a hidden widget to store the list of output keys
                let outputKeysWidget = this.widgets.find(w => w.name === "output_keys");
                if (!outputKeysWidget) {
                    outputKeysWidget = this.addWidget("string", "output_keys", "", () => {});
                }
                outputKeysWidget.type = "hidden"; // Hide from the user
                outputKeysWidget.computeSize = () => [0, -4]; // Remove visual footprint

                // Add a button to trigger the schema hydration
                this.addWidget("button", "🔄 Hydrate Sockets from Schema", "hydrate", async () => {
                    const nodeClassWidget = this.widgets.find(w => w.name === "node_class");
                    if (!nodeClassWidget || !nodeClassWidget.value) return;

                    const nodeClass = nodeClassWidget.value;

                    // Simulated Schema Discovery. In a full production environment,
                    // this fetches the `comfy_node_schemas` doc for the targeted class.
                    let schemaInputs = {};
                    if (nodeClass === "KSampler") {
                        schemaInputs = {
                            "seed": "INT", "steps": "INT", "cfg": "FLOAT",
                            "sampler_name": "STRING", "scheduler": "STRING", "denoise": "FLOAT"
                        };
                    } else if (nodeClass === "EmptySD3LatentImage") {
                        schemaInputs = { "width": "INT", "height": "INT", "batch_size": "INT" };
                    } else if (nodeClass === "CLIPTextEncode") {
                        schemaInputs = { "text": "STRING" };
                    }

                    // 1. Remove existing dynamic outputs to prevent duplicate stacking
                    while (this.outputs && this.outputs.length > 0) {
                        this.removeOutput(0);
                    }

                    // 2. Rebuild the draggable output sockets based on the schema types
                    const newKeys = [];
                    for (const [key, type] of Object.entries(schemaInputs)) {
                        this.addOutput(key, type);
                        newKeys.push(key);
                    }

                    // 3. Save the comma-separated keys to the hidden widget so Python knows the return order
                    outputKeysWidget.value = newKeys.join(",");

                    // Resize node to fit new sockets and refresh canvas
                    this.size = this.computeSize();
                    app.graph.setDirtyCanvas(true, true);
                });
            };
        }
    }
});