import { app } from "../../../scripts/app.js";

function safeJsonParse(text) {
    if (!text || typeof text !== "string") return {};
    try {
        return JSON.parse(text);
    } catch {
        return {};
    }
}

function extractSource(payload) {
    if (payload && typeof payload === "object" && !Array.isArray(payload)) {
        if ("data" in payload && Object.keys(payload).length <= 6) {
            return extractSource(payload.data);
        }
        if ("document" in payload) return payload.document;
        if ("documents" in payload) return payload.documents;
    }
    return payload;
}

function flattenKeys(data, parentKey = "", out = new Set()) {
    if (Array.isArray(data)) {
        data.slice(0, 50).forEach((item, index) => {
            const key = parentKey ? `${parentKey}.${index}` : `${index}`;
            if (item && typeof item === "object") {
                flattenKeys(item, key, out);
            } else {
                out.add(key);
            }
        });
        return out;
    }

    if (data && typeof data === "object") {
        Object.entries(data).forEach(([key, value]) => {
            const nextKey = parentKey ? `${parentKey}.${key}` : key;
            if (value && typeof value === "object") {
                flattenKeys(value, nextKey, out);
            } else {
                out.add(nextKey);
            }
        });
        return out;
    }

    if (parentKey) {
        out.add(parentKey);
    }
    return out;
}

function getWidget(node, name) {
    return node.widgets?.find((w) => w.name === name);
}

app.registerExtension({
    name: "ZMongo.QueryBuilder",

    async beforeRegisterNodeDef(nodeType, nodeData) {
        if (nodeData.name !== "ZMongoQueryBuilderNode") {
            return;
        }

        const onNodeCreated = nodeType.prototype.onNodeCreated;
        nodeType.prototype.onNodeCreated = function () {
            const result = onNodeCreated ? onNodeCreated.apply(this, arguments) : undefined;

            const sampleWidget = getWidget(this, "sample_data_json");
            const selectedFieldWidget = getWidget(this, "selected_field");

            if (!sampleWidget || !selectedFieldWidget) {
                return result;
            }

            // Make selected_field behave like a combo box with dynamic values
            selectedFieldWidget.options = selectedFieldWidget.options || {};
            selectedFieldWidget.options.values = selectedFieldWidget.options.values || [""];

            const refreshFields = () => {
                const parsed = safeJsonParse(sampleWidget.value);
                const source = extractSource(parsed);
                const fields = Array.from(flattenKeys(source)).sort();

                const currentValue = selectedFieldWidget.value || "";
                const values = ["", ...fields];

                selectedFieldWidget.options.values = values;

                if (currentValue && values.includes(currentValue)) {
                    selectedFieldWidget.value = currentValue;
                } else if (!currentValue && fields.length > 0) {
                    selectedFieldWidget.value = fields[0];
                } else if (!values.includes(selectedFieldWidget.value)) {
                    selectedFieldWidget.value = "";
                }

                this.setDirtyCanvas(true, true);
            };

            const originalCallback = sampleWidget.callback;
            sampleWidget.callback = (...args) => {
                if (originalCallback) {
                    originalCallback.apply(sampleWidget, args);
                }
                refreshFields();
            };

            // Initial population
            refreshFields();

            return result;
        };
    },
});