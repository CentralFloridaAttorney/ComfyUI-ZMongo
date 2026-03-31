import { app } from "/scripts/app.js";

function findWidget(node, name) {
    return node.widgets?.find((w) => w.name === name);
}

async function fetchFlattenedFields(collectionName) {
    if (!collectionName || String(collectionName).startsWith("<")) {
        return [];
    }

    const url = `/zmongo/flattened_fields?collection_name=${encodeURIComponent(collectionName)}`;
    const response = await fetch(url);

    if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    if (!data.success || !Array.isArray(data.fields)) {
        return [];
    }

    return data.fields;
}

function setWidgetOptions(widget, values) {
    const safeValues = values.length ? values : ["<no_fields_found>"];

    widget.options = widget.options || {};
    widget.options.values = safeValues;

    if (!safeValues.includes(widget.value)) {
        widget.value = safeValues[0];
    }
}

function clampIndex(value, maxLen) {
    if (!maxLen || maxLen <= 0) {
        return 0;
    }
    const n = Number(value);
    if (Number.isNaN(n)) {
        return 0;
    }
    return Math.max(0, Math.min(n, maxLen - 1));
}

async function refreshFieldWidget(node, collectionWidget, fieldWidget, indexWidget) {
    const collectionName = collectionWidget.value;
    const fields = await fetchFlattenedFields(collectionName);

    setWidgetOptions(fieldWidget, fields);

    const values = fieldWidget.options?.values || [];
    const idx = clampIndex(indexWidget.value, values.length);

    indexWidget.value = idx;
    fieldWidget.value = values[idx] ?? "<no_fields_found>";

    node.setDirtyCanvas(true, true);
}

app.registerExtension({
    name: "ZMongo.FlattenedFieldDropdown",

    async nodeCreated(node) {
        if (node.comfyClass !== "ZMongoFlattenedFieldDropdownNode") {
            return;
        }

        const collectionWidget = findWidget(node, "collection_name");
        const fieldWidget = findWidget(node, "field_name");
        const indexWidget = findWidget(node, "field_index");

        if (!collectionWidget || !fieldWidget || !indexWidget) {
            console.warn("ZMongo node widgets not found.");
            return;
        }

        const syncIndexFromField = () => {
            const values = fieldWidget.options?.values || [];
            const idx = values.indexOf(fieldWidget.value);
            indexWidget.value = idx >= 0 ? idx : 0;
            node.setDirtyCanvas(true, true);
        };

        const syncFieldFromIndex = () => {
            const values = fieldWidget.options?.values || [];
            if (!values.length) {
                fieldWidget.value = "<no_fields_found>";
                indexWidget.value = 0;
                node.setDirtyCanvas(true, true);
                return;
            }

            const idx = clampIndex(indexWidget.value, values.length);
            indexWidget.value = idx;
            fieldWidget.value = values[idx];
            node.setDirtyCanvas(true, true);
        };

        const originalCollectionCallback = collectionWidget.callback;
        collectionWidget.callback = async (...args) => {
            if (originalCollectionCallback) {
                originalCollectionCallback.apply(collectionWidget, args);
            }

            try {
                await refreshFieldWidget(node, collectionWidget, fieldWidget, indexWidget);
            } catch (err) {
                console.error("Failed refreshing fields after collection change:", err);
            }
        };

        const originalFieldCallback = fieldWidget.callback;
        fieldWidget.callback = (...args) => {
            if (originalFieldCallback) {
                originalFieldCallback.apply(fieldWidget, args);
            }
            syncIndexFromField();
        };

        const originalIndexCallback = indexWidget.callback;
        indexWidget.callback = (...args) => {
            if (originalIndexCallback) {
                originalIndexCallback.apply(indexWidget, args);
            }
            syncFieldFromIndex();
        };

        // initial startup refresh from default selected collection
        try {
            await refreshFieldWidget(node, collectionWidget, fieldWidget, indexWidget);
        } catch (err) {
            console.error("Initial ZMongo field refresh failed:", err);
        }

        // delayed retry to handle restored workflow timing
        setTimeout(async () => {
            try {
                await refreshFieldWidget(node, collectionWidget, fieldWidget, indexWidget);
            } catch (err) {
                console.error("Delayed ZMongo field refresh failed:", err);
            }
        }, 200);
    },
});