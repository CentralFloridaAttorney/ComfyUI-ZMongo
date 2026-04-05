import { app } from "../../scripts/app.js";

function findWidget(node, name) {
    return node.widgets?.find((w) => w.name === name);
}

async function fetchFlattenedFields(collectionName) {
    if (!collectionName || String(collectionName).startsWith("<")) {
        return [];
    }

    const response = await fetch(
        `/zmongo/flattened_fields?collection_name=${encodeURIComponent(collectionName)}`
    );

    if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    if (!data.success || !Array.isArray(data.fields)) {
        return [];
    }

    return data.fields;
}

function setComboValues(widget, values, preferredValue = null) {
    const safeValues = values.length ? values : ["<no_fields_found>"];

    widget.options = widget.options || {};
    widget.options.values = safeValues;

    if (preferredValue && safeValues.includes(preferredValue)) {
        widget.value = preferredValue;
    } else if (!safeValues.includes(widget.value)) {
        widget.value = safeValues[0];
    }
}

function clampIndex(idx, len) {
    if (!len || len <= 0) return 0;
    if (Number.isNaN(idx)) return 0;
    return Math.max(0, Math.min(idx, len - 1));
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
            return;
        }

        let isUpdating = false;

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

            let idx = Number(indexWidget.value ?? 0);
            idx = clampIndex(idx, values.length);
            indexWidget.value = idx;
            fieldWidget.value = values[idx];
            node.setDirtyCanvas(true, true);
        };

        const refreshFields = async ({ preserveField = true } = {}) => {
            if (isUpdating) return;
            isUpdating = true;

            try {
                const collectionName = collectionWidget.value;
                const oldFieldValue = fieldWidget.value;
                const fields = await fetchFlattenedFields(collectionName);

                let preferredValue = null;
                if (
                    preserveField &&
                    oldFieldValue &&
                    !String(oldFieldValue).startsWith("<") &&
                    fields.includes(oldFieldValue)
                ) {
                    preferredValue = oldFieldValue;
                }

                setComboValues(fieldWidget, fields, preferredValue);

                const values = fieldWidget.options?.values || [];
                let idx = Number(indexWidget.value ?? 0);
                idx = clampIndex(idx, values.length);
                indexWidget.value = idx;

                if (!preferredValue) {
                    fieldWidget.value = values[idx] ?? "<no_fields_found>";
                }

                syncIndexFromField();
                node.setDirtyCanvas(true, true);
            } catch (err) {
                console.error("ZMongo flattened field dropdown refresh failed:", err);
                setComboValues(fieldWidget, ["<error_loading_fields>"]);
                fieldWidget.value = "<error_loading_fields>";
                indexWidget.value = 0;
                node.setDirtyCanvas(true, true);
            } finally {
                isUpdating = false;
            }
        };

        const originalCollectionCallback = collectionWidget.callback;
        collectionWidget.callback = async (...args) => {
            if (originalCollectionCallback) {
                originalCollectionCallback.apply(collectionWidget, args);
            }
            await refreshFields({ preserveField: false });
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

        // Initial population from the default selected collection.
        await refreshFields({ preserveField: false });

        // Run again on next tick because ComfyUI sometimes finishes widget restoration
        // after nodeCreated, which can otherwise leave the placeholder stuck.
        setTimeout(async () => {
            await refreshFields({ preserveField: false });
        }, 0);

        // One more delayed refresh for restored workflows / startup timing issues.
        setTimeout(async () => {
            await refreshFields({ preserveField: false });
        }, 150);
    },
});