import { app } from "../../scripts/app.js";

app.registerExtension({
    name: "ZMongo.DropdownPicker",

    async beforeRegisterNodeDef(nodeType, nodeData) {
        if (nodeData.name !== "ZMongoDropdownPickerNode") {
            return;
        }

        const onExecuted = nodeType.prototype.onExecuted;
        nodeType.prototype.onExecuted = function (message) {
            try {
                const dropdownWidget = this.widgets?.find(w => w.name === "selected_option");
                if (!dropdownWidget) {
                    if (onExecuted) onExecuted.apply(this, arguments);
                    return;
                }

                const ui = message?.ui || {};
                const items = ui.dropdown_items || [];
                const selected = ui.selected_option || "";

                if (Array.isArray(items) && items.length > 0) {
                    dropdownWidget.options.values = items;
                } else {
                    dropdownWidget.options.values = ["<no_items>"];
                }

                if (selected && dropdownWidget.options.values.includes(selected)) {
                    dropdownWidget.value = selected;
                } else if (dropdownWidget.options.values.length > 0) {
                    dropdownWidget.value = dropdownWidget.options.values[0];
                }
            } catch (err) {
                console.error("ZMongo.DropdownPicker update failed", err);
            }

            if (onExecuted) {
                onExecuted.apply(this, arguments);
            }
        };
    },
});