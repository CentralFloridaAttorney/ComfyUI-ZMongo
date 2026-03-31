import { app } from "/scripts/app.js";

function findWidget(node, name) {
    return node.widgets?.find((w) => w.name === name);
}

async function fetchJson(url) {
    const response = await fetch(url);
    if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
    }
    const data = await response.json();
    if (!data.success) {
        throw new Error(data.error || "Unknown error");
    }
    return data;
}

async function postJson(url, body) {
    const response = await fetch(url, {
        method: "POST",
        headers: {
            "Content-Type": "application/json",
        },
        body: JSON.stringify(body),
    });

    if (!response.ok) {
        throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();
    if (!data.success) {
        throw new Error(data.error || "Unknown error");
    }

    return data;
}

async function loadRecordEditorPayload(collectionName, recordId, selectedRecordJson) {
    const url =
        `/zmongo/record_editor/load?collection_name=${encodeURIComponent(collectionName || "")}` +
        `&record_id=${encodeURIComponent(recordId || "")}` +
        `&selected_record_json=${encodeURIComponent(selectedRecordJson || "")}`;

    return await fetchJson(url);
}

app.registerExtension({
    name: "ZMongo.RecordEditor",

    async nodeCreated(node) {
        if (node.comfyClass !== "ZMongoRecordEditorNode") {
            return;
        }

        const collectionWidget = findWidget(node, "collection_name");
        const recordIdWidget = findWidget(node, "record_id");
        const selectedRecordJsonWidget = findWidget(node, "selected_record_json");
        const saveStatusWidget = findWidget(node, "save_status_in");

        if (!collectionWidget || !recordIdWidget) {
            console.warn("ZMongoRecordEditorNode widgets not found.");
            return;
        }

        const container = document.createElement("div");
        container.style.display = "flex";
        container.style.flexDirection = "column";
        container.style.gap = "8px";
        container.style.width = "100%";
        container.style.minHeight = "320px";
        container.style.maxHeight = "620px";
        container.style.background = "#111";
        container.style.border = "1px solid #333";
        container.style.borderRadius = "8px";
        container.style.padding = "8px";
        container.style.boxSizing = "border-box";

        const infoBar = document.createElement("div");
        infoBar.style.fontSize = "12px";
        infoBar.style.color = "#aaa";
        infoBar.textContent = "Load a record to edit.";

        const toolbar = document.createElement("div");
        toolbar.style.display = "flex";
        toolbar.style.gap = "8px";
        toolbar.style.alignItems = "center";
        toolbar.style.flexWrap = "wrap";

        const loadButton = document.createElement("button");
        loadButton.textContent = "Load Record";
        loadButton.style.padding = "6px 10px";
        loadButton.style.borderRadius = "6px";
        loadButton.style.border = "1px solid #555";
        loadButton.style.background = "#2a2a2a";
        loadButton.style.color = "#eee";
        loadButton.style.cursor = "pointer";

        const saveButton = document.createElement("button");
        saveButton.textContent = "Save";
        saveButton.style.padding = "6px 10px";
        saveButton.style.borderRadius = "6px";
        saveButton.style.border = "1px solid #555";
        saveButton.style.background = "#2a2a2a";
        saveButton.style.color = "#eee";
        saveButton.style.cursor = "pointer";

        const saveStatusLabel = document.createElement("div");
        saveStatusLabel.style.fontSize = "11px";
        saveStatusLabel.style.color = "#8fb3ff";
        saveStatusLabel.textContent = "";

        toolbar.appendChild(loadButton);
        toolbar.appendChild(saveButton);

        const editorWrap = document.createElement("div");
        editorWrap.style.flex = "1";
        editorWrap.style.overflow = "auto";
        editorWrap.style.border = "1px solid #333";
        editorWrap.style.borderRadius = "6px";
        editorWrap.style.background = "#181818";
        editorWrap.style.padding = "8px";
        editorWrap.style.display = "flex";
        editorWrap.style.flexDirection = "column";
        editorWrap.style.gap = "8px";

        container.appendChild(infoBar);
        container.appendChild(toolbar);
        container.appendChild(saveStatusLabel);
        container.appendChild(editorWrap);

        if (node.addDOMWidget) {
            node.addDOMWidget("record_editor", "div", container, {
                serialize: false,
                hideOnZoom: false,
            });
        }

        let currentRecord = {};
        let currentFields = [];
        let fieldEditors = new Map();

        function getSelectedRecordJsonValue() {
            if (!selectedRecordJsonWidget) {
                return "";
            }
            return String(selectedRecordJsonWidget.value || "");
        }

        function getRecordIdValue() {
            return String(recordIdWidget.value || "").trim();
        }

        function buildFieldEditor(path, value) {
            const row = document.createElement("div");
            row.style.display = "flex";
            row.style.flexDirection = "column";
            row.style.gap = "4px";
            row.style.padding = "6px";
            row.style.border = "1px solid #2f2f2f";
            row.style.borderRadius = "6px";
            row.style.background = "#141414";

            const label = document.createElement("div");
            label.textContent = path;
            label.title = path;
            label.style.fontSize = "12px";
            label.style.fontWeight = "600";
            label.style.color = "#cfd8ff";
            label.style.wordBreak = "break-word";

            const editor = document.createElement("textarea");
            editor.value = value ?? "";
            editor.style.width = "100%";
            editor.style.minHeight = "48px";
            editor.style.maxHeight = "220px";
            editor.style.resize = "vertical";
            editor.style.padding = "6px 8px";
            editor.style.borderRadius = "6px";
            editor.style.border = "1px solid #444";
            editor.style.background = "#1b1b1b";
            editor.style.color = "#ddd";
            editor.style.whiteSpace = "pre-wrap";
            editor.style.overflowWrap = "anywhere";
            editor.style.boxSizing = "border-box";

            row.appendChild(label);
            row.appendChild(editor);

            return { row, editor };
        }

        function renderEditors(fields) {
            editorWrap.innerHTML = "";
            fieldEditors = new Map();

            if (!Array.isArray(fields) || !fields.length) {
                const empty = document.createElement("div");
                empty.textContent = "No editable fields loaded.";
                empty.style.color = "#888";
                empty.style.fontSize = "12px";
                editorWrap.appendChild(empty);
                return;
            }

            for (const field of fields) {
                const path = String(field.path || "");
                const value = String(field.value ?? "");
                const { row, editor } = buildFieldEditor(path, value);
                fieldEditors.set(path, editor);
                editorWrap.appendChild(row);
            }
        }

        function collectChanges() {
            const changes = {};
            for (const [path, editor] of fieldEditors.entries()) {
                changes[path] = editor.value;
            }
            return changes;
        }

        async function loadRecord() {
            const collectionName = String(collectionWidget.value || "").trim();
            const recordId = getRecordIdValue();
            const selectedRecordJson = getSelectedRecordJsonValue();

            const payload = await loadRecordEditorPayload(
                collectionName,
                recordId,
                selectedRecordJson
            );

            currentRecord = payload.record || {};
            currentFields = Array.isArray(payload.fields) ? payload.fields : [];

            if (payload.record_id) {
                recordIdWidget.value = payload.record_id;
            }

            renderEditors(currentFields);

            const shownRecordId = payload.record_id || recordId || "";
            infoBar.textContent = shownRecordId
                ? `Editing ${collectionName} / ${shownRecordId}`
                : `Editing ${collectionName}`;

            saveStatusLabel.textContent = "";
            node.setDirtyCanvas(true, true);
        }

        async function saveRecord() {
            const collectionName = String(collectionWidget.value || "").trim();
            const recordId = getRecordIdValue();

            if (!collectionName) {
                throw new Error("collection_name is required");
            }
            if (!recordId) {
                throw new Error("record_id is required");
            }

            const changes = collectChanges();

            const payload = await postJson("/zmongo/record_editor/save", {
                collection_name: collectionName,
                record_id: recordId,
                changes,
            });

            currentRecord = payload.record || {};
            saveStatusLabel.textContent = `Saved ${collectionName} / ${recordId}`;

            if (saveStatusWidget) {
                saveStatusWidget.value = `Saved ${collectionName} / ${recordId}`;
            }

            await loadRecord();
            node.setDirtyCanvas(true, true);
        }

        loadButton.addEventListener("click", async () => {
            try {
                await loadRecord();
            } catch (err) {
                console.error("Failed to load record editor:", err);
                infoBar.textContent = `Error: ${err.message}`;
            }
        });

        saveButton.addEventListener("click", async () => {
            try {
                await saveRecord();
            } catch (err) {
                console.error("Failed to save record editor:", err);
                saveStatusLabel.textContent = `Error: ${err.message}`;
            }
        });

        const originalCollectionCallback = collectionWidget.callback;
        collectionWidget.callback = async (...args) => {
            if (originalCollectionCallback) {
                originalCollectionCallback.apply(collectionWidget, args);
            }
            try {
                await loadRecord();
            } catch (err) {
                console.error("Failed to reload editor after collection change:", err);
            }
        };

        const originalRecordIdCallback = recordIdWidget.callback;
        recordIdWidget.callback = async (...args) => {
            if (originalRecordIdCallback) {
                originalRecordIdCallback.apply(recordIdWidget, args);
            }
            try {
                await loadRecord();
            } catch (err) {
                console.error("Failed to reload editor after record_id change:", err);
            }
        };

        try {
            await loadRecord();
        } catch (err) {
            console.error("Initial record editor load failed:", err);
            infoBar.textContent = `Error: ${err.message}`;
        }
    },
});