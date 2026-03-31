import { app } from "/scripts/app.web";

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

async function fetchTabularRecords(collectionName) {
    if (!collectionName || String(collectionName).startsWith("<")) {
        return {
            headings: [],
            flat_records: [],
            record_ids: [],
            default_record_id: "",
            record_count: 0,
        };
    }

    const url = `/zmongo/tabular_records?collection_name=${encodeURIComponent(collectionName)}`;
    return await fetchJson(url);
}

async function fetchTabularRecordsSearch(collectionName, searchText, flattenedFieldName) {
    if (!collectionName || String(collectionName).startsWith("<")) {
        return {
            headings: [],
            flat_records: [],
            record_ids: [],
            default_record_id: "",
            record_count: 0,
        };
    }

    const url =
        `/zmongo/tabular_records_search?collection_name=${encodeURIComponent(collectionName)}` +
        `&search_text=${encodeURIComponent(searchText ?? "")}` +
        `&flattened_field_name=${encodeURIComponent(flattenedFieldName ?? "")}`;

    return await fetchJson(url);
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

function buildCell(content, isHeader = false) {
    const cell = document.createElement(isHeader ? "th" : "td");
    cell.style.border = "1px solid #444";
    cell.style.padding = "0";
    cell.style.verticalAlign = "top";
    cell.style.background = isHeader ? "#2a2a2a" : "#1d1d1d";
    cell.style.color = "#ddd";

    const inner = document.createElement("div");
    inner.textContent = content ?? "";
    inner.style.minWidth = "140px";
    inner.style.maxWidth = "480px";
    inner.style.padding = "6px";
    inner.style.whiteSpace = "pre-wrap";
    inner.style.overflowWrap = "anywhere";
    inner.style.wordBreak = "break-word";
    inner.style.resize = "horizontal";
    inner.style.overflow = "auto";
    inner.style.boxSizing = "border-box";

    if (isHeader) {
        inner.style.fontWeight = "600";
        inner.style.cursor = "pointer";
        inner.style.userSelect = "none";
    }

    cell.appendChild(inner);
    return { cell, inner };
}

app.registerExtension({
    name: "ZMongo.TabularRecordView",

    async nodeCreated(node) {
        if (node.comfyClass !== "ZMongoTabularRecordViewNode") {
            return;
        }

        function updateSearchModeLabel() {
            const flattenedFieldName = String(fieldWidget.value || "").trim();
            const searchText = String(exactSearchInput.value || "").trim();

            const targetText = flattenedFieldName
                ? `field "${flattenedFieldName}"`
                : "all fields";

            const queryText = searchText
                ? ` | query: "${searchText}"`
                : "";

            searchModeLabel.textContent = `Search target: ${targetText}${queryText}`;
        }

        const collectionWidget = findWidget(node, "collection_name");
        const fieldWidget = findWidget(node, "flattened_field_name");
        const recordIdWidget = findWidget(node, "record_id");
        const indexWidget = findWidget(node, "selected_record_index");

        if (!collectionWidget || !fieldWidget || !recordIdWidget || !indexWidget) {
            console.warn("ZMongoTabularRecordViewNode widgets not found.");
            return;
        }

        const container = document.createElement("div");
        container.style.display = "flex";
        container.style.flexDirection = "column";
        container.style.gap = "6px";
        container.style.width = "100%";
        container.style.minHeight = "260px";
        container.style.maxHeight = "520px";
        container.style.background = "#111";
        container.style.border = "1px solid #333";
        container.style.borderRadius = "8px";
        container.style.padding = "8px";
        container.style.boxSizing = "border-box";

        const infoBar = document.createElement("div");
        infoBar.style.fontSize = "12px";
        infoBar.style.color = "#aaa";
        infoBar.textContent = "Loading records...";

        const searchModeLabel = document.createElement("div");
        searchModeLabel.style.fontSize = "11px";
        searchModeLabel.style.color = "#8fb3ff";
        searchModeLabel.textContent = "Search target: all fields";

        const searchBar = document.createElement("div");
        searchBar.style.display = "flex";
        searchBar.style.gap = "8px";
        searchBar.style.alignItems = "center";
        searchBar.style.flexWrap = "wrap";

        const exactSearchInput = document.createElement("input");
        exactSearchInput.type = "text";
        exactSearchInput.placeholder = "Search values (* and ? wildcards supported)";
        exactSearchInput.style.flex = "1";
        exactSearchInput.style.minWidth = "220px";
        exactSearchInput.style.padding = "6px 8px";
        exactSearchInput.style.borderRadius = "6px";
        exactSearchInput.style.border = "1px solid #444";
        exactSearchInput.style.background = "#1b1b1b";
        exactSearchInput.style.color = "#ddd";
        exactSearchInput.style.outline = "none";

        const exactSearchButton = document.createElement("button");
        exactSearchButton.textContent = "Search";
        exactSearchButton.style.padding = "6px 10px";
        exactSearchButton.style.borderRadius = "6px";
        exactSearchButton.style.border = "1px solid #555";
        exactSearchButton.style.background = "#2a2a2a";
        exactSearchButton.style.color = "#eee";
        exactSearchButton.style.cursor = "pointer";

        const clearSearchButton = document.createElement("button");
        clearSearchButton.textContent = "Show All";
        clearSearchButton.style.padding = "6px 10px";
        clearSearchButton.style.borderRadius = "6px";
        clearSearchButton.style.border = "1px solid #555";
        clearSearchButton.style.background = "#2a2a2a";
        clearSearchButton.style.color = "#eee";
        clearSearchButton.style.cursor = "pointer";

        searchBar.appendChild(exactSearchInput);
        searchBar.appendChild(exactSearchButton);
        searchBar.appendChild(clearSearchButton);

        const tableWrap = document.createElement("div");
        tableWrap.style.flex = "1";
        tableWrap.style.overflow = "auto";
        tableWrap.style.border = "1px solid #333";
        tableWrap.style.borderRadius = "6px";
        tableWrap.style.background = "#181818";

        const table = document.createElement("table");
        table.style.borderCollapse = "collapse";
        table.style.width = "max-content";
        table.style.minWidth = "100%";
        table.style.tableLayout = "auto";
        table.style.fontSize = "12px";

        tableWrap.appendChild(table);
        container.appendChild(infoBar);
        container.appendChild(searchModeLabel);
        container.appendChild(searchBar);
        container.appendChild(tableWrap);

        if (node.addDOMWidget) {
            node.addDOMWidget("record_table", "div", container, {
                serialize: false,
                hideOnZoom: false,
            });
        }

        let lastPayload = {
            headings: [],
            flat_records: [],
            record_ids: [],
            default_record_id: "",
            record_count: 0,
        };

        function getSafeRecordIds() {
            return Array.isArray(lastPayload.record_ids) ? lastPayload.record_ids : [];
        }

        function resolveSelectedIndex() {
            const recordIds = getSafeRecordIds();
            const recordId = String(recordIdWidget.value || "").trim();

            if (recordId) {
                const idx = recordIds.indexOf(recordId);
                if (idx >= 0) {
                    return idx;
                }
            }

            return clampIndex(indexWidget.value, recordIds.length);
        }

        function syncWidgetsFromIndex(preferredIndex = null) {
            const recordIds = getSafeRecordIds();

            if (!recordIds.length) {
                indexWidget.value = 0;
                recordIdWidget.value = "";
                node.setDirtyCanvas(true, true);
                return 0;
            }

            const idx = clampIndex(
                preferredIndex !== null ? preferredIndex : indexWidget.value,
                recordIds.length
            );

            indexWidget.value = idx;
            recordIdWidget.value = recordIds[idx] || "";
            node.setDirtyCanvas(true, true);
            return idx;
        }

        function renderTable() {
            table.innerHTML = "";

            const headings = Array.isArray(lastPayload.headings) ? lastPayload.headings : [];
            const records = Array.isArray(lastPayload.flat_records) ? lastPayload.flat_records : [];
            const recordIds = getSafeRecordIds();

            if (!headings.length || !records.length) {
                infoBar.textContent = "No records found.";
                return;
            }

            const selectedIndex = resolveSelectedIndex();
            infoBar.textContent = `${records.length} record(s) loaded. Click a row to select it. Click a header to set flattened_field_name.`;

            const thead = document.createElement("thead");
            const headRow = document.createElement("tr");

            const indexHeader = buildCell("#", true);
            indexHeader.inner.style.minWidth = "50px";
            indexHeader.inner.style.maxWidth = "80px";
            headRow.appendChild(indexHeader.cell);

            for (const heading of headings) {
                const { cell, inner } = buildCell(heading, true);
                inner.title = heading;
                inner.addEventListener("click", () => {
                    fieldWidget.value = heading;
                    updateSearchModeLabel();
                    node.setDirtyCanvas(true, true);
                });
                headRow.appendChild(cell);
            }

            thead.appendChild(headRow);
            table.appendChild(thead);

            const tbody = document.createElement("tbody");

            records.forEach((record, rowIndex) => {
                const tr = document.createElement("tr");
                tr.style.cursor = "pointer";

                if (rowIndex === selectedIndex) {
                    tr.style.background = "#24364d";
                }

                tr.addEventListener("click", () => {
                    indexWidget.value = rowIndex;
                    recordIdWidget.value = recordIds[rowIndex] || "";
                    renderTable();
                    node.setDirtyCanvas(true, true);
                });

                const indexCell = buildCell(String(rowIndex), false);
                indexCell.inner.style.minWidth = "50px";
                indexCell.inner.style.maxWidth = "80px";
                tr.appendChild(indexCell.cell);

                for (const heading of headings) {
                    let value = record[heading];

                    if (value === null || value === undefined) {
                        value = "";
                    } else if (typeof value !== "string") {
                        try {
                            value = JSON.stringify(value, null, 2);
                        } catch {
                            value = String(value);
                        }
                    }

                    const { cell, inner } = buildCell(value, false);
                    inner.title = value;
                    tr.appendChild(cell);
                }

                tbody.appendChild(tr);
            });

            table.appendChild(tbody);
        }

        async function refreshTable() {
            const collectionName = collectionWidget.value;
            lastPayload = await fetchTabularRecords(collectionName);

            const recordIds = getSafeRecordIds();
            if (recordIds.length) {
                const currentId = String(recordIdWidget.value || "").trim();

                if (!currentId || !recordIds.includes(currentId)) {
                    recordIdWidget.value = lastPayload.default_record_id || recordIds[0] || "";
                }

                const idx = recordIds.indexOf(recordIdWidget.value);
                indexWidget.value = idx >= 0 ? idx : 0;
            } else {
                recordIdWidget.value = "";
                indexWidget.value = 0;
            }

            renderTable();
            node.setDirtyCanvas(true, true);
        }

        async function refreshTableSearch(searchText) {
            const collectionName = collectionWidget.value;
            const flattenedFieldName = String(fieldWidget.value || "").trim();

            lastPayload = await fetchTabularRecordsSearch(
                collectionName,
                searchText,
                flattenedFieldName
            );

            const recordIds = getSafeRecordIds();
            if (recordIds.length) {
                recordIdWidget.value = lastPayload.default_record_id || recordIds[0] || "";
                indexWidget.value = 0;
            } else {
                recordIdWidget.value = "";
                indexWidget.value = 0;
            }

            renderTable();
            node.setDirtyCanvas(true, true);
        }

        exactSearchButton.addEventListener("click", async () => {
            updateSearchModeLabel();
            try {
                await refreshTableSearch(exactSearchInput.value);
                updateSearchModeLabel();
                infoBar.textContent = `Search returned ${lastPayload.record_count || 0} record(s).`;
            } catch (err) {
                console.error("Search failed:", err);
                infoBar.textContent = `Error: ${err.message}`;
            }
        });

        exactSearchInput.addEventListener("keydown", async (event) => {
            if (event.key === "Enter") {
                event.preventDefault();
                updateSearchModeLabel();
                try {
                    await refreshTableSearch(exactSearchInput.value);
                    updateSearchModeLabel();
                    infoBar.textContent = `Search returned ${lastPayload.record_count || 0} record(s).`;
                } catch (err) {
                    console.error("Search failed:", err);
                    infoBar.textContent = `Error: ${err.message}`;
                }
            }
        });

        clearSearchButton.addEventListener("click", async () => {
            updateSearchModeLabel();
            try {
                exactSearchInput.value = "";
                await refreshTable();
                updateSearchModeLabel();
            } catch (err) {
                console.error("Refresh all records failed:", err);
                infoBar.textContent = `Error: ${err.message}`;
            }
        });

        const originalCollectionCallback = collectionWidget.callback;
        collectionWidget.callback = async (...args) => {
            if (originalCollectionCallback) {
                originalCollectionCallback.apply(collectionWidget, args);
            }
            updateSearchModeLabel();
            try {
                exactSearchInput.value = "";
                await refreshTable();
            } catch (err) {
                console.error("Failed to refresh tabular records after collection change:", err);
                infoBar.textContent = `Error: ${err.message}`;
            }
        };

        const originalFieldCallback = fieldWidget.callback;
        fieldWidget.callback = (...args) => {
            if (originalFieldCallback) {
                originalFieldCallback.apply(fieldWidget, args);
            }
            updateSearchModeLabel();
            node.setDirtyCanvas(true, true);
        };

        const originalIndexCallback = indexWidget.callback;
        indexWidget.callback = (...args) => {
            if (originalIndexCallback) {
                originalIndexCallback.apply(indexWidget, args);
            }
            syncWidgetsFromIndex(indexWidget.value);
            renderTable();
        };

        const originalRecordIdCallback = recordIdWidget.callback;
        recordIdWidget.callback = (...args) => {
            if (originalRecordIdCallback) {
                originalRecordIdCallback.apply(recordIdWidget, args);
            }

            const ids = getSafeRecordIds();
            const idx = ids.indexOf(String(recordIdWidget.value || "").trim());

            if (idx >= 0) {
                indexWidget.value = idx;
            } else if (ids.length) {
                recordIdWidget.value = ids[0];
                indexWidget.value = 0;
            } else {
                recordIdWidget.value = "";
                indexWidget.value = 0;
            }

            renderTable();
            node.setDirtyCanvas(true, true);
        };
        updateSearchModeLabel();
        try {
            await refreshTable();
        } catch (err) {
            console.error("Initial ZMongo tabular record refresh failed:", err);
            infoBar.textContent = `Error: ${err.message}`;
        }

        setTimeout(async () => {
            updateSearchModeLabel();
            try {
                await refreshTable();
            } catch (err) {
                console.error("Delayed ZMongo tabular record refresh failed:", err);
            }
        }, 200);
    },
});