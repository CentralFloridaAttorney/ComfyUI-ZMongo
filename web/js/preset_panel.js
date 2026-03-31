import { app } from "../../scripts/app.web";

const EXT_ID = "ZMongo.PresetPanel";
const PANEL_ID = "zmongo-preset-panel";

function getWorkflowNodes() {
  return app.graph?._nodes ?? [];
}

function getNodeTitle(node) {
  return String(node?.title || "").trim();
}

function findNodeByTitle(title) {
  return getWorkflowNodes().find((node) => getNodeTitle(node) === title) || null;
}

function findWidget(node, widgetName) {
  return node?.widgets?.find((w) => w?.name === widgetName) || null;
}

function coerceValueForWidget(widget, value) {
  if (!widget) return value;
  const current = widget.value;
  if (typeof current === "number" && typeof value === "string") {
    const n = Number(value);
    return Number.isNaN(n) ? value : n;
  }
  return value;
}

function setWidgetValue(node, widgetName, value) {
  const widget = findWidget(node, widgetName);
  if (!widget) return false;

  const finalValue = coerceValueForWidget(widget, value);
  widget.value = finalValue;

  if (typeof widget.callback === "function") {
    try {
      widget.callback(finalValue);
    } catch {}
  }

  if (typeof node.onWidgetChanged === "function") {
    try {
      node.onWidgetChanged(widgetName, finalValue, widget);
    } catch {}
  }

  node.setDirtyCanvas?.(true, true);
  return true;
}

function applyPreset(preset) {
  const failures = [];
  const updates = preset?.updates ?? {};

  for (const [nodeTitle, widgetValues] of Object.entries(updates)) {
    const node = findNodeByTitle(nodeTitle);
    if (!node) {
      failures.push(`Missing node: ${nodeTitle}`);
      continue;
    }

    for (const [widgetName, value] of Object.entries(widgetValues)) {
      const ok = setWidgetValue(node, widgetName, value);
      if (!ok) failures.push(`Missing widget: ${nodeTitle}.${widgetName}`);
    }
  }

  app.graph?.setDirtyCanvas?.(true, true);

  if (failures.length) {
    alert(`Preset applied with warnings:\n\n${failures.join("\n")}`);
  }
}

function queuePrompt() {
  try {
    app.queuePrompt?.(0);
  } catch (err) {
    console.warn("[ZMongo PresetPanel] Queue failed", err);
  }
}

async function loadPresets(workflow = "default") {
  const res = await fetch(`/zmongo/presets?workflow=${encodeURIComponent(workflow)}`);
  if (!res.ok) {
    throw new Error(`Failed to load presets (${res.status})`);
  }
  const data = await res.json();
  if (!data.success) {
    throw new Error(data.error || "Failed to load presets");
  }

  const groups = {};
  for (const p of data.presets ?? []) {
    const g = p.group || "Default";
    if (!groups[g]) groups[g] = [];
    groups[g].push(p);
  }

  return {
    groups: Object.entries(groups).map(([name, presets]) => ({ name, presets }))
  };
}

function extractCurrentValues(nodeTitles) {
  const out = {};
  for (const title of nodeTitles) {
    const node = findNodeByTitle(title);
    if (!node || !node.widgets) continue;

    out[title] = {};
    for (const widget of node.widgets) {
      if (!widget?.name) continue;
      out[title][widget.name] = widget.value;
    }
  }
  return out;
}

async function savePreset(workflow, name, group, updates) {
  const res = await fetch("/zmongo/presets", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      workflow,
      name,
      group,
      updates
    })
  });

  const data = await res.json();
  if (!res.ok || !data.success) {
    throw new Error(data.error || "Failed to save preset");
  }
}

async function deletePreset(workflow, name) {
  const res = await fetch(`/zmongo/presets/${encodeURIComponent(workflow)}/${encodeURIComponent(name)}`, {
    method: "DELETE"
  });

  const data = await res.json();
  if (!res.ok || !data.success) {
    throw new Error(data.error || "Failed to delete preset");
  }
}

function makeButton(label, onClick) {
  const btn = document.createElement("button");
  btn.textContent = label;
  btn.style.width = "100%";
  btn.style.marginBottom = "8px";
  btn.style.padding = "8px";
  btn.style.borderRadius = "6px";
  btn.style.cursor = "pointer";
  btn.onclick = onClick;
  return btn;
}

function makeSectionTitle(text) {
  const el = document.createElement("div");
  el.textContent = text;
  el.style.fontWeight = "bold";
  el.style.margin = "12px 0 8px";
  el.style.fontSize = "14px";
  return el;
}

function makeInfo(text) {
  const el = document.createElement("div");
  el.textContent = text;
  el.style.fontSize = "12px";
  el.style.opacity = "0.8";
  el.style.marginBottom = "10px";
  return el;
}

async function buildPanel(container, workflow = "default") {
  container.innerHTML = "";
  container.appendChild(makeInfo(`Workflow key: ${workflow}`));

  const config = await loadPresets(workflow);

  for (const group of config.groups ?? []) {
    container.appendChild(makeSectionTitle(group.name));

    for (const preset of group.presets ?? []) {
      const row = document.createElement("div");
      row.style.display = "flex";
      row.style.gap = "6px";
      row.style.marginBottom = "6px";

      const applyBtn = makeButton(preset.name, () => applyPreset(preset));
      applyBtn.style.flex = "1";

      const applyQueueBtn = makeButton("Apply + Queue", () => {
        applyPreset(preset);
        queuePrompt();
      });
      applyQueueBtn.style.flex = "1";

      const delBtn = makeButton("Del", async () => {
        if (!confirm(`Delete preset "${preset.name}"?`)) return;
        try {
          await deletePreset(workflow, preset.name);
          await buildPanel(container, workflow);
        } catch (err) {
          alert(String(err));
        }
      });
      delBtn.style.flex = "0 0 64px";

      row.appendChild(applyBtn);
      row.appendChild(applyQueueBtn);
      row.appendChild(delBtn);
      container.appendChild(row);
    }
  }

  container.appendChild(makeSectionTitle("Save Current Preset"));

  const nameInput = document.createElement("input");
  nameInput.placeholder = "Preset name";
  nameInput.style.width = "100%";
  nameInput.style.marginBottom = "6px";
  nameInput.style.padding = "8px";

  const groupInput = document.createElement("input");
  groupInput.placeholder = "Group name";
  groupInput.value = "Default";
  groupInput.style.width = "100%";
  groupInput.style.marginBottom = "6px";
  groupInput.style.padding = "8px";

  const nodesInput = document.createElement("textarea");
  nodesInput.placeholder = "Node titles, one per line";
  nodesInput.style.width = "100%";
  nodesInput.style.minHeight = "120px";
  nodesInput.style.marginBottom = "6px";
  nodesInput.style.padding = "8px";
  nodesInput.value = [
    "MAIN_MODEL",
    "MAIN_LORA",
    "MAIN_SAMPLER",
    "IMAGE_SETTINGS",
    "VIDEO_MODEL",
    "VIDEO_UNET",
    "VIDEO_LORA",
    "VIDEO_SAMPLER",
    "VIDEO_SETTINGS"
  ].join("\n");

  container.appendChild(nameInput);
  container.appendChild(groupInput);
  container.appendChild(nodesInput);

  container.appendChild(
    makeButton("Save Current Preset to ZMongo", async () => {
      const name = nameInput.value.trim();
      const group = groupInput.value.trim() || "Default";
      const nodeTitles = nodesInput.value
        .split("\n")
        .map((s) => s.trim())
        .filter(Boolean);

      if (!name) {
        alert("Enter a preset name.");
        return;
      }

      const updates = extractCurrentValues(nodeTitles);

      try {
        await savePreset(workflow, name, group, updates);
        await buildPanel(container, workflow);
      } catch (err) {
        alert(String(err));
      }
    })
  );

  container.appendChild(makeSectionTitle("Utility"));

  container.appendChild(makeButton("Queue Prompt", () => queuePrompt()));

  container.appendChild(
    makeButton("Refresh Presets", async () => {
      try {
        await buildPanel(container, workflow);
      } catch (err) {
        alert(String(err));
      }
    })
  );
}

app.registerExtension({
  name: EXT_ID,

  async setup() {
    const panel = document.createElement("div");
    panel.id = PANEL_ID;
    panel.style.padding = "10px";
    panel.style.height = "100%";
    panel.style.overflowY = "auto";
    panel.style.boxSizing = "border-box";

    const workflow = "default";

    try {
      await buildPanel(panel, workflow);
    } catch (err) {
      panel.textContent = String(err);
      console.error(err);
    }

    if (app.extensionManager?.registerSidebarTab) {
      app.extensionManager.registerSidebarTab({
        id: PANEL_ID,
        icon: "list",
        title: "Presets",
        tooltip: "ZMongo preset panel",
        type: "custom",
        render: (el) => {
          el.innerHTML = "";
          el.appendChild(panel);
        }
      });
    }
  }
});