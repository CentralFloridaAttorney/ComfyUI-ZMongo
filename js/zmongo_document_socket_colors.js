/**
 * ComfyUI-ZMongo socket color helper.
 *
 * Purpose:
 * - Gives semantic ZMongo sockets distinct colors in ComfyUI/LiteGraph.
 * - Makes it harder to accidentally connect document IDs, field paths,
 *   file paths, filenames, and text values to the wrong inputs.
 *
 * Install path:
 *   /home/comfyuser/comfy_build/ComfyUI/custom_nodes/ComfyUI-ZMongo/js/zmongo_document_socket_colors.js
 */

import { app } from "../../scripts/app.js";

const ZMONGO_SOCKET_COLORS = {
    ZMONGO_DOCUMENT_ID: {
        color_on: "#4aa3ff",
        color_off: "#1f5f99",
        label: "ZMongo Document ID",
    },
    ZMONGO_FIELD_PATH: {
        color_on: "#ff9f3f",
        color_off: "#9b5a1c",
        label: "ZMongo Field Path",
    },
    ZMONGO_FILE_PATH: {
        color_on: "#44d17a",
        color_off: "#237a47",
        label: "ZMongo File Path",
    },
    ZMONGO_FILENAME: {
        color_on: "#b6ff4a",
        color_off: "#6d9127",
        label: "ZMongo Filename",
    },
    ZMONGO_TEXT: {
        color_on: "#c084fc",
        color_off: "#6d3faf",
        label: "ZMongo Text",
    },
    ZMONGO_STATUS: {
        color_on: "#ffd84a",
        color_off: "#9a7b1e",
        label: "ZMongo Status",
    },
    ZMONGO_API_SESSION: {
        color_on: "#00d4ff",
        color_off: "#00758c",
        label: "ZMongo API Session",
    },
};

function patchLiteGraphSocketColors() {
    const LiteGraph = window.LiteGraph;
    if (!LiteGraph) {
        return false;
    }

    LiteGraph.slot_types_default_in = LiteGraph.slot_types_default_in || {};
    LiteGraph.slot_types_default_out = LiteGraph.slot_types_default_out || {};

    for (const [socketType, colors] of Object.entries(ZMONGO_SOCKET_COLORS)) {
        LiteGraph.slot_types_default_in[socketType] = {
            shape: LiteGraph.CIRCLE_SHAPE,
            color_on: colors.color_on,
            color_off: colors.color_off,
        };

        LiteGraph.slot_types_default_out[socketType] = {
            shape: LiteGraph.CIRCLE_SHAPE,
            color_on: colors.color_on,
            color_off: colors.color_off,
        };
    }

    if (LiteGraph.slot_types) {
        for (const [socketType, colors] of Object.entries(ZMONGO_SOCKET_COLORS)) {
            LiteGraph.slot_types[socketType] = {
                shape: LiteGraph.CIRCLE_SHAPE,
                color_on: colors.color_on,
                color_off: colors.color_off,
            };
        }
    }

    return true;
}

function patchCanvasColors() {
    const LiteGraph = window.LiteGraph;
    if (!LiteGraph || !LiteGraph.LGraphCanvas) {
        return false;
    }

    const proto = LiteGraph.LGraphCanvas.prototype;
    if (!proto || proto.__zmongoSocketColorPatchApplied) {
        return true;
    }

    const originalGetConnectionColor = proto.getConnectionColor;

    proto.getConnectionColor = function(inputNode, outputNode, inputSlot, outputSlot) {
        const output = outputNode && outputNode.outputs ? outputNode.outputs[outputSlot] : null;
        const input = inputNode && inputNode.inputs ? inputNode.inputs[inputSlot] : null;

        const socketType = (output && output.type) || (input && input.type) || "";
        const socketConfig = ZMONGO_SOCKET_COLORS[socketType];

        if (socketConfig) {
            return socketConfig.color_on;
        }

        if (typeof originalGetConnectionColor === "function") {
            return originalGetConnectionColor.apply(this, arguments);
        }

        return "#999";
    };

    proto.__zmongoSocketColorPatchApplied = true;
    return true;
}

function patchNodeSlotDrawing() {
    const LiteGraph = window.LiteGraph;
    if (!LiteGraph || !LiteGraph.LGraphCanvas) {
        return false;
    }

    const proto = LiteGraph.LGraphCanvas.prototype;
    if (!proto || proto.__zmongoSlotDrawingPatchApplied) {
        return true;
    }

    const originalDrawNodeShape = proto.drawNodeShape;

    proto.drawNodeShape = function(node, ctx, size, fgcolor, bgcolor, selected, mouseOver) {
        const result = originalDrawNodeShape
            ? originalDrawNodeShape.apply(this, arguments)
            : undefined;

        try {
            drawZMongoSocketHighlights(this, node, ctx);
        } catch {
            // Never break ComfyUI canvas rendering because of socket decoration.
        }

        return result;
    };

    proto.__zmongoSlotDrawingPatchApplied = true;
    return true;
}

function drawZMongoSocketHighlights(canvas, node, ctx) {
    if (!node || !ctx) return;

    const leftX = 0;
    const rightX = node.size ? node.size[0] : 0;
    const radius = 5;

    if (Array.isArray(node.inputs)) {
        for (let index = 0; index < node.inputs.length; index++) {
            const input = node.inputs[index];
            if (!input || !ZMONGO_SOCKET_COLORS[input.type]) continue;

            const y = node.getConnectionPos
                ? node.getConnectionPos(true, index)[1] - node.pos[1]
                : 20 + index * 20;

            drawSocketDot(ctx, leftX, y, radius, ZMONGO_SOCKET_COLORS[input.type].color_on);
        }
    }

    if (Array.isArray(node.outputs)) {
        for (let index = 0; index < node.outputs.length; index++) {
            const output = node.outputs[index];
            if (!output || !ZMONGO_SOCKET_COLORS[output.type]) continue;

            const y = node.getConnectionPos
                ? node.getConnectionPos(false, index)[1] - node.pos[1]
                : 20 + index * 20;

            drawSocketDot(ctx, rightX, y, radius, ZMONGO_SOCKET_COLORS[output.type].color_on);
        }
    }
}

function drawSocketDot(ctx, x, y, radius, color) {
    ctx.save();
    ctx.beginPath();
    ctx.fillStyle = color;
    ctx.strokeStyle = "#111";
    ctx.lineWidth = 1.5;
    ctx.arc(x, y, radius, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.restore();
}

function addLegendToMenu() {
    const existing = document.getElementById("zmongo-socket-color-legend-style");
    if (existing) return;

    const style = document.createElement("style");
    style.id = "zmongo-socket-color-legend-style";
    style.textContent = `
        .zmongo-socket-color-hint {
            font-size: 11px;
            opacity: 0.82;
            line-height: 1.35;
            padding: 4px 0;
        }
        .zmongo-socket-color-chip {
            display: inline-block;
            width: 9px;
            height: 9px;
            border-radius: 50%;
            margin-right: 5px;
            vertical-align: middle;
            border: 1px solid rgba(255,255,255,0.35);
        }
    `;
    document.head.appendChild(style);
}

function installPatch() {
    addLegendToMenu();

    const ok1 = patchLiteGraphSocketColors();
    const ok2 = patchCanvasColors();
    const ok3 = patchNodeSlotDrawing();

    if (app && app.graph && app.graph.setDirtyCanvas) {
        app.graph.setDirtyCanvas(true, true);
    }

    return ok1 || ok2 || ok3;
}

app.registerExtension({
    name: "ComfyUI.ZMongo.DocumentSocketColors",

    async setup() {
        installPatch();

        // LiteGraph can be initialized after extension setup in some ComfyUI builds.
        setTimeout(installPatch, 250);
        setTimeout(installPatch, 1000);
    },

    async beforeRegisterNodeDef(nodeType, nodeData) {
        if (!nodeData || !nodeData.input || !nodeData.output) {
            return;
        }

        // Keep this hook lightweight. The actual drawing/coloring happens globally.
        // This exists so ComfyUI reloads the extension when semantic socket nodes load.
    },
});
