import { app } from "../../scripts/app.js";
/**
 * Standard ComfyUI Utility: $el
 * Creates DOM elements with attributes and children easily.
 */
export const $el = (tag, propsOrChildren, children) => {
	const split = tag.split(".");
	const element = document.createElement(split.shift() || "div");
	element.classList.add(...split);

	if (propsOrChildren) {
		if (Array.isArray(propsOrChildren) || propsOrChildren instanceof Node || typeof propsOrChildren !== "object") {
			children = propsOrChildren;
		} else {
			const { style, dataset, ...props } = propsOrChildren;
			if (style) Object.assign(element.style, style);
			if (dataset) Object.assign(element.dataset, dataset);
			Object.assign(element, props);
		}
	}

	if (children) {
		element.append(...(Array.isArray(children) ? children : [children]));
	}
	return element;
};

/**
 * Z-Mongo Management Dialog
 * A standard ComfyUI modal wrapping your terminal-style dashboard.
 */
class ZMongoDialog {
    constructor() {
        this.element = $el("div.comfy-modal", {
            style: {
                display: "none",
                position: "fixed",
                zIndex: 1000,
                top: 0,
                left: 0,
                width: "100vw",
                height: "100vh",
                background: "rgba(0,0,0,0.8)",
                justifyContent: "center",
                alignItems: "center"
            },
            onclick: (e) => {
                if (e.target === this.element) this.close();
            }
        }, [
            $el("div", {
                style: {
                    width: "90%",
                    height: "85%",
                    background: "#000",
                    border: "1px solid #00ff41",
                    position: "relative",
                    display: "flex",
                    flexDirection: "column"
                }
            }, [
                // Header / Close button
                $el("div", {
                    style: {
                        background: "#001100",
                        padding: "5px 10px",
                        display: "flex",
                        justifyContent: "space-between",
                        borderBottom: "1px solid #00ff41"
                    }
                }, [
                    $el("span", { textContent: "Z-MONGO_SYSTEM_MANAGER", style: { color: "#00ff41", fontSize: "12px" } }),
                    $el("button", {
                        textContent: "[X]",
                        style: { background: "none", color: "#00ff41", border: "none", cursor: "pointer" },
                        onclick: () => this.close()
                    })
                ]),
                // Dashboard Iframe
                $el("iframe", {
                    src: "/zai/zmongo/dashboard", // Matches the unified backend route
                    style: {
                        flexGrow: 1,
                        width: "100%",
                        border: "none"
                    }
                })
            ])
        ]);

        document.body.appendChild(this.element);
    }

    show() {
        this.element.style.display = "flex";
    }

    close() {
        this.element.style.display = "none";
    }
}

// Global instance to prevent multiple appends
let zmongoDialogInstance = null;

app.registerExtension({
	name: "ZAI.ZMongoUI",
	async setup() {
		// Find the ComfyUI menu to inject the button
		const menu = document.querySelector(".comfy-menu");
		if (!menu) return;

		const managerBtn = $el("button", {
			id: "zmongo-manager-btn",
			textContent: "Z-MONGO_DB",
			style: {
				color: "#00ff41",
				border: "1px solid #00ff41",
                marginTop: "5px",
                width: "100%"
			},
			onclick: () => {
                if (!zmongoDialogInstance) zmongoDialogInstance = new ZMongoDialog();
                zmongoDialogInstance.show();
            }
		});

		// Insert button at the top of the menu list
		menu.insertBefore(managerBtn, menu.firstChild);
	}
});