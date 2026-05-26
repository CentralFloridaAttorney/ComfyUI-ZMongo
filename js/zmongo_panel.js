import { app } from "../../scripts/app.js";

function el(tag, props = {}, children = []) {
    const node = document.createElement(tag);

    for (const [key, value] of Object.entries(props)) {
        if (key === "className") {
            node.className = value;
        } else if (key === "text") {
            node.textContent = value;
        } else if (key === "html") {
            node.innerHTML = value;
        } else if (key === "style") {
            node.style.cssText = value;
        } else if (key.startsWith("on") && typeof value === "function") {
            node.addEventListener(key.slice(2).toLowerCase(), value);
        } else if (value != null) {
            node.setAttribute(key, String(value));
        }
    }

    for (const child of children) {
        if (child instanceof Node) {
            node.appendChild(child);
        } else if (child != null) {
            node.appendChild(document.createTextNode(String(child)));
        }
    }

    return node;
}

function clearElement(element) {
    while (element.firstChild) {
        element.removeChild(element.firstChild);
    }
}

function normalizeBaseUrl(value) {
    return String(value || "")
        .trim()
        .replace(/\/+$/, "");
}

function openUrlInNewTab(url) {
    window.open(url, "_blank", "noopener,noreferrer");
}

function escapeHtmlAttribute(value) {
    return String(value ?? "")
        .replace(/&/g, "&amp;")
        .replace(/"/g, "&quot;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
}

function submitLoginToNewTab(baseUrl, username, password) {
    const popup = window.open("about:blank", "_blank");
    if (!popup) {
        return false;
    }

    const actionUrl = `${baseUrl}/user/login`;
    const nextUrl = `${baseUrl}/user/settings`;

    popup.document.open();
    popup.document.write(`<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Business Process Applications Login</title>
</head>
<body>
  <form id="loginForm" method="POST" action="${escapeHtmlAttribute(actionUrl)}">
    <input type="hidden" name="username" value="${escapeHtmlAttribute(username)}">
    <input type="hidden" name="password" value="${escapeHtmlAttribute(password)}">
    <input type="hidden" name="next" value="${escapeHtmlAttribute(nextUrl)}">
  </form>
  <script>
    document.getElementById("loginForm").submit();
  </script>
</body>
</html>`);
    popup.document.close();
    return true;
}

function buildPanel(container) {
    clearElement(container);

    const defaultBaseUrl = "https://businessprocessapplications.com";

    const root = el("div", {
        className: "bpa-zmongo-panel-root",
        style: [
            "padding:12px",
            "display:flex",
            "flex-direction:column",
            "gap:10px",
            "font-family:inherit"
        ].join(";")
    });

    const title = el("div", {
        text: "Business Process Applications / ZMongo",
        style: "font-size:16px;font-weight:700;"
    });

    const help = el("div", {
        text: "Login with an existing account or register a new account. After login, copy your API key into the ZMongo API Key Session node.",
        style: "font-size:12px;opacity:0.82;line-height:1.35;"
    });

    const status = el("div", {
        text: "Ready",
        style: [
            "font-size:12px",
            "padding:6px 8px",
            "border:1px solid #444",
            "border-radius:6px",
            "background:#111",
            "white-space:pre-wrap"
        ].join(";")
    });

    const baseUrlLabel = el("label", {
        text: "Base URL",
        style: "font-size:13px;font-weight:600;"
    });

    const baseUrlInput = el("input", {
        type: "text",
        value: defaultBaseUrl,
        style: "width:100%;padding:8px;box-sizing:border-box;"
    });

    const usernameLabel = el("label", {
        text: "Username",
        style: "font-size:13px;font-weight:600;"
    });

    const usernameInput = el("input", {
        type: "text",
        placeholder: "username",
        autocomplete: "username",
        style: "width:100%;padding:8px;box-sizing:border-box;"
    });

    const passwordLabel = el("label", {
        text: "Password",
        style: "font-size:13px;font-weight:600;"
    });

    const passwordInput = el("input", {
        type: "password",
        placeholder: "password",
        autocomplete: "current-password",
        style: "width:100%;padding:8px;box-sizing:border-box;"
    });

    const buttonsRow = el("div", {
        style: "display:flex;flex-direction:column;gap:8px;"
    });

    const loginButton = el("button", {
        text: "Login",
        style: "padding:8px 10px;font-weight:600;"
    });

    const registerButton = el("button", {
        text: "Register",
        style: "padding:8px 10px;"
    });

    function updateStatus(text) {
        status.textContent = text;
    }

    function getValues() {
        return {
            base: normalizeBaseUrl(baseUrlInput.value),
            username: String(usernameInput.value || "").trim(),
            password: passwordInput.value || ""
        };
    }

    loginButton.addEventListener("click", () => {
        const { base, username, password } = getValues();

        if (!base || !username || !password) {
            updateStatus("Base URL, username, and password are required to login.");
            return;
        }

        const submitted = submitLoginToNewTab(base, username, password);
        updateStatus(submitted ? "Login submitted in a new tab." : "Popup blocked. Allow popups for ComfyUI and try again.");
    });

    registerButton.addEventListener("click", () => {
        const { base } = getValues();

        if (!base) {
            updateStatus("Base URL is required to register.");
            return;
        }

        openUrlInNewTab(`${base}/user/register`);
        updateStatus("Opened registration page in a new tab.");
    });

    buttonsRow.append(loginButton, registerButton);

    root.append(
        title,
        help,
        status,
        baseUrlLabel,
        baseUrlInput,
        usernameLabel,
        usernameInput,
        passwordLabel,
        passwordInput,
        buttonsRow
    );

    container.appendChild(root);
}

app.registerExtension({
    name: "ComfyUI.ZMongo.SidebarPanel",

    async setup() {
        app.extensionManager.registerSidebarTab({
            id: "bpa-zmongo-browser",
            icon: "pi pi-database",
            title: "ZMongo",
            tooltip: "Business Process Applications ZMongo Login/Register",
            type: "custom",
            render: (element) => {
                buildPanel(element);
            },
        });
    },
});