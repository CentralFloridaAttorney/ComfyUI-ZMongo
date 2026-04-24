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

function submitPostToNewTab(url, fields) {
    const form = document.createElement("form");
    form.method = "POST";
    form.action = url;
    form.target = "_blank";
    form.style.display = "none";

    for (const [name, value] of Object.entries(fields)) {
        const input = document.createElement("input");
        input.type = "hidden";
        input.name = name;
        input.value = value ?? "";
        form.appendChild(input);
    }

    document.body.appendChild(form);
    form.submit();
    document.body.removeChild(form);
}

function openUrlInNewTab(url) {
    window.open(url, "_blank", "noopener,noreferrer");
}

function buildPanel(container) {
    clearElement(container);

    const baseUrl = "https://ztarot.app";
    const loginUrl = `${baseUrl}/user/login`;
    const managerUrl = `${baseUrl}/user/manager`;

    const root = el("div", {
        className: "ztarot-panel-root",
        style: [
            "padding:12px",
            "display:flex",
            "flex-direction:column",
            "gap:10px",
            "font-family:inherit"
        ].join(";")
    });

    const title = el("div", {
        text: "ZMongo Login Panel",
        style: "font-size:16px;font-weight:700;"
    });

    const help = el("div", {
        text: "Enter credentials and open ZTarot in a new tab.",
        style: "font-size:12px;opacity:0.8;"
    });

    const status = el("div", {
        text: "Idle",
        style: [
            "font-size:12px",
            "padding:6px 8px",
            "border:1px solid #444",
            "border-radius:6px",
            "background:#111"
        ].join(";")
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

    const baseUrlLabel = el("label", {
        text: "Base URL",
        style: "font-size:13px;font-weight:600;"
    });

    const baseUrlInput = el("input", {
        type: "text",
        value: baseUrl,
        style: "width:100%;padding:8px;box-sizing:border-box;"
    });

    const buttonsRow = el("div", {
        style: "display:flex;flex-direction:column;gap:8px;"
    });

    const openHomeButton = el("button", {
        text: "Open ZTarot",
        style: "padding:8px 10px;"
    });

    const loginButton = el("button", {
        text: "Login + Open Manager",
        style: "padding:8px 10px;"
    });

    const loginOnlyButton = el("button", {
        text: "Login (POST to /user/login)",
        style: "padding:8px 10px;"
    });

    function updateStatus(text) {
        status.textContent = text;
    }

    function getValues() {
        return {
            base: (baseUrlInput.value || "").trim().replace(/\/+$/, ""),
            username: (usernameInput.value || "").trim(),
            password: passwordInput.value || ""
        };
    }

    openHomeButton.addEventListener("click", () => {
        const { base } = getValues();
        if (!base) {
            updateStatus("Base URL is required.");
            return;
        }
        openUrlInNewTab(base);
        updateStatus(`Opened ${base}`);
    });

    loginOnlyButton.addEventListener("click", () => {
        const { base, username, password } = getValues();

        if (!base || !username || !password) {
            updateStatus("Base URL, username, and password are required.");
            return;
        }

        submitPostToNewTab(`${base}/user/login`, {
            username,
            password
        });

        updateStatus("Submitted login form to new tab.");
    });

    loginButton.addEventListener("click", () => {
        const { base, username, password } = getValues();

        if (!base || !username || !password) {
            updateStatus("Base URL, username, and password are required.");
            return;
        }

        const popup = window.open("about:blank", "_blank");
        if (!popup) {
            updateStatus("Popup blocked by browser.");
            return;
        }

        const safeAction = `${base}/user/login`;
        const safeNext = `${base}/user/manager`;

        popup.document.open();
        popup.document.write(`
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>ZTarot Login</title>
</head>
<body>
  <form id="loginForm" method="POST" action="${safeAction}">
    <input type="hidden" name="username" value="${username.replace(/"/g, "&quot;")}">
    <input type="hidden" name="password" value="${password.replace(/"/g, "&quot;")}">
    <input type="hidden" name="next" value="${safeNext.replace(/"/g, "&quot;")}">
  </form>
  <script>
    document.getElementById("loginForm").submit();
  </script>
</body>
</html>
        `);
        popup.document.close();

        updateStatus("Opened login tab and submitted credentials.");
    });

    buttonsRow.append(
        openHomeButton,
        loginButton,
        loginOnlyButton
    );

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
            id: "ztarot-zmongo-browser",
            icon: "pi pi-database",
            title: "ZMongo",
            tooltip: "ZTarot ZMongo Browser",
            type: "custom",
            render: (el) => {
                buildPanel(el);
            },
        });
    },
});