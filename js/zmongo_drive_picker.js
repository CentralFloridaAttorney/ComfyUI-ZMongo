import { app } from "../../scripts/app.js";
import { api } from "../../scripts/api.js";

// Insert your ZLegal Codex Web client ID here
const CLIENT_ID = '798721940362-REPLACE_WITH_FULL_CLIENT_ID.apps.googleusercontent.com';
const SCOPES = 'https://www.googleapis.com/auth/drive.readonly';

app.registerExtension({
    name: "ZMongo.DrivePicker",
    async beforeRegisterNodeDef(nodeType, nodeData, app) {
        if (nodeData.name === "ZMongoGoogleDriveLoader") {
            const onNodeCreated = nodeType.prototype.onNodeCreated;

            nodeType.prototype.onNodeCreated = function () {
                const r = onNodeCreated ? onNodeCreated.apply(this, arguments) : undefined;

                // Add the visual "Browse Google Drive" button
                this.addWidget("button", "Browse Google Drive", "📂 Open Drive", async () => {
                    await loadGoogleApiAndOpenPicker(this);
                });

                return r;
            };
        }
    }
});

// --- Google Picker Lifecycle ---

async function loadGoogleApiAndOpenPicker(node) {
    if (typeof gapi === "undefined") {
        const script = document.createElement("script");
        script.src = "https://apis.google.com/js/api.js";
        script.onload = () => gapi.load('picker', () => authenticateAndPick(node));
        document.head.appendChild(script);

        const gisScript = document.createElement("script");
        gisScript.src = "https://accounts.google.com/gsi/client";
        document.head.appendChild(gisScript);
    } else {
        authenticateAndPick(node);
    }
}

function authenticateAndPick(node) {
    const tokenClient = google.accounts.oauth2.initTokenClient({
        client_id: CLIENT_ID,
        scope: SCOPES,
        callback: (tokenResponse) => {
            if (tokenResponse.error !== undefined) {
                console.error("Google Auth Error:", tokenResponse);
                return;
            }
            showPicker(tokenResponse.access_token, node);
        },
    });
    tokenClient.requestAccessToken();
}

function showPicker(accessToken, node) {
    const view = new google.picker.DocsView(google.picker.ViewId.DOCS);
    const picker = new google.picker.PickerBuilder()
        .addView(view)
        .setOAuthToken(accessToken)
        .setDeveloperKey('YOUR_API_KEY_IF_REQUIRED_OR_REMOVE') // Optional depending on GCP setup
        .setCallback((data) => pickerCallback(data, accessToken, node))
        .build();
    picker.setVisible(true);
}

// --- Download and Upload Logic ---

async function pickerCallback(data, accessToken, node) {
    if (data[google.picker.Response.ACTION] === google.picker.Action.PICKED) {
        const doc = data[google.picker.Response.DOCUMENTS][0];
        const fileId = doc[google.picker.Document.ID];
        const fileName = doc[google.picker.Document.NAME];

        console.log(`Downloading ${fileName} from Google Drive...`);

        // 1. Download the raw file bytes directly from Google Drive
        const response = await fetch(`https://www.googleapis.com/drive/v3/files/${fileId}?alt=media`, {
            headers: { Authorization: `Bearer ${accessToken}` }
        });
        const blob = await response.blob();

        // 2. Upload the raw bytes to ComfyUI's native input folder
        const formData = new FormData();
        formData.append("image", blob, fileName);

        console.log("Uploading file to ComfyUI backend...");
        const uploadResponse = await api.fetchApi("/upload/image", {
            method: "POST",
            body: formData,
        });

        if (uploadResponse.ok) {
            const uploadData = await uploadResponse.json();

            // 3. Bind the uploaded filename to the node's widget
            const imageWidget = node.widgets.find(w => w.name === "image");
            if (imageWidget) {
                imageWidget.value = uploadData.name;
                app.graph.setDirtyCanvas(true);
            }
            alert(`Successfully loaded: ${uploadData.name}`);
        }
    }
}