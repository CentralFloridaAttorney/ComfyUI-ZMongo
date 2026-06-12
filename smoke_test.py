import requests
import json
import time

# --- CONFIGURATION ---
BASE_URL = "https://www.businessprocessapplications.com"
TEST_API_KEY = "2c721a67-0212-4907-ae82-f4039866ef56"  # Inserted Temporary API Key
USERNAME = "asdfasdf"

headers = {
    "Authorization": f"Bearer {TEST_API_KEY}",
    "Content-Type": "application/json",
    "X-ZAI-User": USERNAME,
}

# The secured endpoints that ComfyUI nodes actually call
ENDPOINTS_TO_TEST = [
    # 1. Core API Auth & Health
    {"name": "Auth Verify", "method": "GET", "path": "/api/auth/verify"},
    # 2. Comfy ZMongo Health (Confirmed Working in last test)
    {"name": "Comfy ZMongo Health", "method": "GET", "path": "/comfy-zmongo/health"},
    # 3. Manager UI API (Silo Access)
    {
        "name": "List User Collections",
        "method": "GET",
        "path": "/api/manager/collections",
    },
    # 4. ComfyUI Node Data Operations (GuardedZMongo bounds)
    {
        "name": "List ComfyUI Presets",
        "method": "GET",
        "path": "/comfy-zmongo/collections/sdxl_presets/docs",
    },
    # 5. Fleet / Agent Integration
    {"name": "Fleet Status", "method": "GET", "path": "/fleet/status"},
]


def run_smoke_test():
    print(f"🚀 Initiating Authenticated Smoke Test against {BASE_URL}...\n")

    passed = 0
    failed = 0

    for idx, endpoint in enumerate(ENDPOINTS_TO_TEST):
        name = endpoint["name"]
        method = endpoint["method"]
        path = endpoint["path"]
        url = f"{BASE_URL}{path}"

        print(f"[{idx + 1}/{len(ENDPOINTS_TO_TEST)}] Testing {name}...")
        print(f"   -> {method} {url}")

        try:
            start_time = time.time()
            if method == "GET":
                response = requests.get(url, headers=headers, timeout=10)
            elif method == "POST":
                response = requests.post(
                    url, headers=headers, json={"test": True}, timeout=10
                )

            latency = round((time.time() - start_time) * 1000, 2)

            if response.status_code == 200:
                print(f"   ✅ SUCCESS ({response.status_code}) - {latency}ms")
                try:
                    data = response.json()
                    snippet = (
                        json.dumps(data)[:150] + "..."
                        if len(json.dumps(data)) > 150
                        else json.dumps(data)
                    )
                    print(f"   📄 Payload: {snippet}\n")
                except:
                    print("   ⚠️ Warning: Response was not JSON.\n")
                passed += 1
            elif response.status_code in [401, 403]:
                print(
                    f"   ❌ AUTH FAILURE ({response.status_code}) - Check API Key or JWT implementation."
                )
                print(f"   📄 Response: {response.text[:100]}\n")
                failed += 1
            else:
                print(f"   ❌ FAILED ({response.status_code})")
                print(f"   📄 Response: {response.text[:100]}\n")
                failed += 1

        except requests.exceptions.RequestException as e:
            print(f"   🚨 CONNECTION ERROR: {e}\n")
            failed += 1

    print("=========================================")
    print(f"🏁 Test Complete. Passed: {passed} | Failed: {failed}")
    print("=========================================")


if __name__ == "__main__":
    run_smoke_test()
