import json
import time
from typing import Any, Dict, List, Optional

import requests

# --- CONFIGURATION ---
BASE_URL = "https://www.businessprocessapplications.com"

# Temporary API key. Rotate this after testing because it was pasted into chat.
TEST_API_KEY = "2c721a67-0212-4907-ae82-f4039866ef56"
USERNAME = "asdfasdf"

TIMEOUT_SECONDS = 15


def build_headers(auth_mode: str) -> Dict[str, str]:
    """
    auth_mode:
      - bearer: Authorization: Bearer <key>
      - api_key_header: X-API-Key / X-ZAI-API-Key style headers
      - none: no auth headers except content type
    """
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "X-ZAI-User": USERNAME,
    }

    if auth_mode == "bearer":
        headers["Authorization"] = f"Bearer {TEST_API_KEY}"
    elif auth_mode == "api_key_header":
        headers["X-API-Key"] = TEST_API_KEY
        headers["X-ZAI-API-Key"] = TEST_API_KEY
        headers["X-ZMongo-API-Key"] = TEST_API_KEY

    return headers


ENDPOINTS_TO_TEST: List[Dict[str, Any]] = [
    # Public / unauthenticated health.
    {
        "name": "Comfy ZMongo Health",
        "method": "GET",
        "path": "/comfy-zmongo/health",
        "auth_modes": ["none"],
    },

    # Auth verify. This may expect a JWT, not an API key, so 401 is informative.
    {
        "name": "Auth Verify using Bearer Token",
        "method": "GET",
        "path": "/api/auth/verify",
        "auth_modes": ["bearer"],
    },

    # Corrected manager route. This is browser/session-oriented, so API-key auth may still fail.
    {
        "name": "Manager Collections",
        "method": "GET",
        "path": "/user/manager/api/collections",
        "auth_modes": ["bearer", "api_key_header"],
    },

    # Corrected Comfy route variants.
    {
        "name": "Comfy Collections - deployed prefix candidate",
        "method": "GET",
        "path": "/comfy-zmongo/api/collections",
        "auth_modes": ["bearer", "api_key_header"],
    },
    {
        "name": "Comfy Collections - canonical target prefix candidate",
        "method": "GET",
        "path": "/api/comfy-zmongo/collections",
        "auth_modes": ["bearer", "api_key_header"],
    },
    {
        "name": "Comfy Docs sdxl_presets - deployed prefix candidate",
        "method": "GET",
        "path": "/comfy-zmongo/api/docs/sdxl_presets",
        "auth_modes": ["bearer", "api_key_header"],
    },
    {
        "name": "Comfy Docs sdxl_presets - canonical target prefix candidate",
        "method": "GET",
        "path": "/api/comfy-zmongo/docs/sdxl_presets",
        "auth_modes": ["bearer", "api_key_header"],
    },

    # Fleet route variants. /fleet/status currently appears to 404 from public testing.
    {
        "name": "Fleet Status",
        "method": "GET",
        "path": "/fleet/status",
        "auth_modes": ["none", "bearer", "api_key_header"],
    },
    {
        "name": "Fleet Root",
        "method": "GET",
        "path": "/fleet/",
        "auth_modes": ["none", "bearer", "api_key_header"],
    },
]


def compact_response_text(response: requests.Response, limit: int = 220) -> str:
    text = response.text.strip()

    if not text:
        return "<empty response>"

    try:
        parsed = response.json()
        text = json.dumps(parsed, ensure_ascii=False, sort_keys=True)
    except ValueError:
        pass

    if len(text) > limit:
        return text[:limit] + "..."

    return text


def request_once(
    method: str,
    url: str,
    headers: Dict[str, str],
    json_body: Optional[Dict[str, Any]] = None,
) -> requests.Response:
    if method == "GET":
        return requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS, allow_redirects=False)

    if method == "POST":
        return requests.post(
            url,
            headers=headers,
            json=json_body or {},
            timeout=TIMEOUT_SECONDS,
            allow_redirects=False,
        )

    raise ValueError(f"Unsupported method: {method}")


def classify_status(status_code: int) -> str:
    if 200 <= status_code < 300:
        return "SUCCESS"
    if status_code in (301, 302, 303, 307, 308):
        return "REDIRECT"
    if status_code in (401, 403):
        return "AUTH"
    if status_code == 404:
        return "MISSING"
    if 500 <= status_code < 600:
        return "SERVER"
    return "FAILED"


def run_endpoint_test(endpoint: Dict[str, Any]) -> Dict[str, Any]:
    name = endpoint["name"]
    method = endpoint["method"]
    path = endpoint["path"]
    auth_modes = endpoint.get("auth_modes", ["bearer"])
    json_body = endpoint.get("json")

    url = f"{BASE_URL}{path}"

    print(f"\nTesting: {name}")
    print(f"   Path: {method} {url}")

    attempts = []

    for auth_mode in auth_modes:
        headers = build_headers(auth_mode)

        try:
            start_time = time.time()
            response = request_once(method, url, headers, json_body=json_body)
            latency_ms = round((time.time() - start_time) * 1000, 2)

            status_class = classify_status(response.status_code)
            location = response.headers.get("Location")

            attempt = {
                "auth_mode": auth_mode,
                "status_code": response.status_code,
                "status_class": status_class,
                "latency_ms": latency_ms,
                "location": location,
                "response": compact_response_text(response),
            }
            attempts.append(attempt)

            icon = {
                "SUCCESS": "✅",
                "REDIRECT": "↪️",
                "AUTH": "🔐",
                "MISSING": "❌",
                "SERVER": "🔥",
                "FAILED": "⚠️",
            }.get(status_class, "⚠️")

            print(
                f"   {icon} {auth_mode}: {response.status_code} "
                f"({status_class}) - {latency_ms}ms"
            )

            if location:
                print(f"      Redirect: {location}")

            print(f"      Response: {attempt['response']}")

        except requests.exceptions.RequestException as exc:
            attempts.append(
                {
                    "auth_mode": auth_mode,
                    "status_code": None,
                    "status_class": "CONNECTION",
                    "latency_ms": None,
                    "location": None,
                    "response": str(exc),
                }
            )
            print(f"   🚨 {auth_mode}: CONNECTION ERROR - {exc}")

    best = pick_best_attempt(attempts)

    return {
        "name": name,
        "method": method,
        "path": path,
        "best": best,
        "attempts": attempts,
    }


def pick_best_attempt(attempts: List[Dict[str, Any]]) -> Dict[str, Any]:
    priority = {
        "SUCCESS": 0,
        "REDIRECT": 1,
        "AUTH": 2,
        "SERVER": 3,
        "FAILED": 4,
        "MISSING": 5,
        "CONNECTION": 6,
    }

    return sorted(
        attempts,
        key=lambda item: priority.get(item.get("status_class", "FAILED"), 99),
    )[0]


def print_summary(results: List[Dict[str, Any]]) -> None:
    print("\n=========================================")
    print("Smoke Test Summary")
    print("=========================================")

    counts: Dict[str, int] = {}

    for result in results:
        best = result["best"]
        status_class = best["status_class"]
        counts[status_class] = counts.get(status_class, 0) + 1

        print(
            f"{status_class:10} "
            f"{str(best['status_code']):>4} "
            f"{best['auth_mode']:>14} "
            f"{result['method']} {result['path']} "
            f"- {result['name']}"
        )

    print("\nCounts:")
    for status_class in sorted(counts):
        print(f"   {status_class}: {counts[status_class]}")

    print("\nInterpretation:")
    print("   SUCCESS  = route exists and worked")
    print("   REDIRECT = route exists, probably browser/session login required")
    print("   AUTH     = route exists, credentials/token format rejected")
    print("   MISSING  = route is not registered at that path")
    print("   SERVER   = route exists but crashed")
    print("   FAILED   = route exists or responded, but not cleanly")


def run_smoke_test() -> None:
    print(f"Initiating smoke test against {BASE_URL}")
    print(f"Username: {USERNAME}")
    print("Note: API key is tested as both Bearer and API-key headers where applicable.")

    results = []

    for endpoint in ENDPOINTS_TO_TEST:
        results.append(run_endpoint_test(endpoint))

    print_summary(results)


if __name__ == "__main__":
    run_smoke_test()