"""
HTTP server for the Image Picker web app.

Serves static/image-picker.html and provides API endpoints for:
  - Reading/writing image_config.json
  - Generating Printful mockups (with 24h cache)
  - Listing product templates and color variants

Usage:
    python3.11 -m etl.image_server
"""

import json
import os
import time
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer

from etl.image_manager import (
    SLOT_NAMES,
    load_image_config,
    save_image_config,
    _load_printful_key,
)

PORT = 8520
ETL_DIR = os.path.dirname(__file__)
CACHE_PATH = os.path.join(ETL_DIR, "image_mockups_cache.json")
STATIC_DIR = os.path.join(ETL_DIR, "static")
CACHE_TTL = 86400  # 24 hours


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------

def _load_cache() -> dict:
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}


def _save_cache(cache: dict) -> None:
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)
        f.write("\n")


def _get_cached(product_type: str, cache_key: str) -> dict | None:
    entry = _load_cache().get(product_type, {}).get(cache_key)
    if not entry:
        return None
    if time.time() - entry.get("timestamp", 0) > CACHE_TTL:
        return None
    return entry.get("mockups")


def _set_cached(product_type: str, cache_key: str, mockups: dict) -> None:
    cache = _load_cache()
    cache.setdefault(product_type, {})[cache_key] = {
        "timestamp": time.time(),
        "mockups": mockups,
    }
    _save_cache(cache)


# ---------------------------------------------------------------------------
# Printful API helpers
# ---------------------------------------------------------------------------

def _load_store_id() -> str:
    sid = os.environ.get("PRINTFUL_STORE_ID")
    if sid:
        return sid
    env_path = os.path.expanduser("~/.keys/printful.env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip().startswith("PRINTFUL_STORE_ID="):
                    return line.strip().split("=", 1)[1].strip().strip("'\"")
    return ""


def _pf_headers() -> dict:
    key = _load_printful_key()
    headers = {
        "Authorization": f"Bearer {key}",
        "Content-Type": "application/json",
    }
    store_id = _load_store_id()
    if store_id:
        headers["X-PF-Store-Id"] = store_id
    return headers


def _get_templates() -> list:
    """Return all product templates from Printful store."""
    import requests
    headers = _pf_headers()
    all_templates = []
    offset = 0
    while True:
        r = requests.get(
            f"https://api.printful.com/product-templates?limit=100&offset={offset}",
            headers=headers, timeout=30,
        )
        r.raise_for_status()
        items = r.json().get("result", {}).get("items", [])
        all_templates.extend(items)
        if len(items) < 100:
            break
        offset += 100
    return all_templates


def _get_variants(printful_product_id: int) -> list:
    """Return [{variant_id, name, color}] for a catalog product (one per color)."""
    import requests
    headers = _pf_headers()
    r = requests.get(
        f"https://api.printful.com/products/{printful_product_id}",
        headers=headers, timeout=30,
    )
    r.raise_for_status()
    seen = set()
    variants = []
    for v in r.json().get("result", {}).get("variants", []):
        color = v.get("color", "")
        if color and color not in seen:
            seen.add(color)
            variants.append({
                "variant_id": v["id"],
                "name": v.get("name", ""),
                "color": color,
            })
    return variants


AMAZON_OPTION_GROUPS = [
    "Flat",
    "Flat, Premium",
    "Production",
    "Product details",
]


def _get_option_groups(printful_product_id: int) -> list[str]:
    """Return available option_groups for a product from Printful printfiles API."""
    import requests
    headers = _pf_headers()
    r = requests.get(
        f"https://api.printful.com/mockup-generator/printfiles/{printful_product_id}",
        headers=headers, timeout=30,
    )
    r.raise_for_status()
    return r.json().get("result", {}).get("option_groups", [])


def _generate_mockups(
    printful_product_id: int,
    variant_id: int,
    template_id: int | None = None,
) -> dict:
    """Generate mockups via Printful. Returns {generator_mockup_id: {url, label, group}}."""
    import requests
    headers = _pf_headers()

    # Get available groups and filter to Amazon-useful ones
    available = _get_option_groups(printful_product_id)
    groups = [g for g in AMAZON_OPTION_GROUPS if g in available]

    payload: dict = {"variant_ids": [variant_id]}
    if template_id:
        payload["product_template_id"] = template_id
    if groups:
        payload["option_groups"] = groups

    url = f"https://api.printful.com/mockup-generator/create-task/{printful_product_id}"
    resp = requests.post(url, json=payload, headers=headers, timeout=30)
    resp.raise_for_status()
    task_key = resp.json()["result"]["task_key"]

    poll_url = f"https://api.printful.com/mockup-generator/task?task_key={task_key}"
    for _ in range(40):
        time.sleep(10)
        pr = requests.get(poll_url, headers=headers, timeout=30)
        pr.raise_for_status()
        result = pr.json().get("result", {})
        status = result.get("status")

        if status == "completed":
            out: dict[str, dict] = {}
            for m in result.get("mockups", []):
                gid = m.get("generator_mockup_id")
                murl = m.get("mockup_url")
                placement = m.get("placement", "")
                if gid and murl:
                    out[str(gid)] = {
                        "url": murl,
                        "label": placement.replace("_", " ").title(),
                        "group": "Main",
                    }
                for ex in m.get("extra", []):
                    egid = ex.get("generator_mockup_id")
                    eurl = ex.get("url")
                    title = ex.get("title") or ex.get("option") or ""
                    group = ex.get("option_group", "")
                    if egid and eurl:
                        out[str(egid)] = {
                            "url": eurl,
                            "label": f"{title} ({group})" if group else title,
                            "group": group,
                        }
            return out

        elif status == "failed":
            raise RuntimeError(f"Mockup generation failed: {result.get('error')}")

    raise TimeoutError("Timed out waiting for Printful mockup generation")


# ---------------------------------------------------------------------------
# Request handler
# ---------------------------------------------------------------------------

class ImagePickerHandler(BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        print(f"  {self.address_string()} - {fmt % args}")

    def _json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _err(self, msg, status=400):
        self._json({"error": msg}, status)

    def _body(self) -> bytes:
        length = int(self.headers.get("Content-Length", 0))
        return self.rfile.read(length) if length else b""

    def do_GET(self):
        path = self.path.split("?")[0]
        parts = [p for p in path.split("/") if p]

        # Serve HTML
        if path in ("/", "/index.html"):
            html_path = os.path.join(STATIC_DIR, "image-picker.html")
            if not os.path.exists(html_path):
                self.send_error(404, "image-picker.html not found")
                return
            with open(html_path, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # GET /api/config
        if path == "/api/config":
            self._json(load_image_config())
            return

        # GET /api/variants/<product_type>
        if len(parts) == 3 and parts[:2] == ["api", "variants"]:
            config = load_image_config()
            pt = parts[2]
            if pt not in config:
                return self._err(f"Unknown product type: {pt}", 404)
            pid = config[pt].get("printful_product_id")
            if not pid:
                return self._err("No printful_product_id", 400)
            try:
                self._json(_get_variants(pid))
            except Exception as e:
                self._err(str(e), 500)
            return

        # GET /api/templates/<product_type>
        if len(parts) == 3 and parts[:2] == ["api", "templates"]:
            config = load_image_config()
            pt = parts[2]
            if pt not in config:
                return self._err(f"Unknown product type: {pt}", 404)
            try:
                templates = _get_templates()
                self._json([
                    {"id": t["id"], "title": t.get("title", "")}
                    for t in templates
                ])
            except Exception as e:
                self._err(str(e), 500)
            return

        self.send_error(404, "Not found")

    def do_POST(self):
        path = self.path.split("?")[0]
        parts = [p for p in path.split("/") if p]

        # POST /api/config/<product_type>/order
        if len(parts) == 4 and parts[:2] == ["api", "config"] and parts[3] == "order":
            pt = parts[2]
            try:
                data = json.loads(self._body())
            except json.JSONDecodeError as e:
                return self._err(str(e))

            # Accept [{id, label}] or [id, id, ...]
            if isinstance(data, list) and data and isinstance(data[0], dict):
                new_order = []
                for i, item in enumerate(data[:len(SLOT_NAMES)]):
                    new_order.append({
                        "slot": SLOT_NAMES[i],
                        "placement_id": int(item["id"]),
                        "label": item.get("label", ""),
                    })
            elif isinstance(data, list):
                new_ids = [int(x) for x in data]
                if len(new_ids) > len(SLOT_NAMES):
                    return self._err(f"Max {len(SLOT_NAMES)} slots")
                config = load_image_config()
                if pt not in config:
                    return self._err(f"Unknown: {pt}", 404)
                labels = {
                    item["placement_id"]: item.get("label", "")
                    for item in config[pt].get("image_order", [])
                }
                new_order = [
                    {"slot": SLOT_NAMES[i], "placement_id": pid, "label": labels.get(pid, "")}
                    for i, pid in enumerate(new_ids)
                ]
            else:
                return self._err("Body must be array")

            config = load_image_config()
            if pt not in config:
                return self._err(f"Unknown: {pt}", 404)
            config[pt]["image_order"] = new_order
            save_image_config(config)
            self._json({"ok": True, "image_order": new_order})
            return

        # POST /api/generate/<product_type>/<variant_id>
        if len(parts) == 4 and parts[:2] == ["api", "generate"]:
            pt = parts[2]
            vid = parts[3]
            config = load_image_config()
            if pt not in config:
                return self._err(f"Unknown: {pt}", 404)

            entry = config[pt]
            pid = entry.get("printful_product_id")
            if not pid:
                return self._err("No printful_product_id", 400)

            # Optional template_id from body
            template_id = None
            body = self._body()
            if body:
                try:
                    bd = json.loads(body)
                    template_id = bd.get("template_id")
                except json.JSONDecodeError:
                    pass

            cache_key = f"{vid}_{template_id}" if template_id else vid
            cached = _get_cached(pt, cache_key)
            if cached is not None:
                return self._json({"cached": True, "mockups": cached})

            try:
                mockups = _generate_mockups(int(pid), int(vid), template_id)
                _set_cached(pt, cache_key, mockups)
                self._json({"cached": False, "mockups": mockups})
            except Exception as e:
                self._err(str(e), 500)
            return

        self.send_error(404, "Not found")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    server = HTTPServer(("localhost", PORT), ImagePickerHandler)
    url = f"http://localhost:{PORT}"
    print(f"Image Picker running at {url}")
    webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")
