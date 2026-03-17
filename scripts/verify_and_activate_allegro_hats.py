#!/usr/bin/env python3
"""
Verify and activate Allegro PFT cap offers.
Checks both Set A (inactive drafts) and Set B (claimed active) offers,
then attempts to activate any that are not ACTIVE.
"""

import json
import time
import os
from pathlib import Path
from datetime import datetime

import httpx
from dotenv import load_dotenv, set_key

# ── Config ────────────────────────────────────────────────────────────────────
ENV_PATH = Path("~/.keys/allegro.env").expanduser()
BASE_URL = "https://api.allegro.pl"
ALLEGRO_HEADERS = {
    "Accept": "application/vnd.allegro.public.v1+json",
    "Content-Type": "application/vnd.allegro.public.v1+json",
}

# Set A — created as INACTIVE drafts
SET_A = [
    {"sku": "PFT-WDH", "offer_id": "18414805632", "price": 99.00},
    {"sku": "PFT-OBH", "offer_id": "18414805647", "price": 119.00},
    {"sku": "PFT-BBH", "offer_id": "18414805665", "price": 109.00},
    {"sku": "PFT-DDH", "offer_id": "18414805695", "price": 99.00},
    {"sku": "PFT-CCH", "offer_id": "18414805720", "price": 109.00},
]

# Set B — shown as ACTIVE in previous run
SET_B = [
    {"sku": "PFT-WDH", "offer_id": "18414728279", "price": 89.99},
    {"sku": "PFT-DDH", "offer_id": "18414731024", "price": 96.99},
    {"sku": "PFT-CCH", "offer_id": "18414731041", "price": 104.99},
    {"sku": "PFT-OBH", "offer_id": "18414731001", "price": 119.99},
    {"sku": "PFT-BBH", "offer_id": "18414808073", "price": 99.99},
]


# ── Token management ──────────────────────────────────────────────────────────
def load_env() -> dict:
    load_dotenv(ENV_PATH, override=True)
    return {
        "client_id": os.environ["ALLEGRO_CLIENT_ID"],
        "client_secret": os.environ["ALLEGRO_CLIENT_SECRET"],
        "access_token": os.environ["ALLEGRO_ACCESS_TOKEN"],
        "refresh_token": os.environ["ALLEGRO_REFRESH_TOKEN"],
        "expires_at": int(os.environ.get("ALLEGRO_TOKEN_EXPIRES_AT", "0")),
    }


def refresh_token(creds: dict) -> str:
    """Refresh the access token and save new tokens to env file."""
    print("Token expired — refreshing...")
    resp = httpx.post(
        "https://allegro.pl/auth/oauth/token",
        auth=(creds["client_id"], creds["client_secret"]),
        data={
            "grant_type": "refresh_token",
            "refresh_token": creds["refresh_token"],
        },
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    new_access = data["access_token"]
    new_refresh = data.get("refresh_token", creds["refresh_token"])
    new_expires_at = int(time.time()) + int(data.get("expires_in", 3600))

    # Persist to env file
    set_key(str(ENV_PATH), "ALLEGRO_ACCESS_TOKEN", new_access)
    set_key(str(ENV_PATH), "ALLEGRO_REFRESH_TOKEN", new_refresh)
    set_key(str(ENV_PATH), "ALLEGRO_TOKEN_EXPIRES_AT", str(new_expires_at))

    print(f"Token refreshed. New expiry: {datetime.fromtimestamp(new_expires_at).isoformat()}")
    return new_access


def get_access_token() -> str:
    creds = load_env()
    now = int(time.time())
    if now >= creds["expires_at"] - 60:  # 60s buffer
        return refresh_token(creds)
    return creds["access_token"]


# ── API helpers ───────────────────────────────────────────────────────────────
def get_offer(client: httpx.Client, offer_id: str, token: str) -> dict | None:
    """Fetch offer details. Returns None if 404."""
    url = f"{BASE_URL}/sale/product-offers/{offer_id}"
    headers = {**ALLEGRO_HEADERS, "Authorization": f"Bearer {token}"}
    resp = client.get(url, headers=headers, timeout=30)
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()


def activate_offer(client: httpx.Client, offer_id: str, token: str) -> dict:
    """Attempt to activate an offer. Returns the response JSON."""
    url = f"{BASE_URL}/sale/product-offers/{offer_id}"
    headers = {**ALLEGRO_HEADERS, "Authorization": f"Bearer {token}"}
    payload = {"publication": {"status": "ACTIVE"}}
    resp = client.patch(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    token = get_access_token()
    results = {
        "run_at": datetime.now().isoformat(),
        "set_a": [],
        "set_b": [],
        "summary": {
            "active": [],
            "activated": [],
            "failed": [],
            "not_found": [],
        },
    }

    print("\n" + "=" * 60)
    print("VERIFYING AND ACTIVATING ALLEGRO PFT CAP OFFERS")
    print("=" * 60)

    with httpx.Client() as client:
        for set_name, offer_set, result_key in [
            ("Set A (draft)", SET_A, "set_a"),
            ("Set B (claimed active)", SET_B, "set_b"),
        ]:
            print(f"\n── {set_name} ──────────────────────────────────────")
            for entry in offer_set:
                sku = entry["sku"]
                offer_id = entry["offer_id"]
                print(f"\n  [{sku}] offer_id={offer_id}")

                # Check current status
                try:
                    offer_data = get_offer(client, offer_id, token)
                except httpx.HTTPStatusError as e:
                    print(f"    ERROR fetching: {e.response.status_code} — {e.response.text[:200]}")
                    record = {
                        "sku": sku,
                        "offer_id": offer_id,
                        "status": "ERROR",
                        "error": f"{e.response.status_code}: {e.response.text[:200]}",
                    }
                    results[result_key].append(record)
                    results["summary"]["failed"].append({"sku": sku, "offer_id": offer_id, "reason": record["error"]})
                    time.sleep(0.5)
                    continue

                if offer_data is None:
                    print(f"    NOT FOUND (404)")
                    record = {"sku": sku, "offer_id": offer_id, "status": "NOT_FOUND"}
                    results[result_key].append(record)
                    results["summary"]["not_found"].append({"sku": sku, "offer_id": offer_id})
                    time.sleep(0.5)
                    continue

                # Extract key fields
                pub = offer_data.get("publication", {})
                status = pub.get("status", "UNKNOWN")
                title = offer_data.get("name", "(no title)")
                # Price extraction — product-offers schema
                selling = offer_data.get("sellingMode", {})
                price_val = selling.get("price", {}).get("amount", "?")
                allegro_url = f"https://allegro.pl/oferta/{offer_id}"

                print(f"    Title:  {title}")
                print(f"    Status: {status}")
                print(f"    Price:  {price_val} PLN")
                print(f"    URL:    {allegro_url}")

                record = {
                    "sku": sku,
                    "offer_id": offer_id,
                    "title": title,
                    "status_before": status,
                    "price": price_val,
                    "allegro_url": allegro_url,
                }

                if status == "ACTIVE":
                    print(f"    -> Already ACTIVE")
                    record["action"] = "none"
                    record["status_after"] = "ACTIVE"
                    results["summary"]["active"].append({
                        "sku": sku,
                        "offer_id": offer_id,
                        "title": title,
                        "price": price_val,
                        "url": allegro_url,
                    })
                else:
                    # Try to activate
                    print(f"    -> Attempting activation...")
                    try:
                        time.sleep(0.5)
                        act_resp = activate_offer(client, offer_id, token)
                        new_status = act_resp.get("publication", {}).get("status", "UNKNOWN")
                        print(f"    -> Activation response status: {new_status}")
                        record["action"] = "activation_attempted"
                        record["status_after"] = new_status
                        if new_status == "ACTIVE":
                            results["summary"]["activated"].append({
                                "sku": sku,
                                "offer_id": offer_id,
                                "title": title,
                                "price": price_val,
                                "url": allegro_url,
                            })
                        else:
                            results["summary"]["failed"].append({
                                "sku": sku,
                                "offer_id": offer_id,
                                "reason": f"Activation returned status: {new_status}",
                            })
                    except httpx.HTTPStatusError as e:
                        err_text = e.response.text[:300]
                        print(f"    -> Activation FAILED: {e.response.status_code} — {err_text}")
                        record["action"] = "activation_failed"
                        record["status_after"] = status
                        record["error"] = f"{e.response.status_code}: {err_text}"
                        results["summary"]["failed"].append({
                            "sku": sku,
                            "offer_id": offer_id,
                            "reason": record["error"],
                        })

                results[result_key].append(record)
                time.sleep(0.5)

    # ── Final report ──────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("FINAL REPORT")
    print("=" * 60)

    active_all = results["summary"]["active"] + results["summary"]["activated"]

    print(f"\nCONFIRMED ACTIVE ({len(active_all)}):")
    for item in active_all:
        print(f"  [{item['sku']}] {item['offer_id']} — {item.get('title', '?')[:60]}")
        print(f"    Price: {item['price']} PLN")
        print(f"    URL:   {item['url']}")

    if results["summary"]["activated"]:
        print(f"\nJUST ACTIVATED ({len(results['summary']['activated'])}):")
        for item in results["summary"]["activated"]:
            print(f"  [{item['sku']}] {item['offer_id']} — {item['url']}")

    if results["summary"]["not_found"]:
        print(f"\nNOT FOUND ({len(results['summary']['not_found'])}):")
        for item in results["summary"]["not_found"]:
            print(f"  [{item['sku']}] {item['offer_id']}")

    if results["summary"]["failed"]:
        print(f"\nFAILED ({len(results['summary']['failed'])}):")
        for item in results["summary"]["failed"]:
            print(f"  [{item['sku']}] {item['offer_id']} — {item['reason'][:120]}")

    # Save results
    out_path = Path("/Users/alexanderrogalski/nesell-analytics/scripts/activation_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(f"\nResults saved to: {out_path}")

    return results


if __name__ == "__main__":
    main()
