"""
Product image configuration manager for Amazon listings.

Manages image_config.json which defines per-product-type ordering of
Printful mockup placements to Amazon image slots (main, other_1..other_7).

Usage:
    python3.11 -m etl.image_manager --product dad_hat --show
    python3.11 -m etl.image_manager --product dad_hat --set-order 15086,15090,15091,15087
    python3.11 -m etl.image_manager --product dad_hat --generate --variant-id 12345
"""

import argparse
import json
import os
import sys
import time
from typing import Any

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "image_config.json")

SLOT_NAMES = [
    "main",
    "other_1",
    "other_2",
    "other_3",
    "other_4",
    "other_5",
    "other_6",
    "other_7",
]

AMAZON_ATTR_KEYS = [
    "main_product_image_locator",
    "other_product_image_locator_1",
    "other_product_image_locator_2",
    "other_product_image_locator_3",
    "other_product_image_locator_4",
    "other_product_image_locator_5",
    "other_product_image_locator_6",
    "other_product_image_locator_7",
]


# ---------------------------------------------------------------------------
# Config I/O
# ---------------------------------------------------------------------------

def load_image_config() -> dict:
    """Load and return the image config dict from image_config.json."""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_image_config(config: dict) -> None:
    """Save config dict back to image_config.json with indent=2."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, ensure_ascii=False)
        f.write("\n")


# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------

def get_image_order(product_type: str) -> list[int]:
    """Return ordered list of placement IDs for a product type.

    Raises:
        ValueError: If product_type is not found in config.
    """
    config = load_image_config()
    if product_type not in config:
        available = ", ".join(config.keys())
        raise ValueError(
            f"Product type '{product_type}' not found in config. "
            f"Available: {available}"
        )
    return [entry["placement_id"] for entry in config[product_type]["image_order"]]


def build_image_attrs(
    product_type: str,
    mockup_urls: dict[int | str, str],
    marketplace_id: str,
) -> dict[str, list[dict[str, str]]]:
    """Build Amazon listing image attributes from mockup URLs.

    Args:
        product_type: Key in image_config.json (e.g. 'dad_hat').
        mockup_urls: Mapping of placement_id -> URL. Keys can be int or str.
        marketplace_id: Amazon marketplace ID (e.g. 'A1PA6795UKMFR9').

    Returns:
        Dict mapping Amazon image attribute keys to locator values, e.g.:
        {
            "main_product_image_locator": [
                {"media_location": "https://...", "marketplace_id": "A1PA6795UKMFR9"}
            ],
            "other_product_image_locator_1": [...],
            ...
        }
        Only includes slots that have a matching URL in mockup_urls.
    """
    order = get_image_order(product_type)

    # Normalize mockup_urls keys to int
    normalized: dict[int, str] = {}
    for k, v in mockup_urls.items():
        normalized[int(k)] = v

    attrs: dict[str, list[dict[str, str]]] = {}

    for idx, placement_id in enumerate(order):
        if idx >= len(AMAZON_ATTR_KEYS):
            break
        url = normalized.get(placement_id)
        if url is None:
            continue
        attr_key = AMAZON_ATTR_KEYS[idx]
        attrs[attr_key] = [
            {"media_location": url, "marketplace_id": marketplace_id}
        ]

    return attrs


# ---------------------------------------------------------------------------
# CLI: --show
# ---------------------------------------------------------------------------

def _cmd_show(product_type: str) -> None:
    """Display current image config for a product type in table format."""
    config = load_image_config()
    if product_type not in config:
        print(f"Error: product type '{product_type}' not found.", file=sys.stderr)
        print(f"Available: {', '.join(config.keys())}", file=sys.stderr)
        sys.exit(1)

    entry = config[product_type]
    print(f"Product: {entry.get('label', product_type)}")
    print(f"Printful product ID: {entry.get('printful_product_id', 'N/A')}")
    print()

    order = entry.get("image_order", [])
    if not order:
        print("  (no image order configured)")
        return

    # Table header
    hdr_slot = "Slot"
    hdr_pid = "Placement ID"
    hdr_label = "Label"
    hdr_amazon = "Amazon Attribute"

    w_slot = max(len(hdr_slot), max(len(e["slot"]) for e in order))
    w_pid = max(len(hdr_pid), max(len(str(e["placement_id"])) for e in order))
    w_label = max(len(hdr_label), max(len(e.get("label", "")) for e in order))
    w_amz = max(len(hdr_amazon), max(len(AMAZON_ATTR_KEYS[i]) for i in range(len(order)) if i < len(AMAZON_ATTR_KEYS)))

    fmt = f"  {{:<{w_slot}}}  {{:<{w_pid}}}  {{:<{w_label}}}  {{:<{w_amz}}}"
    sep = f"  {'-' * w_slot}  {'-' * w_pid}  {'-' * w_label}  {'-' * w_amz}"

    print(fmt.format(hdr_slot, hdr_pid, hdr_label, hdr_amazon))
    print(sep)

    for i, e in enumerate(order):
        amz_key = AMAZON_ATTR_KEYS[i] if i < len(AMAZON_ATTR_KEYS) else "(overflow)"
        print(fmt.format(e["slot"], str(e["placement_id"]), e.get("label", ""), amz_key))


# ---------------------------------------------------------------------------
# CLI: --set-order
# ---------------------------------------------------------------------------

def _cmd_set_order(product_type: str, ids_str: str) -> None:
    """Set a new image order for a product type.

    Args:
        product_type: Key in config.
        ids_str: Comma-separated placement IDs in desired order.
    """
    config = load_image_config()
    if product_type not in config:
        print(f"Error: product type '{product_type}' not found.", file=sys.stderr)
        sys.exit(1)

    entry = config[product_type]
    current_order = entry.get("image_order", [])

    # Build lookup: placement_id -> label
    label_lookup: dict[int, str] = {}
    for item in current_order:
        label_lookup[item["placement_id"]] = item.get("label", "")

    # Parse and validate new IDs
    try:
        new_ids = [int(x.strip()) for x in ids_str.split(",") if x.strip()]
    except ValueError:
        print("Error: placement IDs must be integers.", file=sys.stderr)
        sys.exit(1)

    known_ids = set(label_lookup.keys())
    unknown = [pid for pid in new_ids if pid not in known_ids]
    if unknown:
        print(
            f"Error: unknown placement IDs: {unknown}. "
            f"Known IDs: {sorted(known_ids)}",
            file=sys.stderr,
        )
        sys.exit(1)

    if len(new_ids) > len(SLOT_NAMES):
        print(
            f"Error: max {len(SLOT_NAMES)} slots supported, got {len(new_ids)}.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build new order with updated slot names
    new_order = []
    for i, pid in enumerate(new_ids):
        new_order.append({
            "slot": SLOT_NAMES[i],
            "placement_id": pid,
            "label": label_lookup.get(pid, ""),
        })

    entry["image_order"] = new_order
    save_image_config(config)

    print(f"Updated image order for '{product_type}' ({len(new_ids)} slots):")
    _cmd_show(product_type)


# ---------------------------------------------------------------------------
# CLI: --generate
# ---------------------------------------------------------------------------

def _load_printful_key() -> str:
    """Load Printful API key from environment or ~/.keys/printful.env."""
    for var in ("PRINTFUL_API_KEY", "PRINTFUL_API_TOKEN"):
        key = os.environ.get(var)
        if key:
            return key

    env_path = os.path.expanduser("~/.keys/printful.env")
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                for prefix in ("PRINTFUL_API_KEY=", "PRINTFUL_API_TOKEN="):
                    if line.startswith(prefix):
                        return line.split("=", 1)[1].strip().strip("'\"")

    print(
        "Error: PRINTFUL_API_KEY/TOKEN not found in env or ~/.keys/printful.env",
        file=sys.stderr,
    )
    sys.exit(1)


def _cmd_generate(product_type: str, variant_id: int) -> None:
    """Generate mockups via Printful API and display URLs."""
    import requests

    config = load_image_config()
    if product_type not in config:
        print(f"Error: product type '{product_type}' not found.", file=sys.stderr)
        sys.exit(1)

    entry = config[product_type]
    product_id = entry.get("printful_product_id")
    if not product_id:
        print(f"Error: no printful_product_id for '{product_type}'.", file=sys.stderr)
        sys.exit(1)

    order = entry.get("image_order", [])
    if not order:
        print(f"Error: no image_order configured for '{product_type}'.", file=sys.stderr)
        sys.exit(1)

    api_key = _load_printful_key()
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    store_id = os.environ.get("PRINTFUL_STORE_ID", "")
    if not store_id:
        env_path = os.path.expanduser("~/.keys/printful.env")
        if os.path.exists(env_path):
            with open(env_path, "r") as f:
                for line in f:
                    if line.strip().startswith("PRINTFUL_STORE_ID="):
                        store_id = line.strip().split("=", 1)[1].strip().strip("'\"")
    if store_id:
        headers["X-PF-Store-Id"] = store_id

    # Create mockup task
    create_url = f"https://api.printful.com/mockup-generator/create-task/{product_id}"
    payload: dict[str, Any] = {
        "variant_ids": [variant_id],
    }

    print(f"Creating mockup task for product {product_id}, variant {variant_id}...")
    resp = requests.post(create_url, json=payload, headers=headers, timeout=30)

    if resp.status_code != 200:
        print(f"Error creating mockup task: {resp.status_code}", file=sys.stderr)
        print(resp.text, file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    task_key = data.get("result", {}).get("task_key")
    if not task_key:
        print("Error: no task_key in response.", file=sys.stderr)
        print(json.dumps(data, indent=2), file=sys.stderr)
        sys.exit(1)

    print(f"Task key: {task_key}")
    print("Polling for results...")

    # Poll for completion
    poll_url = f"https://api.printful.com/mockup-generator/task?task_key={task_key}"
    max_attempts = 40  # 40 * 15s = 10 min max
    for attempt in range(1, max_attempts + 1):
        time.sleep(15)
        poll_resp = requests.get(poll_url, headers=headers, timeout=30)

        if poll_resp.status_code != 200:
            print(f"  Poll attempt {attempt}: HTTP {poll_resp.status_code}", file=sys.stderr)
            continue

        poll_data = poll_resp.json()
        status = poll_data.get("result", {}).get("status")
        print(f"  Poll attempt {attempt}: status={status}")

        if status == "completed":
            mockups = poll_data.get("result", {}).get("mockups", [])
            _display_mockup_results(mockups, order)
            return
        elif status == "failed":
            error = poll_data.get("result", {}).get("error")
            print(f"Error: mockup generation failed: {error}", file=sys.stderr)
            sys.exit(1)

    print("Error: timed out waiting for mockup generation.", file=sys.stderr)
    sys.exit(1)


def _display_mockup_results(mockups: list[dict], order: list[dict]) -> None:
    """Display mockup URLs grouped by placement."""
    # Build placement_id -> label lookup
    label_lookup = {e["placement_id"]: e.get("label", "") for e in order}
    ordered_ids = [e["placement_id"] for e in order]

    # Group mockup URLs by placement
    by_placement: dict[int, list[str]] = {}
    for mockup in mockups:
        placement = mockup.get("placement")
        url = mockup.get("mockup_url")
        extra = mockup.get("extra", [])

        if placement and url:
            # Printful returns placement as string like "front", not ID.
            # Also may include placement_id in some responses.
            pid = mockup.get("placement_id")
            if pid is not None:
                by_placement.setdefault(int(pid), []).append(url)
            else:
                # Print without grouping if no placement_id
                print(f"\n  Placement '{placement}':")
                print(f"    {url}")
                for ex in extra:
                    ex_url = ex.get("url")
                    if ex_url:
                        print(f"    {ex_url}")
                continue

        for ex in extra:
            ex_url = ex.get("url")
            ex_pid = ex.get("placement_id")
            if ex_url and ex_pid is not None:
                by_placement.setdefault(int(ex_pid), []).append(ex_url)

    if by_placement:
        print("\nMockup URLs by placement:")
        # Show in configured order first, then any remaining
        shown = set()
        for pid in ordered_ids:
            if pid in by_placement:
                label = label_lookup.get(pid, "")
                label_str = f" ({label})" if label else ""
                print(f"\n  Placement {pid}{label_str}:")
                for url in by_placement[pid]:
                    print(f"    {url}")
                shown.add(pid)

        for pid in sorted(by_placement.keys()):
            if pid not in shown:
                print(f"\n  Placement {pid} (not in config):")
                for url in by_placement[pid]:
                    print(f"    {url}")

    if not mockups:
        print("\n  (no mockups returned)")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Manage product image configuration for Amazon listings."
    )
    parser.add_argument(
        "--product",
        required=True,
        help="Product type key from image_config.json (e.g. dad_hat)",
    )
    parser.add_argument(
        "--show",
        action="store_true",
        help="Display current config for the product",
    )
    parser.add_argument(
        "--set-order",
        metavar="IDS",
        help="Comma-separated placement IDs in desired order",
    )
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate mockups via Printful API",
    )
    parser.add_argument(
        "--variant-id",
        type=int,
        metavar="VID",
        help="Printful variant ID (required with --generate)",
    )

    args = parser.parse_args()

    if args.generate and not args.variant_id:
        parser.error("--generate requires --variant-id")

    if args.show:
        _cmd_show(args.product)
    elif args.set_order:
        _cmd_set_order(args.product, args.set_order)
    elif args.generate:
        _cmd_generate(args.product, args.variant_id)
    else:
        # Default: show
        _cmd_show(args.product)


if __name__ == "__main__":
    main()
