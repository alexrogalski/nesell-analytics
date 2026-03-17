"""Printful Production Pipeline — generate designs, upload, create products, list on Amazon.

4-step PRODUCTION pipeline for SUM26 collection:
  1. Generate 10 embroidery-ready PNGs with Pillow
  2. Upload designs to Printful File Library
  3. Create 10 Printful Sync Products (dad hat / trucker / bucket)
  4. Create Amazon listings on DE + PL

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/pipeline_printful_production.py
    python3.11 scripts/pipeline_printful_production.py --step 2   # run from step 2
    python3.11 scripts/pipeline_printful_production.py --only 3   # run only step 3
"""

import json
import math
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont

# ── Path setup ───────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DESIGNS_DIR = PROJECT_DIR / "designs" / "sum26"
STATE_FILE = SCRIPT_DIR / "pipeline_sum26_production_state.json"

sys.path.insert(0, str(PROJECT_DIR))

from scripts.pipeline_printful_scale import (
    PRODUCTS, ALL_PRODUCTS, NICHE_CONTENT, BULLET_TEMPLATES,
    PRICE_TIERS, BROWSE_NODES, COLOR_TRANSLATIONS, META_FIELDS,
    HAT_FORM_TYPES, build_listing_attrs, ALL_MARKETS,
    DAD_HAT_VARIANTS, TRUCKER_VARIANTS, BUCKET_VARIANTS,
    PRINTFUL_TOKEN, PRINTFUL_STORE_ID, PRINTFUL_BASE,
    pf_get, pf_post,
)
from etl.amazon_listings import (
    MARKETPLACE_IDS, LANG_TAGS, CURRENCIES, SIZE_SYSTEMS,
    SELLER_ID, put_listing, check_listing_exists,
)


# ── State management ─────────────────────────────────────────────────────────

def load_state() -> dict:
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {
        "step1_designs": {},
        "step2_uploads": {},
        "step3_printful_products": {},
        "step4_amazon_listings": {},
        "errors": [],
    }


def save_state(state: dict) -> None:
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
        f.write("\n")
    print(f"  [STATE] saved to {STATE_FILE.name}")


# ── Printful upload headers (multipart — no Content-Type) ────────────────────

UPLOAD_HEADERS = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
}
if PRINTFUL_STORE_ID:
    UPLOAD_HEADERS["X-PF-Store-Id"] = PRINTFUL_STORE_ID

PRINTFUL_JSON_HEADERS = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
    "Content-Type": "application/json",
}
if PRINTFUL_STORE_ID:
    PRINTFUL_JSON_HEADERS["X-PF-Store-Id"] = PRINTFUL_STORE_ID


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Generate Embroidery Design PNGs
# ══════════════════════════════════════════════════════════════════════════════

# Design specs: key -> (main_text, draw_icon_function)
DESIGN_SPECS = {
    "mountain":  "EXPLORE",
    "coffee":    "COFFEE",
    "surf":      "SURF",
    "cycling":   "RIDE",
    "dog-dad":   "DOG DAD",
    "vinyl":     "ANALOG",
    "garden":    "GROW",
    "astro":     "COSMOS",
    "camping":   "WILD",
    "yoga":      "BREATHE",
}

SIZE = 4000
WHITE = (255, 255, 255, 255)
TRANSPARENT = (0, 0, 0, 0)


def _get_font(size: int) -> ImageFont.FreeTypeFont:
    """Get a bold font at requested size. Falls back to default."""
    # Try common bold fonts available on macOS
    candidates = [
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFCompact.ttf",
        "/Library/Fonts/Arial Bold.ttf",
        "/Library/Fonts/Arial.ttf",
        "/System/Library/Fonts/HelveticaNeue.ttc",
    ]
    for path in candidates:
        if os.path.exists(path):
            try:
                return ImageFont.truetype(path, size)
            except Exception:
                continue
    # Ultimate fallback — Pillow default (bitmap) scaled up won't look great,
    # but we try loading any TTF we can find
    try:
        return ImageFont.truetype("DejaVuSans-Bold.ttf", size)
    except Exception:
        return ImageFont.load_default()


def _draw_mountain(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple triangle mountain peak."""
    half_w = icon_h // 2
    pts = [(cx, top), (cx + half_w, top + icon_h), (cx - half_w, top + icon_h)]
    draw.polygon(pts, fill=WHITE)


def _draw_coffee(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple coffee cup outline: rectangle + handle arc."""
    cup_w = int(icon_h * 0.6)
    cup_h = int(icon_h * 0.8)
    lw = max(20, icon_h // 15)
    x0 = cx - cup_w // 2
    y0 = top + (icon_h - cup_h) // 2
    # cup body
    draw.rectangle([x0, y0, x0 + cup_w, y0 + cup_h], outline=WHITE, width=lw)
    # handle (arc on the right)
    handle_r = cup_h // 3
    draw.arc(
        [x0 + cup_w - lw // 2, y0 + cup_h // 4,
         x0 + cup_w + handle_r, y0 + cup_h * 3 // 4],
        start=-90, end=90, fill=WHITE, width=lw,
    )
    # steam lines
    for dx in (-cup_w // 4, 0, cup_w // 4):
        sx = cx + dx
        for i in range(3):
            sy = y0 - 30 - i * 40
            draw.line([(sx - 10, sy), (sx + 10, sy - 30)], fill=WHITE, width=lw // 2)


def _draw_wave(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple wave line."""
    lw = max(20, icon_h // 12)
    w = int(icon_h * 1.2)
    mid_y = top + icon_h // 2
    points = []
    for i in range(100):
        x = cx - w // 2 + int(i * w / 99)
        y = mid_y + int(icon_h * 0.3 * math.sin(i * 2 * math.pi / 50))
        points.append((x, y))
    draw.line(points, fill=WHITE, width=lw)


def _draw_wheel(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple circle (bicycle wheel)."""
    lw = max(20, icon_h // 12)
    r = icon_h // 2 - lw
    draw.ellipse([cx - r, top + lw, cx + r, top + icon_h - lw], outline=WHITE, width=lw)
    # hub dot
    dot_r = lw * 2
    draw.ellipse([cx - dot_r, top + icon_h // 2 - dot_r, cx + dot_r, top + icon_h // 2 + dot_r], fill=WHITE)


def _draw_paw(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple paw print — 1 big pad + 4 small toes."""
    pad_r = icon_h // 4
    pad_cy = top + icon_h - pad_r - 10
    draw.ellipse([cx - pad_r, pad_cy - pad_r, cx + pad_r, pad_cy + pad_r], fill=WHITE)
    # 4 toes
    toe_r = pad_r // 2
    offsets = [(-pad_r, -pad_r * 1.3), (-pad_r * 0.3, -pad_r * 1.8),
               (pad_r * 0.3, -pad_r * 1.8), (pad_r, -pad_r * 1.3)]
    for dx, dy in offsets:
        tx = cx + int(dx)
        ty = pad_cy + int(dy)
        draw.ellipse([tx - toe_r, ty - toe_r, tx + toe_r, ty + toe_r], fill=WHITE)


def _draw_record(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple vinyl record — circle with center hole."""
    lw = max(20, icon_h // 12)
    r = icon_h // 2 - lw
    draw.ellipse([cx - r, top + lw, cx + r, top + icon_h - lw], outline=WHITE, width=lw)
    # inner groove ring
    r2 = r // 2
    draw.ellipse([cx - r2, top + icon_h // 2 - r2, cx + r2, top + icon_h // 2 + r2], outline=WHITE, width=lw // 2)
    # center hole
    hole_r = lw * 2
    draw.ellipse([cx - hole_r, top + icon_h // 2 - hole_r, cx + hole_r, top + icon_h // 2 + hole_r], fill=WHITE)


def _draw_leaf(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple leaf shape."""
    lw = max(15, icon_h // 18)
    # Leaf outline (ellipse tilted — approximate with polygon)
    pts = [
        (cx, top),
        (cx + icon_h // 3, top + icon_h // 2),
        (cx, top + icon_h),
        (cx - icon_h // 3, top + icon_h // 2),
    ]
    draw.polygon(pts, outline=WHITE, width=lw)
    # center vein
    draw.line([(cx, top + 20), (cx, top + icon_h - 20)], fill=WHITE, width=lw)


def _draw_crescent(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Crescent moon + star dot."""
    r = icon_h // 2 - 20
    # Full moon circle
    draw.ellipse([cx - r, top + 20, cx + r, top + icon_h - 20], fill=WHITE)
    # Erase a portion to make crescent
    offset = int(r * 0.6)
    draw.ellipse([cx - r + offset, top, cx + r + offset, top + icon_h], fill=TRANSPARENT)
    # Star dot
    star_r = r // 5
    star_x = cx + r // 2
    star_y = top + icon_h // 4
    draw.ellipse([star_x - star_r, star_y - star_r, star_x + star_r, star_y + star_r], fill=WHITE)


def _draw_tent(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple tent (triangle) + star dots."""
    lw = max(15, icon_h // 15)
    tent_h = int(icon_h * 0.8)
    tent_w = int(icon_h * 0.7)
    tent_top = top + icon_h - tent_h
    pts = [(cx, tent_top), (cx + tent_w // 2, top + icon_h), (cx - tent_w // 2, top + icon_h)]
    draw.polygon(pts, outline=WHITE, width=lw)
    # door slit
    draw.line([(cx, tent_top + tent_h // 3), (cx, top + icon_h)], fill=WHITE, width=lw)
    # star dots above tent
    dot_r = lw
    for dx, dy in [(-tent_w // 3, -20), (tent_w // 3, -40), (0, -60)]:
        sx = cx + dx
        sy = tent_top + dy
        draw.ellipse([sx - dot_r, sy - dot_r, sx + dot_r, sy + dot_r], fill=WHITE)


def _draw_lotus(draw: ImageDraw.ImageDraw, cx: int, top: int, icon_h: int):
    """Simple lotus petal shapes."""
    lw = max(15, icon_h // 18)
    petal_h = int(icon_h * 0.6)
    petal_w = int(icon_h * 0.2)
    base_y = top + icon_h

    # Center petal
    pts_c = [(cx, base_y - petal_h), (cx + petal_w // 2, base_y), (cx - petal_w // 2, base_y)]
    draw.polygon(pts_c, outline=WHITE, width=lw)

    # Left petal
    offset = petal_w + 10
    pts_l = [(cx - offset, base_y - int(petal_h * 0.7)),
             (cx - offset + petal_w // 2, base_y),
             (cx - offset - petal_w // 2, base_y)]
    draw.polygon(pts_l, outline=WHITE, width=lw)

    # Right petal
    pts_r = [(cx + offset, base_y - int(petal_h * 0.7)),
             (cx + offset + petal_w // 2, base_y),
             (cx + offset - petal_w // 2, base_y)]
    draw.polygon(pts_r, outline=WHITE, width=lw)

    # Far-left petal (smaller)
    offset2 = offset * 2
    pts_fl = [(cx - offset2, base_y - int(petal_h * 0.45)),
              (cx - offset2 + petal_w // 3, base_y),
              (cx - offset2 - petal_w // 3, base_y)]
    draw.polygon(pts_fl, outline=WHITE, width=lw)

    # Far-right petal (smaller)
    pts_fr = [(cx + offset2, base_y - int(petal_h * 0.45)),
              (cx + offset2 + petal_w // 3, base_y),
              (cx + offset2 - petal_w // 3, base_y)]
    draw.polygon(pts_fr, outline=WHITE, width=lw)


ICON_DRAWERS = {
    "mountain": _draw_mountain,
    "coffee":   _draw_coffee,
    "surf":     _draw_wave,
    "cycling":  _draw_wheel,
    "dog-dad":  _draw_paw,
    "vinyl":    _draw_record,
    "garden":   _draw_leaf,
    "astro":    _draw_crescent,
    "camping":  _draw_tent,
    "yoga":     _draw_lotus,
}


def generate_design(key: str, text: str) -> Path:
    """Generate a single embroidery-ready PNG design."""
    img = Image.new("RGBA", (SIZE, SIZE), TRANSPARENT)
    draw = ImageDraw.Draw(img)

    # Layout: icon in upper portion, text below
    icon_h = 1200
    icon_top = 800
    text_top = icon_top + icon_h + 200
    cx = SIZE // 2

    # Draw icon
    drawer = ICON_DRAWERS.get(key)
    if drawer:
        drawer(draw, cx, icon_top, icon_h)

    # Draw text
    font = _get_font(400)
    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    tx = (SIZE - tw) // 2
    ty = text_top
    draw.text((tx, ty), text, fill=WHITE, font=font)

    # Save
    DESIGNS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DESIGNS_DIR / f"{key}.png"
    img.save(out_path, "PNG")
    return out_path


def step1_generate_designs(state: dict) -> dict:
    """Generate all 10 design PNGs."""
    print("\n" + "=" * 70)
    print("STEP 1: Generate Embroidery Design PNGs")
    print("=" * 70)

    results = {"generated": 0, "skipped": 0}

    for key, text in DESIGN_SPECS.items():
        out_path = DESIGNS_DIR / f"{key}.png"

        # Skip if already generated and recorded in state
        if key in state.get("step1_designs", {}) and out_path.exists():
            print(f"  [SKIP] {key} — already exists at {out_path}")
            results["skipped"] += 1
            continue

        print(f"  Generating {key}.png — text='{text}'...")
        path = generate_design(key, text)
        file_size = path.stat().st_size

        state.setdefault("step1_designs", {})[key] = {
            "path": str(path),
            "text": text,
            "size_bytes": file_size,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        print(f"  [OK] {key}.png — {file_size:,} bytes")
        results["generated"] += 1

    save_state(state)
    print(f"\n  Summary: {results['generated']} generated, {results['skipped']} skipped")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: Upload Designs to Printful File Library
# ══════════════════════════════════════════════════════════════════════════════

def wait_for_file_processing(file_id: int, max_wait: int = 120) -> dict:
    """Poll Printful until file status is 'ok'."""
    for _ in range(max_wait // 5):
        time.sleep(5)
        try:
            resp = requests.get(
                f"{PRINTFUL_BASE}/files/{file_id}",
                headers=PRINTFUL_JSON_HEADERS,
            )
            resp.raise_for_status()
            info = resp.json().get("result", {})
            status = info.get("status", "unknown")
            if status == "ok":
                return info
            elif status == "failed":
                print(f"    [WARN] File {file_id} processing failed")
                return info
        except Exception as e:
            print(f"    [WARN] status check error: {e}")
    return {}


def step2_upload_designs(state: dict) -> dict:
    """Upload design PNGs to Printful file library."""
    print("\n" + "=" * 70)
    print("STEP 2: Upload Designs to Printful File Library")
    print("=" * 70)

    results = {"uploaded": 0, "skipped": 0, "errors": 0}

    for key in ALL_PRODUCTS:
        design_file = DESIGNS_DIR / f"{key}.png"

        # Skip if already uploaded
        if key in state.get("step2_uploads", {}) and state["step2_uploads"][key].get("file_id"):
            fid = state["step2_uploads"][key]["file_id"]
            print(f"  [SKIP] {key} — already uploaded (file_id={fid})")
            results["skipped"] += 1
            continue

        if not design_file.exists():
            print(f"  [ERROR] {key} — design file not found at {design_file}")
            results["errors"] += 1
            continue

        print(f"  Uploading {key}.png to Printful (via tmpfiles.org)...")
        try:
            # Step A: Upload to tmpfiles.org to get a public URL
            result = subprocess.run(
                ["curl", "-s", "-F", f"file=@{design_file}", "https://tmpfiles.org/api/v1/upload"],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode != 0:
                raise RuntimeError(f"tmpfiles.org upload failed: {result.stderr}")
            tmp_resp = json.loads(result.stdout)
            if tmp_resp.get("status") != "success":
                raise RuntimeError(f"tmpfiles.org error: {result.stdout[:200]}")

            # Convert tmpfiles URL to direct download URL
            tmp_url = tmp_resp["data"]["url"]  # http://tmpfiles.org/XXXX/file.png
            direct_url = tmp_url.replace("tmpfiles.org/", "tmpfiles.org/dl/")
            if not direct_url.startswith("https"):
                direct_url = direct_url.replace("http://", "https://")
            print(f"    hosted at: {direct_url}")

            # Step B: Register with Printful via JSON URL upload
            time.sleep(1)
            resp = requests.post(
                f"{PRINTFUL_BASE}/files",
                headers=PRINTFUL_JSON_HEADERS,
                json={"url": direct_url},
            )
            resp.raise_for_status()
            data = resp.json()
            file_info = data.get("result", {})
            file_id = file_info["id"]

            print(f"    file_id={file_id}, status={file_info.get('status')} — waiting for processing...")

            # Wait for processing
            if file_info.get("status") != "ok":
                processed = wait_for_file_processing(file_id)
                if processed:
                    file_info = processed

            state.setdefault("step2_uploads", {})[key] = {
                "file_id": file_id,
                "preview_url": file_info.get("preview_url") or file_info.get("url", ""),
                "thumbnail_url": file_info.get("thumbnail_url", ""),
                "status": file_info.get("status", "unknown"),
                "hosted_url": direct_url,
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state)

            print(f"  [OK] {key} — file_id={file_id}, status={file_info.get('status')}")
            results["uploaded"] += 1
            time.sleep(1.5)  # rate limit

        except Exception as e:
            print(f"  [ERROR] {key} — {e}")
            state.setdefault("errors", []).append({
                "step": 2, "key": key, "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            save_state(state)
            results["errors"] += 1
            time.sleep(1)

    save_state(state)
    print(f"\n  Summary: {results['uploaded']} uploaded, {results['skipped']} skipped, {results['errors']} errors")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: Create 10 Printful Sync Products
# ══════════════════════════════════════════════════════════════════════════════

HAT_TYPE_NAMES = {
    "dad_hat": "Dad Hat",
    "trucker": "Trucker Cap",
    "bucket": "Bucket Hat",
}

RETAIL_PRICES = {
    "dad_hat": "24.99",
    "trucker": "27.99",
    "bucket": "24.99",
}

# ── Correct Printful Catalog IDs ────────────────────────────────────────────
# Pipeline used wrong IDs (145=sweatshirt, 381/379=wrong or no EU)
# Real embroidery hat catalog IDs:
CORRECT_CATALOG_IDS = {
    "dad_hat": 206,    # Yupoong 6245CM Classic Dad Hat (EMBROIDERY)
    "trucker": 252,    # Yupoong 6606 Retro Trucker Hat (EMBROIDERY)
    "bucket":  253,    # Flexfit 5003 Bucket Hat (EMBROIDERY, EU available)
}

# Correct variant IDs for Trucker 252 (EU available)
CORRECT_TRUCKER_VARIANTS = {
    "BLACK":           8747,
    "BLACK-WHITE":     8748,
    "CHARCOAL-BLACK":  16709,
    "HEATHER-WHITE":   22454,
    "NAVY":            8751,
    "NAVY-WHITE":      8755,
    "WHITE":           8746,
    "KHAKI":           8752,
}

# Correct variant IDs for Bucket 253 / Flexfit 5003 (EU)
CORRECT_BUCKET_VARIANTS = {
    "BLACK": 8760,
    "GREY":  8763,
    "NAVY":  8761,
    "WHITE": 8759,
    "KHAKI": 8762,
}

# Map pipeline color keys to correct variant IDs per hat type
# Some pipeline colors don't match catalog exactly
_TRUCKER_COLOR_MAP = {
    "CHARCOAL": "CHARCOAL-BLACK",  # Trucker 252 has Charcoal/Black, not plain Charcoal
}

def _get_correct_variant_id(hat_type: str, color_key: str) -> int | None:
    """Get correct Printful variant ID for a hat type + color."""
    if hat_type == "dad_hat":
        # Dad hat 206 uses same IDs as pipeline's DAD_HAT_VARIANTS
        return DAD_HAT_VARIANTS.get(color_key)
    elif hat_type == "trucker":
        mapped = _TRUCKER_COLOR_MAP.get(color_key, color_key)
        return CORRECT_TRUCKER_VARIANTS.get(mapped)
    elif hat_type == "bucket":
        return CORRECT_BUCKET_VARIANTS.get(color_key)
    return None

# Embroidery options per hat type
# Dad hats & truckers use embroidery_front_large, bucket uses embroidery_front
def _get_embroidery_options(hat_type: str) -> list[dict]:
    if hat_type == "bucket":
        return [
            {"id": "embroidery_type", "value": "flat"},
            {"id": "thread_colors_front", "value": ["#FFFFFF"]},
            {"id": "text_thread_colors_front", "value": []},
        ]
    return [
        {"id": "embroidery_type", "value": "flat"},
        {"id": "thread_colors_front_large", "value": ["#FFFFFF"]},
        {"id": "text_thread_colors_front_large", "value": []},
    ]

def _get_embroidery_file_type(hat_type: str) -> str:
    if hat_type == "bucket":
        return "embroidery_front"
    return "embroidery_front_large"


def _check_product_exists(product_name: str) -> dict | None:
    """Check if a Printful sync product with this name already exists."""
    try:
        resp = pf_get("/store/products", params={"limit": 100})
        for p in resp.get("result", []):
            if p.get("name") == product_name:
                return p
    except Exception as e:
        print(f"    [WARN] could not check existing products: {e}")
    return None


def step3_create_printful_products(state: dict) -> dict:
    """Create 10 Printful sync products with embroidery designs."""
    print("\n" + "=" * 70)
    print("STEP 3: Create Printful Sync Products")
    print("=" * 70)

    results = {"created": 0, "skipped": 0, "errors": 0}

    for key in ALL_PRODUCTS:
        product = PRODUCTS[key]
        hat_type = product["hat_type"]
        hat_name = HAT_TYPE_NAMES[hat_type]
        niche_title = product["niche"].split(" / ")[0]
        product_name = f"SUM26 {niche_title} Hat — {hat_name}"
        retail_price = RETAIL_PRICES[hat_type]

        # Skip if already created
        if key in state.get("step3_printful_products", {}) and \
           state["step3_printful_products"][key].get("sync_product_id"):
            spid = state["step3_printful_products"][key]["sync_product_id"]
            print(f"  [SKIP] {key} — already created (sync_product_id={spid})")
            results["skipped"] += 1
            continue

        # Check if design was uploaded
        upload_info = state.get("step2_uploads", {}).get(key)
        if not upload_info or not upload_info.get("file_id"):
            print(f"  [ERROR] {key} — no file_id found. Run step 2 first.")
            results["errors"] += 1
            continue

        design_file_id = upload_info["file_id"]
        design_preview = upload_info.get("preview_url", "")

        # Check if product already exists on Printful
        existing = _check_product_exists(product_name)
        if existing:
            sync_id = existing.get("id")
            print(f"  [SKIP] {key} — product already exists: '{product_name}' (id={sync_id})")
            state.setdefault("step3_printful_products", {})[key] = {
                "sync_product_id": sync_id,
                "name": product_name,
                "skipped_existing": True,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state)
            results["skipped"] += 1
            time.sleep(1)
            continue

        # Build sync variants with CORRECT catalog/variant IDs
        colors = product["colors"]
        correct_catalog_id = CORRECT_CATALOG_IDS[hat_type]
        sync_variants = []

        for color_key in colors:
            variant_id = _get_correct_variant_id(hat_type, color_key)
            if variant_id is None:
                print(f"    [WARN] No variant found for {hat_type}/{color_key} — skipping")
                continue
            child_sku = f"PFT-SUM26-{key.upper()}-{color_key}"
            sync_variants.append({
                "variant_id": variant_id,
                "retail_price": retail_price,
                "sku": child_sku,
                "files": [
                    {
                        "type": _get_embroidery_file_type(hat_type),
                        "id": design_file_id,
                    }
                ],
                "options": _get_embroidery_options(hat_type),
            })

        # Use design preview_url as thumbnail, fallback to hosted URL
        thumbnail = design_preview or upload_info.get("hosted_url", "")
        body = {
            "sync_product": {
                "name": product_name,
                "thumbnail": thumbnail,
            },
            "sync_variants": sync_variants,
        }

        print(f"  Creating: {product_name}")
        print(f"    hat_type={hat_type}, catalog_id={product['catalog_id']}, colors={colors}")
        print(f"    design_file_id={design_file_id}, variants={len(sync_variants)}")

        try:
            resp = pf_post("/store/products", body)
            result = resp.get("result", {})
            sync_product = result.get("sync_product", {})
            sync_product_id = sync_product.get("id")
            created_variants = result.get("sync_variants", [])

            state.setdefault("step3_printful_products", {})[key] = {
                "sync_product_id": sync_product_id,
                "name": product_name,
                "hat_type": hat_type,
                "variant_count": len(created_variants),
                "variant_ids": [v.get("id") for v in created_variants],
                "skus": [v.get("sku") for v in created_variants],
                "design_file_id": design_file_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state)

            print(f"  [OK] {key} — sync_product_id={sync_product_id}, {len(created_variants)} variants")
            results["created"] += 1
            time.sleep(2)  # rate limit

        except requests.exceptions.HTTPError as e:
            error_body = ""
            try:
                error_body = e.response.text[:500]
            except Exception:
                pass
            print(f"  [ERROR] {key} — HTTP {e.response.status_code}: {error_body}")
            state.setdefault("errors", []).append({
                "step": 3, "key": key, "error": str(e), "response": error_body,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            save_state(state)
            results["errors"] += 1
            time.sleep(2)

        except Exception as e:
            print(f"  [ERROR] {key} — {e}")
            state.setdefault("errors", []).append({
                "step": 3, "key": key, "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat(),
            })
            save_state(state)
            results["errors"] += 1
            time.sleep(2)

    save_state(state)
    print(f"\n  Summary: {results['created']} created, {results['skipped']} skipped, {results['errors']} errors")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 4: Create Amazon Listings on DE + PL
# ══════════════════════════════════════════════════════════════════════════════

TARGET_MARKETS = ["DE", "PL"]


def step4_create_amazon_listings(state: dict) -> dict:
    """Create parent + child Amazon listings on DE and PL."""
    print("\n" + "=" * 70)
    print("STEP 4: Create Amazon Listings (DE + PL)")
    print("=" * 70)

    results = {"created": 0, "skipped": 0, "errors": 0}
    listings_state = state.setdefault("step4_amazon_listings", {})

    for key in ALL_PRODUCTS:
        product = PRODUCTS[key]
        parent_sku = f"PFT-SUM26-{key.upper()}"
        colors = product["colors"]

        print(f"\n  ── Product: {key.upper()} | SKU: {parent_sku} ──")
        print(f"     Colors: {colors} | Hat: {product['hat_type']}")

        for mkt_code in TARGET_MARKETS:
            mkt_id = MARKETPLACE_IDS[mkt_code]
            listing_key = f"{parent_sku}_{mkt_code}"

            print(f"\n    Market: {mkt_code}")

            # ── Parent ──
            parent_state_key = f"{parent_sku}_{mkt_code}_parent"
            if parent_state_key in listings_state and listings_state[parent_state_key].get("status") == "ok":
                print(f"      [SKIP] Parent {parent_sku} — already created on {mkt_code}")
                results["skipped"] += 1
            else:
                # Check Amazon
                if check_listing_exists(parent_sku, mkt_id):
                    print(f"      [SKIP] Parent {parent_sku} — already exists on {mkt_code}")
                    listings_state[parent_state_key] = {"status": "ok", "skipped_existing": True}
                    results["skipped"] += 1
                else:
                    attrs = build_listing_attrs(key, mkt_code, None, parent_sku, is_parent=True)
                    title = attrs["item_name"][0]["value"]
                    print(f"      [PARENT] {parent_sku} — \"{title[:80]}\"")
                    assert len(title) <= 200, f"Title too long ({len(title)} chars)"

                    status_code, resp = put_listing(parent_sku, mkt_id, attrs, dry_run=False)
                    if status_code in (200, 202):
                        listings_state[parent_state_key] = {
                            "status": "ok", "http": status_code,
                            "created_at": datetime.now(timezone.utc).isoformat(),
                        }
                        results["created"] += 1
                    else:
                        listings_state[parent_state_key] = {
                            "status": "error", "http": status_code,
                            "response": str(resp)[:300],
                        }
                        results["errors"] += 1
                    time.sleep(1.5)

            # ── Children ──
            for color_key in colors:
                child_sku = f"{parent_sku}-{color_key}"
                child_state_key = f"{child_sku}_{mkt_code}"

                if child_state_key in listings_state and listings_state[child_state_key].get("status") == "ok":
                    print(f"      [SKIP] Child {child_sku} — already created on {mkt_code}")
                    results["skipped"] += 1
                    continue

                if check_listing_exists(child_sku, mkt_id):
                    print(f"      [SKIP] Child {child_sku} — already exists on {mkt_code}")
                    listings_state[child_state_key] = {"status": "ok", "skipped_existing": True}
                    results["skipped"] += 1
                    continue

                attrs = build_listing_attrs(key, mkt_code, color_key, parent_sku, is_parent=False)
                title = attrs["item_name"][0]["value"]
                color_val = attrs["color"][0]["value"]
                print(f"      [CHILD] {child_sku} [{color_val}]")
                assert len(title) <= 200, f"Title too long ({len(title)} chars)"

                status_code, resp = put_listing(child_sku, mkt_id, attrs, dry_run=False)
                if status_code in (200, 202):
                    listings_state[child_state_key] = {
                        "status": "ok", "http": status_code,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    }
                    results["created"] += 1
                else:
                    listings_state[child_state_key] = {
                        "status": "error", "http": status_code,
                        "response": str(resp)[:300],
                    }
                    results["errors"] += 1
                time.sleep(1)

            save_state(state)

    save_state(state)
    print(f"\n  Summary: {results['created']} created, {results['skipped']} skipped, {results['errors']} errors")
    return results


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def print_final_summary(state: dict):
    """Print a clean summary of everything done."""
    print("\n" + "=" * 70)
    print("FINAL SUMMARY")
    print("=" * 70)

    # Printful products
    print("\n  Printful Sync Products:")
    for key in ALL_PRODUCTS:
        info = state.get("step3_printful_products", {}).get(key, {})
        spid = info.get("sync_product_id", "N/A")
        name = info.get("name", "not created")
        vc = info.get("variant_count", 0)
        print(f"    {key:12s} — sync_product_id={spid}, variants={vc}, name='{name}'")

    # Amazon listings
    print("\n  Amazon Listings:")
    listings = state.get("step4_amazon_listings", {})
    ok_count = sum(1 for v in listings.values() if v.get("status") == "ok")
    err_count = sum(1 for v in listings.values() if v.get("status") == "error")
    print(f"    Total entries: {len(listings)} (ok={ok_count}, errors={err_count})")

    for key in ALL_PRODUCTS:
        parent_sku = f"PFT-SUM26-{key.upper()}"
        for mkt in TARGET_MARKETS:
            parent_status = listings.get(f"{parent_sku}_{mkt}_parent", {}).get("status", "?")
            child_statuses = []
            for color in PRODUCTS[key]["colors"]:
                cs = listings.get(f"{parent_sku}-{color}_{mkt}", {}).get("status", "?")
                child_statuses.append(cs)
            children_ok = sum(1 for s in child_statuses if s == "ok")
            children_total = len(child_statuses)
            print(f"    {key:12s} {mkt}: parent={parent_status}, children={children_ok}/{children_total} ok")

    # Errors
    errors = state.get("errors", [])
    if errors:
        print(f"\n  Errors ({len(errors)}):")
        for e in errors[-10:]:
            print(f"    step={e.get('step')} key={e.get('key')} — {e.get('error', '')[:100]}")

    print("\n" + "=" * 70)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="SUM26 Production Pipeline")
    parser.add_argument("--step", type=int, default=1,
                        help="Start from this step (1-4, default: 1)")
    parser.add_argument("--only", type=int, default=0,
                        help="Run only this step (1-4)")
    args = parser.parse_args()

    start_step = args.step
    only_step = args.only

    print("=" * 70)
    print("SUM26 PRODUCTION PIPELINE")
    print(f"Started at: {datetime.now(timezone.utc).isoformat()}")
    print(f"Store ID: {PRINTFUL_STORE_ID}")
    print(f"Designs dir: {DESIGNS_DIR}")
    print(f"State file: {STATE_FILE}")
    if only_step:
        print(f"Running ONLY step {only_step}")
    else:
        print(f"Running from step {start_step}")
    print("=" * 70)

    state = load_state()

    steps = {
        1: ("Generate Designs", step1_generate_designs),
        2: ("Upload to Printful", step2_upload_designs),
        3: ("Create Printful Products", step3_create_printful_products),
        4: ("Create Amazon Listings (DE+PL)", step4_create_amazon_listings),
    }

    for step_num, (step_name, step_fn) in steps.items():
        if only_step and step_num != only_step:
            continue
        if not only_step and step_num < start_step:
            continue

        print(f"\n>>> Running Step {step_num}: {step_name}")
        step_fn(state)

    print_final_summary(state)
    print(f"\nCompleted at: {datetime.now(timezone.utc).isoformat()}")
    print(f"State saved to: {STATE_FILE}")


if __name__ == "__main__":
    main()
