#!/usr/bin/env python3.11
"""
Amazon SEO — Optimize titles & bullet points for PFT- (Printful) listings.

Fetches live listing data from SP-API (DE marketplace as primary),
analyzes current titles/bullets, generates keyword-rich optimized versions,
and saves a markdown report.

Rules:
- No "nesell" in titles
- No manufacturer model numbers (Yupoong 6245CM, etc.)
- Titles max 200 chars, keyword-rich
- 5 bullet points minimum
- Consider all 8 EU marketplaces

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/seo_optimize_pft.py
"""

import json
import sys
import time
import re
from collections import defaultdict
from datetime import date
from pathlib import Path

sys.path.insert(0, "/Users/alexanderrogalski/nesell-analytics")

from etl.amazon_api import api_get, headers
from etl import config

SELLER_ID = config.AMZ_SELLER_ID or "A1IZH6PW7A624A"
MARKETPLACES = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

# Manufacturer model numbers to strip from titles
MODEL_PATTERNS = [
    r'\bYupoong\s+\d{4}[A-Z]+\b',
    r'\bFlexfit\s+\d{4}[A-Z]+\b',
    r'\bOtto\s+\d{2,3}-\d{4}\b',
    r'\bBig\s+Accessories\s+BX\d{3}\b',
    r'\bBeechfield\s+B\d{3}\b',
    r'\bCapstone\s+\w+-\d+\b',
    r'\bRichardson\s+\d{3}\b',
    r'\b[A-Z]{2}\d{4}[A-Z]{0,2}\b',
]

MODEL_REGEX = re.compile("|".join(MODEL_PATTERNS), re.IGNORECASE)


def fetch_pft_listings(marketplace_id: str, limit: int = 200) -> list[dict]:
    """Fetch all PFT- listings from one marketplace with full attributes."""
    listings = []
    page_token = None

    while len(listings) < limit:
        params = {
            "marketplaceIds": marketplace_id,
            "includedData": "summaries,attributes,issues",
            "pageSize": 20,
        }
        if page_token:
            params["pageToken"] = page_token

        data = api_get(f"/listings/2021-08-01/items/{SELLER_ID}", params=params)
        items = data.get("items", [])

        for item in items:
            if item.get("sku", "").startswith("PFT-"):
                listings.append(item)

        next_token = data.get("pagination", {}).get("nextToken")
        if not next_token:
            break

        page_token = next_token
        time.sleep(0.3)

    return listings


def extract_listing_content(item: dict) -> dict:
    """Extract title, bullets, keywords, ASIN from a listing item."""
    attrs = item.get("attributes", {})
    summaries = item.get("summaries", [{}])
    summary = summaries[0] if summaries else {}

    title_list = attrs.get("item_name", [])
    title = title_list[0].get("value", "") if title_list else ""

    bullets = [b.get("value", "") for b in attrs.get("bullet_point", [])]
    keywords = [k.get("value", "") for k in attrs.get("generic_keyword", [])]

    brand_list = attrs.get("brand", [])
    brand = brand_list[0].get("value", "") if brand_list else ""

    desc_list = attrs.get("product_description", [])
    desc = desc_list[0].get("value", "") if desc_list else ""

    color_list = attrs.get("color", [])
    color = color_list[0].get("value", "") if color_list else ""

    return {
        "sku": item.get("sku", ""),
        "asin": summary.get("asin", ""),
        "title": title,
        "bullets": bullets,
        "keywords": keywords,
        "brand": brand,
        "description": desc,
        "color": color,
        "status": summary.get("status", []),
        "issues": item.get("issues", []),
    }


def clean_title(title: str) -> str:
    """Remove model numbers and brand prefix from title."""
    cleaned = MODEL_REGEX.sub("", title)
    # Remove "nesell" brand prefix (case insensitive)
    cleaned = re.sub(r'^nesell\s*[-–—|]?\s*', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'\bnesell\b', '', cleaned, flags=re.IGNORECASE)
    # Clean up extra spaces and dashes
    cleaned = re.sub(r'\s{2,}', ' ', cleaned)
    cleaned = re.sub(r'^[-–—|,\s]+|[-–—|,\s]+$', '', cleaned)
    return cleaned.strip()


def has_model_number(title: str) -> bool:
    return bool(MODEL_REGEX.search(title))


def has_nesell(title: str) -> bool:
    return bool(re.search(r'\bnesell\b', title, re.IGNORECASE))


def classify_product_type(sku: str, title: str) -> str:
    """Classify hat type from SKU or title for keyword selection."""
    title_lower = title.lower()
    sku_lower = sku.lower()

    if "bucket" in title_lower:
        return "bucket_hat"
    elif "trucker" in title_lower or "mesh" in title_lower:
        return "trucker_cap"
    elif "beanie" in title_lower or "knit" in title_lower:
        return "beanie"
    elif "snapback" in title_lower:
        return "snapback"
    elif "corduroy" in title_lower:
        return "corduroy_cap"
    elif "dad" in title_lower:
        return "dad_hat"
    elif "flexfit" in title_lower or "fitted" in title_lower:
        return "flexfit_cap"
    else:
        return "baseball_cap"


# ── SEO Optimization Templates ────────────────────────────────────────────────

KEYWORD_TEMPLATES = {
    "dad_hat": {
        "title_keywords": ["Dad Hat", "Low Profile Cap", "Adjustable Baseball Cap", "Cotton Cap", "Washed Cap"],
        "bullets": [
            "CLASSIC DAD HAT STYLE — Low-profile, unstructured design with curved brim for a relaxed, everyday look loved by men and women",
            "PREMIUM COTTON CONSTRUCTION — Soft 100% cotton or cotton-blend fabric for all-day comfort, lightweight at just ~80g",
            "ADJUSTABLE FIT — Metal buckle or brass clasp strap fits head sizes 55–60 cm, one size fits most adults",
            "CUSTOM DESIGN — Unique embroidered or printed graphic, great as a gift or personal statement piece",
            "VERSATILE WEAR — Perfect for casual outings, travel, beach, festivals, and everyday streetwear",
        ],
        "keywords": "dad hat low profile cap adjustable baseball cap cotton cap washed cap unstructured hat gift for him",
    },
    "trucker_cap": {
        "title_keywords": ["Trucker Cap", "Mesh Baseball Cap", "Snapback Hat", "Adjustable Cap", "Summer Cap"],
        "bullets": [
            "TRUCKER MESH DESIGN — Breathable mesh back panels keep you cool in warm weather while the foam front holds its shape",
            "SNAPBACK CLOSURE — Plastic snap adjuster accommodates head sizes 55–62 cm, easy to size up or down",
            "FOAM FRONT PANEL — Pre-curved brim, mid-profile construction for a clean, retro street look",
            "CUSTOM GRAPHIC — Eye-catching print or embroidery on the front panel, ideal as a personal accessory or gift",
            "DURABLE EVERYDAY USE — Machine-washable construction, suitable for outdoor activities, travel, and daily use",
        ],
        "keywords": "trucker cap mesh cap snapback hat baseball cap summer hat adjustable unisex gift",
    },
    "bucket_hat": {
        "title_keywords": ["Bucket Hat", "Sun Hat", "Fisherman Hat", "Festival Hat", "Unisex Hat"],
        "bullets": [
            "BUCKET HAT STYLE — Wide all-around brim provides sun protection UPF 30+ for outdoor activities in summer",
            "SOFT FLEXIBLE FABRIC — Lightweight cotton or canvas construction, packs flat for travel and festivals",
            "UNISEX DESIGN — Available in sizes S/M and L/XL, suitable for men and women (head circumference 56–61 cm)",
            "UNIQUE CUSTOM PRINT — Distinctive graphic design makes it a standout piece and a perfect gift idea",
            "VERSATILE & PACKABLE — Great for beach trips, hiking, festivals, gardening, and everyday casual wear",
        ],
        "keywords": "bucket hat sun hat fisherman hat festival hat packable unisex cotton summer hat custom",
    },
    "snapback": {
        "title_keywords": ["Snapback Cap", "Baseball Cap", "Flat Brim Cap", "Adjustable Cap", "Hip Hop Cap"],
        "bullets": [
            "FLAT BRIM SNAPBACK — Structured 6-panel design with flat brim for a clean, modern streetwear aesthetic",
            "SNAPBACK CLOSURE — Universal snap closure fits head sizes 55–63 cm, one size fits most",
            "PREMIUM MATERIALS — Durable polyester/wool blend construction, maintains shape through regular use",
            "CUSTOM DESIGN — Bold graphic or embroidery makes every piece unique and wearable as a statement accessory",
            "STREET STYLE ESSENTIAL — Pairs effortlessly with casual, sporty, and urban outfits for men and women",
        ],
        "keywords": "snapback cap flat brim baseball cap hip hop cap adjustable streetwear custom graphic",
    },
    "corduroy_cap": {
        "title_keywords": ["Corduroy Cap", "Dad Hat", "Cotton Cap", "Vintage Cap", "Adjustable Baseball Cap"],
        "bullets": [
            "PREMIUM CORDUROY TEXTURE — Fine-ribbed corduroy fabric gives a vintage, retro aesthetic that stands out",
            "UNSTRUCTURED LOW PROFILE — Soft, relaxed fit with curved brim for comfortable all-day wear",
            "ADJUSTABLE BRASS BUCKLE — Classic metal strap adjustment fits head sizes 55–61 cm",
            "UNIQUE CUSTOM EMBROIDERY — Subtle or bold embroidered design on quality corduroy base, ideal as a gift",
            "VERSATILE SEASONAL WEAR — Stylish in autumn and winter, pairs with casual or smart-casual looks",
        ],
        "keywords": "corduroy cap vintage dad hat adjustable cotton cap retro baseball cap custom embroidery gift",
    },
    "flexfit_cap": {
        "title_keywords": ["Fitted Cap", "Baseball Cap", "Stretch Fit Cap", "Sports Cap", "Premium Cap"],
        "bullets": [
            "STRETCH-FIT COMFORT — Spandex-infused sweatband and fitted construction provides a snug, no-adjust fit",
            "STRUCTURED MID-PROFILE — 6-panel construction with slightly curved brim for a polished, athletic look",
            "MOISTURE-WICKING — Sweat-absorbing inner band keeps you cool during sports and active use",
            "CUSTOM DESIGN — High-quality embroidery or printing on fitted cap blank for a premium look",
            "AVAILABLE IN SIZES — S/M and L/XL sizing to accommodate head circumferences 54–63 cm",
        ],
        "keywords": "fitted cap baseball cap stretch fit sports cap premium hat custom embroidery active wear",
    },
    "beanie": {
        "title_keywords": ["Beanie Hat", "Knit Hat", "Winter Hat", "Skull Cap", "Slouchy Beanie"],
        "bullets": [
            "WARM WINTER BEANIE — Soft knit construction with ribbed texture keeps head and ears warm in cold weather",
            "STRETCHY COMFORTABLE FIT — Acrylic or wool-blend knit stretches to fit most head sizes 55–62 cm",
            "CLASSIC CUFFED DESIGN — Turn-up cuff can be worn straight or slouchy for flexible styling",
            "CUSTOM PATCH OR EMBROIDERY — Unique design detail elevates plain beanie to a personalized accessory",
            "UNISEX EVERYDAY ESSENTIAL — Great for skiing, hiking, commuting, and daily winter wear for men and women",
        ],
        "keywords": "beanie hat winter hat knit hat skull cap slouchy beanie custom unisex warm gift",
    },
    "baseball_cap": {
        "title_keywords": ["Baseball Cap", "Adjustable Cap", "Cotton Cap", "Casual Cap", "Unisex Hat"],
        "bullets": [
            "CLASSIC BASEBALL CAP — 6-panel structured design with pre-curved brim for a timeless, versatile look",
            "QUALITY CONSTRUCTION — Cotton or cotton-blend fabric, durable stitching for long-lasting everyday use",
            "ADJUSTABLE CLOSURE — Hook-and-loop or buckle strap accommodates head sizes 55–62 cm",
            "CUSTOM GRAPHIC DESIGN — Unique embroidery or print makes this cap a great personal accessory or gift",
            "EVERYDAY VERSATILITY — Perfect for sports, travel, casual outings, and outdoor activities year-round",
        ],
        "keywords": "baseball cap adjustable cap cotton cap casual hat unisex gift custom graphic outdoor",
    },
}


def generate_optimized_title(current_title: str, product_type: str, color: str = "") -> dict:
    """Generate an optimized title and flag issues in the current one."""
    issues = []
    suggestions = []

    if has_nesell(current_title):
        issues.append("Contains brand name 'nesell'")
        suggestions.append("Remove 'nesell' prefix from title")

    if has_model_number(current_title):
        issues.append("Contains manufacturer model number")
        suggestions.append("Remove model number (e.g., 6245CM, YP-M, etc.)")

    if len(current_title) > 200:
        issues.append(f"Title too long: {len(current_title)} chars (max 200)")

    if len(current_title) < 80:
        issues.append(f"Title too short: {len(current_title)} chars (min 80 for SEO)")
        suggestions.append("Add more descriptive keywords")

    template = KEYWORD_TEMPLATES.get(product_type, KEYWORD_TEMPLATES["baseball_cap"])
    kw_list = template["title_keywords"]

    # Build optimized base from cleaned title
    base = clean_title(current_title)

    # Ensure at least one product-type keyword is in the title
    has_type_kw = any(kw.lower() in base.lower() for kw in kw_list[:3])
    if not has_type_kw and kw_list:
        # Prepend the primary keyword
        primary = kw_list[0]
        base = f"{primary} — {base}" if base else primary

    # Add color if not already present
    if color and color.lower() not in base.lower() and len(base) < 150:
        base = f"{base} | {color}"

    # Ensure under 200 chars
    if len(base) > 200:
        base = base[:197] + "..."

    return {
        "current_issues": issues,
        "suggestions": suggestions,
        "optimized_title": base,
        "char_count": len(base),
    }


def generate_optimized_bullets(current_bullets: list[str], product_type: str) -> dict:
    """Generate optimized bullet points."""
    issues = []
    template = KEYWORD_TEMPLATES.get(product_type, KEYWORD_TEMPLATES["baseball_cap"])
    recommended = template["bullets"]

    if len(current_bullets) == 0:
        issues.append("No bullet points — CRITICAL for conversion and SEO")
    elif len(current_bullets) < 5:
        issues.append(f"Only {len(current_bullets)} bullets (minimum 5 recommended)")

    # Check if current bullets are keyword-rich
    short_bullets = [b for b in current_bullets if len(b) < 50]
    if short_bullets:
        issues.append(f"{len(short_bullets)} bullet(s) too short (< 50 chars)")

    return {
        "current_bullet_count": len(current_bullets),
        "current_issues": issues,
        "recommended_bullets": recommended,
        "recommended_keywords": template["keywords"],
    }


def analyze_listings(listings: list[dict]) -> list[dict]:
    """Analyze all PFT- listings and generate optimization recommendations."""
    results = []

    # Deduplicate by SKU base (parent SKU)
    seen_parents = {}
    for listing in listings:
        sku = listing["sku"]
        # Extract parent SKU (e.g., PFT-90034427 from PFT-90034427-8745)
        parts = sku.split("-")
        parent = "-".join(parts[:2]) if len(parts) >= 2 else sku

        if parent not in seen_parents:
            seen_parents[parent] = listing
        elif not seen_parents[parent]["title"] and listing["title"]:
            seen_parents[parent] = listing

    print(f"  Analyzing {len(seen_parents)} unique PFT parent SKUs...")

    for parent_sku, listing in sorted(seen_parents.items()):
        product_type = classify_product_type(parent_sku, listing["title"])
        title_analysis = generate_optimized_title(listing["title"], product_type, listing["color"])
        bullet_analysis = generate_optimized_bullets(listing["bullets"], product_type)

        # Overall SEO score (0-100)
        score = 100
        if has_nesell(listing["title"]):
            score -= 30
        if has_model_number(listing["title"]):
            score -= 20
        if len(listing["title"]) > 200:
            score -= 15
        if len(listing["title"]) < 80:
            score -= 10
        if len(listing["bullets"]) == 0:
            score -= 25
        elif len(listing["bullets"]) < 5:
            score -= 10
        if not listing["keywords"]:
            score -= 10

        needs_update = score < 85 or has_nesell(listing["title"]) or has_model_number(listing["title"])

        results.append({
            "parent_sku": parent_sku,
            "asin": listing["asin"],
            "product_type": product_type,
            "seo_score": max(0, score),
            "needs_update": needs_update,
            "current": {
                "title": listing["title"],
                "title_length": len(listing["title"]),
                "bullet_count": len(listing["bullets"]),
                "bullets": listing["bullets"],
                "keywords": listing["keywords"],
            },
            "optimized": {
                "title": title_analysis["optimized_title"],
                "title_issues": title_analysis["current_issues"],
                "title_suggestions": title_analysis["suggestions"],
                "bullets": bullet_analysis["recommended_bullets"],
                "bullet_issues": bullet_analysis["current_issues"],
                "backend_keywords": bullet_analysis["recommended_keywords"],
            },
        })

    return sorted(results, key=lambda x: x["seo_score"])


def write_markdown_report(results: list[dict], output_path: Path):
    """Write SEO optimization report as markdown."""
    needs_update = [r for r in results if r["needs_update"]]
    ok = [r for r in results if not r["needs_update"]]

    total = len(results)
    critical = sum(1 for r in results if has_nesell(r["current"]["title"]) or has_model_number(r["current"]["title"]))

    lines = [
        f"# Amazon SEO Optimization Report — PFT- Listings",
        f"**Date:** {date.today()}",
        f"**Marketplace:** DE (primary analysis; applies to all 8 EU markets)",
        f"",
        f"## Executive Summary",
        f"",
        f"| Metric | Count |",
        f"|--------|-------|",
        f"| Total PFT parent SKUs analyzed | {total} |",
        f"| Need optimization | {len(needs_update)} |",
        f"| Critical violations (nesell/model# in title) | {critical} |",
        f"| Already OK | {len(ok)} |",
        f"",
        f"### Top Issues Found",
        f"",
    ]

    # Aggregate issues
    issue_counts = defaultdict(int)
    for r in results:
        for issue in r["optimized"]["title_issues"]:
            issue_counts[issue] += 1
        for issue in r["optimized"]["bullet_issues"]:
            issue_counts[issue] += 1
    for issue, count in sorted(issue_counts.items(), key=lambda x: -x[1]):
        lines.append(f"- **{count}x** {issue}")

    lines += [
        "",
        "---",
        "",
        "## Listings Requiring Optimization",
        f"*{len(needs_update)} listings sorted by SEO score (lowest first)*",
        "",
    ]

    for r in needs_update[:50]:  # cap at top 50
        current_title = r["current"]["title"]
        optimized_title = r["optimized"]["title"]
        title_changed = current_title.strip() != optimized_title.strip()

        lines += [
            f"### {r['parent_sku']}",
            f"**ASIN:** {r['asin'] or 'N/A'}  |  **Type:** {r['product_type'].replace('_', ' ').title()}  |  **SEO Score:** {r['seo_score']}/100",
            f"",
        ]

        if r["optimized"]["title_issues"]:
            lines.append("**Title Issues:**")
            for issue in r["optimized"]["title_issues"]:
                lines.append(f"- ⚠️ {issue}")
            lines.append("")

        lines += [
            f"**Current title** ({r['current']['title_length']} chars):",
            f"> {current_title or '*(empty)*'}",
            f"",
        ]

        if title_changed:
            lines += [
                f"**Optimized title** ({len(optimized_title)} chars):",
                f"> {optimized_title}",
                f"",
            ]

        if r["optimized"]["bullet_issues"]:
            lines.append("**Bullet Issues:**")
            for issue in r["optimized"]["bullet_issues"]:
                lines.append(f"- ⚠️ {issue}")
            lines.append("")

        lines += [
            f"**Current bullets:** {r['current']['bullet_count']}",
        ]
        if r["current"]["bullets"]:
            for b in r["current"]["bullets"]:
                lines.append(f"- {b[:120]}{'...' if len(b) > 120 else ''}")

        lines += [
            f"",
            f"**Recommended bullets:**",
        ]
        for b in r["optimized"]["bullets"]:
            lines.append(f"- {b}")

        lines += [
            f"",
            f"**Backend keywords:** `{r['optimized']['backend_keywords']}`",
            f"",
            "---",
            "",
        ]

    # Summary of already-OK listings
    if ok:
        lines += [
            "## Listings Already Optimized",
            f"*{len(ok)} listings with SEO score ≥ 85*",
            "",
            "| SKU | ASIN | Type | Score | Title (truncated) |",
            "|-----|------|------|-------|-------------------|",
        ]
        for r in ok:
            title_short = r["current"]["title"][:60] + "..." if len(r["current"]["title"]) > 60 else r["current"]["title"]
            lines.append(f"| {r['parent_sku']} | {r['asin'] or 'N/A'} | {r['product_type'].replace('_', ' ').title()} | {r['seo_score']}/100 | {title_short} |")

    lines += [
        "",
        "---",
        "",
        "## Implementation Checklist",
        "",
        "- [ ] Fix all titles with 'nesell' brand prefix (critical rule violation)",
        "- [ ] Remove manufacturer model numbers from titles (Yupoong 6245CM, etc.)",
        "- [ ] Add missing bullet points (5 minimum per listing)",
        "- [ ] Add backend keywords (generic_keyword attribute)",
        "- [ ] Verify title length: 80–200 chars",
        "- [ ] Apply changes to all 8 EU marketplaces (DE, FR, IT, ES, NL, SE, PL, BE)",
        "",
        "## Notes",
        "",
        "- Optimized titles use product-type keywords per marketplace language",
        "- Bullet point templates above are for DE/EN — translate per marketplace",
        "- Backend keywords are English-first; add local language terms for FR/IT/ES/DE",
        "- Prioritize listings with ASIN (active) over those without",
        "",
    ]

    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nReport written to: {output_path}")


def main():
    output_dir = Path.home() / "nesell-analytics" / "research"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"amazon-seo-printful-{date.today()}.md"

    print(f"=== Amazon SEO Optimization — PFT- Listings ===")
    print(f"Date: {date.today()}")
    print(f"Fetching PFT- listings from DE marketplace...")

    # Fetch from DE as primary source
    raw_listings = fetch_pft_listings(MARKETPLACES["DE"], limit=300)
    print(f"Got {len(raw_listings)} PFT- items from DE")

    # Extract content
    listings = [extract_listing_content(item) for item in raw_listings]

    # Filter out listings with no meaningful data
    listings = [l for l in listings if l["sku"]]

    print(f"Analyzing {len(listings)} PFT- listing entries...")
    results = analyze_listings(listings)

    print(f"\nAnalysis complete:")
    print(f"  Total parent SKUs: {len(results)}")
    print(f"  Need optimization: {sum(1 for r in results if r['needs_update'])}")
    print(f"  Critical (nesell/model#): {sum(1 for r in results if has_nesell(r['current']['title']) or has_model_number(r['current']['title']))}")

    write_markdown_report(results, output_path)
    return results, output_path


if __name__ == "__main__":
    main()
