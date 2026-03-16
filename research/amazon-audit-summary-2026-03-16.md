# Amazon Listing Audit Summary — 2026-03-16

**Script:** `scripts/amazon_listing_audit.py`
**Full results:** `research/amazon-audit-2026-03-16.json`

---

## Overview

| Metric | Value |
|--------|-------|
| Marketplaces audited | DE, FR, IT, ES, NL, PL, SE, BE |
| Total listing records checked | 5 029 |
| Unique SKUs | 1 228 |
| Total issues flagged | 28 354 |
| CRITICAL | 11 686 |
| WARNING | 6 488 |
| INFO | 10 180 |

**Note:** Issues are per-marketplace-per-SKU (one SKU on 8 markets = up to 8 records).
The bulk of issues (~26 900) are on non-hat resell products (Nike, cosmetics, etc.)
which lack hat-specific attributes — not necessarily actionable.

---

## Marketplace Breakdown

| MKT | Listings | CRITICAL | WARNING | INFO |
|-----|----------|----------|---------|------|
| DE | 858 | 1 762 | 1 282 | 1 652 |
| BE | 702 | 2 097 | 879 | 1 556 |
| NL | 619 | 1 407 | 869 | 1 300 |
| PL | 591 | 1 261 | 649 | 1 220 |
| ES | 577 | 1 427 | 786 | 1 176 |
| FR | 580 | 1 338 | 774 | 1 175 |
| IT | 562 | 1 299 | 690 | 1 127 |
| SE | 540 | 1 095 | 559 | 974 |

---

## Top 5 Issues by Frequency (all products)

| # | Issue Type | Count | Severity |
|---|-----------|-------|----------|
| 1 | `missing_gpsr` | 3 533 | WARNING |
| 2 | `missing_keywords` | 3 515 | INFO |
| 3 | `missing_brand` | 3 494 | INFO |
| 4 | `missing_bullets` | 3 385 | CRITICAL |
| 5 | `missing_description` | 3 171 | INFO |

---

## Hat Products Specific Analysis (PFT-/MEGA- SKUs)

These are our own Printful print-on-demand listings — fully actionable.

| Issue | Count | Severity |
|-------|-------|----------|
| `short_title` (< 80 chars) | 666 | WARNING |
| `api_issue_18320` — Missing/invalid main image | 272 | CRITICAL |
| `api_issue_100720` — Invalid language data (FR marketplace, style attr) | 245 | CRITICAL |
| `missing_images` | 111 | CRITICAL |
| `api_issue_18155` — Price below floor | 22 | CRITICAL |
| `missing_keywords` | 22 | INFO |
| `missing_gpsr` | 21 | WARNING |
| `few_bullets` | 14 | WARNING |
| `api_issue_20017/20015` — Broken/unsupported image URLs | 17 | CRITICAL |
| `missing_title` | 6 | CRITICAL |

---

## Key Findings & Recommended Actions

### 1. Short Titles on Hat Listings — 666 occurrences (WARNING)
- Many child variants are 64-77 chars, just under the 80-char threshold
- Examples: `PFT-93856317-7855` (France NF, Beige) = 64 chars
- **Action:** Add color name + "Baseball Cap" suffix to child titles to push past 80 chars
- **Risk:** Low — title update via Listings API PATCH

### 2. Missing Main Image — 272 CRITICAL errors from Amazon
- Amazon error 18320: main image missing or invalid → listing suppressed from search
- Affects hat child variants across multiple markets
- **Action:** Identify which SKUs, re-upload images via `etl/image_manager.py`
- **Risk:** High if not fixed — these listings won't show in search

### 3. Invalid Language Data on FR Marketplace — 245 CRITICAL
- Error 100720: language tag mismatch for "style" attribute on FR marketplace (679831071)
- Root cause: likely using wrong lang tag (e.g. `de_DE` instead of `fr_FR`) for FR style field
- **Action:** Audit `etl/amazon_listings.py` FR propagation, fix lang tag on `item_type_name`/style

### 4. Price Below Floor — 22 CRITICAL (api_issue_18155)
- These hat SKUs are priced below Amazon's minimum floor
- **Action:** Review and reprice; likely requires slight price increase

### 5. Broken Printful Image URLs — 17 CRITICAL (api_issue_20017/20015)
- Temp S3 URLs from Printful (printful-upload.s3-accelerate.amazonaws.com/tmp/…) expired
- **Action:** Re-generate mockups and re-upload permanent image URLs

### 6. Missing Images on 871 SKUs — CRITICAL
- `missing_images`: 2 969 instances, 871 unique SKUs
- Most are resell products without image attributes in SP-API response
- Hat-specific: 111 instances
- **Action:** Check hat SKUs first; resell products may use ASIN-level images not returned by API

### 7. No Suppressed Listings — GOOD
- Zero listings with SUPPRESSED status — all active

---

## Resell Products (Non-Hat)
- ~963 unique SKUs missing bullet points
- ~855 unique SKUs with image issues
- ~2 642 missing titles
- These are likely EAN-based listings (Nike, cosmetics) where attributes
  live on the ASIN catalog, not the seller listing — low priority to fix via API

---

## Files
- Full JSON: `/tmp/amazon-audit-2026-03-16.json` (also in `research/`)
- Audit script: `scripts/amazon_listing_audit.py`

## Next Steps (for human review)
1. Fix broken image URLs (api_issue_20017/20015) — 17 SKUs, high impact
2. Fix FR language tag issue (api_issue_100720) — 245 errors in `etl/amazon_listings.py`
3. Extend child hat titles past 80 chars — 666 warnings
4. Review 22 below-floor-price SKUs and reprice
5. Upload missing main images for hat child variants — 272 errors
