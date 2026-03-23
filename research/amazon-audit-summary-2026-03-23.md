# Amazon Listing Audit Summary — 2026-03-23

**Script:** `scripts/amazon_listing_audit.py`
**Full results:** `research/amazon-audit-2026-03-23.json`
**Previous audit:** `research/amazon-audit-2026-03-16.json` (7 days ago)

---

## 1. Audit Metadata

| Metric | Value |
|--------|-------|
| Audit date | 2026-03-23 |
| Seller ID | A1IZH6PW7A624A |
| Marketplaces audited | DE, FR, IT, ES, NL, PL, SE, BE (8) |
| Total listing records checked | 5 459 |
| Unique SKUs | 1 255 |
| Unique SKUs with issues | 1 224 |
| Unique ASINs with issues | 1 044 |
| Total issues flagged | 28 859 |

**Note:** Issues are per-marketplace-per-SKU (one SKU on 8 markets = up to 8 records). The bulk of issues (~27k) are on resell products (Nike, cosmetics, etc.) where attributes live on the ASIN catalog, not the seller listing.

---

## 2. Issues by Severity

| Severity | Count | % of Total | Change vs 2026-03-16 |
|----------|-------|------------|----------------------|
| CRITICAL | 12 128 | 42.0% | +442 (+3.8%) |
| WARNING | 6 537 | 22.7% | +49 (+0.8%) |
| INFO | 10 194 | 35.3% | +14 (+0.1%) |
| **Total** | **28 859** | **100%** | **+505 (+1.8%)** |

---

## 3. Top 10 Issues by Frequency

| # | Issue Type | Count | Severity | Description |
|---|-----------|-------|----------|-------------|
| 1 | `missing_gpsr` | 3 540 | WARNING | Missing GPSR safety attestation / responsible party address |
| 2 | `missing_keywords` | 3 521 | INFO | No backend keywords (generic_keyword) |
| 3 | `missing_brand` | 3 498 | INFO | No brand attribute set |
| 4 | `missing_bullets` | 3 412 | CRITICAL | No bullet points at all |
| 5 | `missing_description` | 3 175 | INFO | No product description |
| 6 | `missing_images` | 3 067 | CRITICAL | No images found in attributes |
| 7 | `missing_title` | 2 644 | CRITICAL | No title attribute |
| 8 | `short_title` | 1 264 | WARNING | Title under 80 characters |
| 9 | `api_issue_100527` | 630 | WARNING | GPSR manufacturer info required for EU stores |
| 10 | `api_issue_100720` | 459 | CRITICAL | Invalid language data for marketplace attributes |

---

## 4. Breakdown by Marketplace

| Marketplace | Listings | Total Issues | CRITICAL | WARNING | INFO | Issues/Listing |
|-------------|----------|-------------|----------|---------|------|----------------|
| DE | 887 | 4 651 | 1 799 | 1 200 | 1 652 | 5.2 |
| BE | 765 | 4 741 | 2 297 | 884 | 1 560 | 6.2 |
| NL | 681 | 3 616 | 1 429 | 884 | 1 303 | 5.3 |
| PL | 653 | 3 172 | 1 289 | 660 | 1 223 | 4.9 |
| ES | 639 | 3 451 | 1 466 | 806 | 1 179 | 5.4 |
| FR | 640 | 3 364 | 1 387 | 799 | 1 178 | 5.3 |
| IT | 624 | 3 182 | 1 341 | 711 | 1 130 | 5.1 |
| SE | 570 | 2 682 | 1 120 | 593 | 969 | 4.7 |

**Worst marketplace:** BE (6.2 issues/listing, highest CRITICAL count at 2 297).

---

## 5. Comparison with Previous Audit (2026-03-16)

### Overall

| Metric | 2026-03-16 | 2026-03-23 | Delta |
|--------|------------|------------|-------|
| Total listings | 5 029 | 5 459 | +430 (+8.6%) |
| Unique SKUs | 1 228 | 1 255 | +27 (+2.2%) |
| Total issues | 28 354 | 28 859 | +505 (+1.8%) |
| CRITICAL | 11 686 | 12 128 | +442 (+3.8%) |
| WARNING | 6 488 | 6 537 | +49 (+0.8%) |
| INFO | 10 180 | 10 194 | +14 (+0.1%) |
| Unique issue types | 85 | 90 | +5 new types |

### SKU Movement
- **43 new SKUs** now have issues (not present in previous audit)
- **9 SKUs** no longer have issues (resolved or delisted)

### Notable Changes in Top Issues

| Issue Type | Mar 16 | Mar 23 | Delta | Notes |
|-----------|--------|--------|-------|-------|
| `missing_images` | 2 969 | 3 067 | +98 | Growing — new listings lack images |
| `api_issue_100720` (lang mismatch) | 279 | 459 | +180 | Significant increase — likely new FR listings |
| `api_issue_18304` (brand gating) | 133 | 248 | +115 | More SKUs hitting brand restrictions |
| `api_issue_18320` (main image) | 275 | 146 | -129 | Improved — image fixes working |
| `short_title` | 1 332 | 1 264 | -68 | Improved — title updates applied |
| `api_issue_90244` (invalid attrs) | 277 | 324 | +47 | New listings with bad attribute values |
| `missing_title` | 2 648 | 2 644 | -4 | Flat |

### Hat Products (PFT-/MEGA- SKUs)

| Metric | Mar 16 | Mar 23 | Delta |
|--------|--------|--------|-------|
| Unique hat SKUs with issues | 176 | 209 | +33 |
| Total hat issues | 1 415 | 1 650 | +235 |
| `short_title` | 666 | 598 | -68 (improved) |
| `api_issue_18320` (main image) | 272 | 142 | -130 (improved) |
| `api_issue_100720` (lang mismatch) | 245 | 407 | +162 (worsened) |
| `missing_images` | 111 | 208 | +97 (worsened) |
| `api_issue_18155` (price floor) | 22 | 53 | +31 (worsened) |
| `api_issue_100893` (EPR/ERN) | 0 | 20 | +20 (new) |
| `missing_bullets` | 6 | 33 | +27 (worsened) |
| `api_issue_8801` | 0 | 67 | +67 (new) |

**Good news:** Main image errors (18320) dropped by 130 and short titles dropped by 68 — previous fixes are working.

**Bad news:** Language mismatch (100720) spiked by +162, missing images up +97, and price floor violations up +31 — likely caused by new hat listings pushed without proper FR localization and image uploads.

---

## 6. Actionable Recommendations — Top 3 Priorities

### Priority 1: Fix FR Language Mismatch — 407 hat issues (CRITICAL)
- **Issue:** `api_issue_100720` — invalid language tags for "style" attribute on FR marketplace
- **Impact:** 407 hat listing records affected, up from 245 last week (+66%)
- **Root cause:** New hat listings propagated to FR with wrong language tag (e.g. `de_DE` instead of `fr_FR`)
- **Action:** Audit `etl/amazon_listings.py` FR propagation logic. Ensure all attribute lang tags match the target marketplace. Fix and re-push affected SKUs.
- **Effort:** Medium. Script fix + batch re-push.

### Priority 2: Upload Missing Images for New Hat Listings — 208 issues (CRITICAL)
- **Issue:** `missing_images` on hat SKUs nearly doubled (111 -> 208)
- **Impact:** Listings without images are suppressed from search — zero visibility
- **Root cause:** New hat variants pushed to Amazon before Printful mockup images were generated/uploaded
- **Action:** Generate mockups for all 208 affected hat SKU/marketplace combos. Also fix 8 expired Printful S3 URLs (`api_issue_300403`) and 8 broken image format issues (`api_issue_20017`).
- **Effort:** Medium. Batch mockup generation + upload.

### Priority 3: Fix Price Floor Violations — 53 hat issues (CRITICAL)
- **Issue:** `api_issue_18155` — price below Amazon's minimum floor, up from 22 to 53
- **Impact:** Listings are blocked from being buyable until price is corrected
- **Root cause:** New listings pushed with prices that don't meet Amazon's minimum for the category/marketplace
- **Action:** Query affected SKUs, identify the floor prices, and adjust. Consider adding a pre-push price validation check to the ETL pipeline.
- **Effort:** Low. Price update via Listings API.

### Additional Quick Wins
- **Brand gating** (`api_issue_18304`): 248 resell SKUs hitting brand restrictions — review and delist products you can't sell
- **GPSR compliance** (`missing_gpsr` + `api_issue_100527/100528`): 3 540+ warnings — EU GPSR regulation deadline approaching, bulk-update manufacturer info
- **api_issue_100893** (EPR/ERN): 20 new hat issues — provide Extended Producer Responsibility registration numbers for DE/FR

---

## Summary

The catalog grew by 430 listings (+8.6%) but issues grew only +1.8%, meaning new listings are generally cleaner. Two areas regressed significantly: **FR language tags** and **missing images on new hat listings**, both caused by the same root issue — new hat variants being pushed without complete localization and image assets. The main image fix from last week was successful (-130 errors), proving the pipeline works when properly configured.

**Total hat issues requiring action: ~1 650 across 209 SKUs**
**Estimated fix effort: 2-3 hours of pipeline fixes + batch re-push**

---

## Files
- Full JSON (current): `research/amazon-audit-2026-03-23.json`
- Full JSON (previous): `research/amazon-audit-2026-03-16.json`
- Audit script: `scripts/amazon_listing_audit.py`
