# Printful Scale Pipeline — Production Guide

## Overview

Automated pipeline for creating hat listings at scale:
**Design PNG → Printful Upload → Auto-Mockups → Amazon 8 EU Markets**

Collection: **SUM26 (Summer 2026)** — 10 niches, ~40 SKUs, 8 markets = ~400 listings

## Quick Start

```bash
cd ~/nesell-analytics

# 1. Dry run first (always!)
python3.11 scripts/pipeline_printful_scale.py --dry-run --step all

# 2. Upload designs to Printful
python3.11 scripts/pipeline_printful_scale.py --step upload-designs

# 3. Generate mockups
python3.11 scripts/pipeline_printful_scale.py --step generate-mockups

# 4. Create Amazon listings (all 8 EU markets)
python3.11 scripts/pipeline_printful_scale.py --step create-listings

# Or run everything at once:
python3.11 scripts/pipeline_printful_scale.py --step all
```

## Design Files

Place design PNG files in: `~/nesell-analytics/designs/sum26/`

**File naming**: `<product-key>.png` (e.g., `mountain.png`, `coffee.png`)

**Requirements**:
- Min 4000x4000 px (embroidery quality)
- Transparent background (PNG)
- Simple design: icon + short text (embroidery, not full-color print)

### 10 Design Niches

| Key | Niche | Design Concept | Hat Type |
|-----|-------|----------------|----------|
| `mountain` | Mountain/Outdoor | Mountain peak + "EXPLORE" | Dad Hat |
| `coffee` | Coffee/Barista | Coffee cup + "But First, Coffee" | Dad Hat |
| `surf` | Surf/Beach | Wave drawing + "SURF" | Trucker |
| `cycling` | Cycling/Bike | Bicycle icon + "RIDE" | Dad Hat |
| `dog-dad` | Dog Lover | Paw print + "Dog Dad" | Dad Hat |
| `vinyl` | Music/Vinyl | Vinyl record + "ANALOG" | Trucker |
| `garden` | Gardening | Leaf/sprout + "GROW" | Bucket |
| `astro` | Astronomy/Space | Moon + stars + "COSMOS" | Dad Hat |
| `camping` | Camping/Outdoor | Tent + stars + "WILD" | Trucker |
| `yoga` | Yoga/Wellness | Lotus flower + "BREATHE" | Dad Hat |

## SKU Format

```
PFT-SUM26-{NICHE}-{COLOR}

Examples:
  PFT-SUM26-MOUNTAIN          (parent)
  PFT-SUM26-MOUNTAIN-BLACK    (child variant)
  PFT-SUM26-MOUNTAIN-KHAKI    (child variant)
```

## Pricing

| Tier | Hat Type | DE/FR/IT/ES/NL/BE | PL | SE |
|------|----------|--------------------|----|-----|
| Standard | Dad Hat, Bucket | €24.99 | 109.99 PLN | 279 SEK |
| Premium | Trucker | €27.99 | 119.99 PLN | 309 SEK |

Sale price: 85% of list price (auto-calculated)

## Pipeline Steps Detail

### Step 1: `upload-designs`
- Reads PNG files from `designs/sum26/`
- Uploads each to Printful file library via API
- Saves file IDs to `pipeline_sum26_state.json` (resumable)
- Skips already-uploaded files

### Step 2: `generate-mockups`
- Uses Printful Mockup Generator API (async)
- Fetches available mockup styles per product
- Creates mockup task, polls for completion (~10-30 sec per product)
- Saves mockup URLs to state file
- Skips products with existing mockups

### Step 3: `create-listings`
- Creates parent + child listings via Amazon SP-API
- Full localization: titles, bullets, keywords, colors for 8 EU markets
- Parent-child COLOR variation theme
- Checks for existing listings (skip duplicates)
- GPSR compliance fields included

## CLI Options

```bash
--step          upload-designs | generate-mockups | create-listings | all
--product       mountain | coffee | surf | ... | all (default: all)
--market        DE | FR | IT | ES | NL | PL | SE | BE | all (default: all)
--dry-run       Print what would happen without API calls
--design-dir    Custom design directory (default: designs/sum26/)
```

## State Management

The pipeline saves state to `pipeline_sum26_state.json`:
- Tracks uploaded designs (file IDs)
- Tracks generated mockups (URLs)
- Allows resuming from any step

To reset and re-run: delete `pipeline_sum26_state.json`

## Repeating for a New Collection

1. Copy `pipeline_printful_scale.py` to `pipeline_printful_<collection>.py`
2. Change the collection prefix (e.g., `FALL26`, `WIN27`)
3. Update `PRODUCTS` dict with new niches
4. Update `DESIGNS_DIR`, `STATE_FILE`, `RESULTS_FILE` paths
5. Place new design PNGs in the designs directory
6. Run the pipeline

## Dependencies

- `etl.amazon_listings` — Amazon SP-API wrapper (same project)
- `requests` — HTTP client
- Credentials: `~/.keys/printful.env`, `~/.keys/amazon-sp-api.json`

## Existing Products (Don't Duplicate)

- `PFT-*` base products (5 styles)
- `PFT-S26-*` Spring 2026 collection (18 SKUs)
- This collection: `PFT-SUM26-*` (new, non-overlapping)
