# Allegro Ads (Oferty Promowane) — Launch Report

**Date:** 2026-03-16
**Agent:** marketer
**Task:** task_k9EFRlxz8oSy

---

## Executive Summary

**Allegro Ads (CPC / Oferty Promowane) cannot be launched via API.** The Allegro Ads API is not publicly available — access is restricted to certified Allegro Ads agencies. Campaign management must be done manually through the **ads.allegro.pl** web panel.

This report provides: current offer status, recommended top 3 for advertising, step-by-step manual setup instructions, and recommended bid/budget strategy.

---

## Current Offer Status (2026-03-16)

| # | SKU | Offer ID | Product | Price | Status | Visits | Watchers | Sales |
|---|-----|----------|---------|-------|--------|--------|----------|-------|
| 1 | PFT-WDH | 18414728279 | Washed Dad Hat | 89.99 PLN | **ACTIVE** | 0 | 0 | 0 |
| 2 | PFT-DDH | 18414731024 | Distressed Dad Hat | 96.99 PLN | **ACTIVE** | 0 | 0 | 0 |
| 3 | PFT-CCH | 18414731041 | Corduroy Cap | 104.99 PLN | **ENDED** | 0 | 0 | 0 |
| 4 | PFT-OBH | 18414731001 | Organic Bucket Hat | 119.99 PLN | **ACTIVE** | 0 | 0 | 0 |
| 5 | PFT-BBH | 18414808073 | Bucket Hat | 99.99 PLN | **ACTIVE** | 0 | 0 | 0 |

**Notes:**
- All offers are brand new (listed 2026-03-16), hence 0 stats across the board.
- PFT-CCH (Corduroy Cap) was deactivated and **cannot be reactivated** — Allegro reports a duplicate active listing violation. Needs manual investigation: check if there's another active corduroy cap listing in the same subcategory.
- All 4 active offers have promo package "emphasized10d" applied (highlighted listing).
- Offers are also listed on CZ, SK, HU marketplaces with price automation.

---

## Top 3 Recommended for Allegro Ads

Since all offers have 0 stats (too new for data-driven selection), ranking is based on **price competitiveness** and **search volume potential**:

| Priority | SKU | Product | Price | Rationale |
|----------|-----|---------|-------|-----------|
| **1** | PFT-WDH | Washed Dad Hat | 89.99 PLN | Lowest price point, "dad hat" is highest-volume keyword in PL |
| **2** | PFT-BBH | Bucket Hat | 99.99 PLN | "Bucket hat" trending keyword, competitive price under 100 PLN |
| **3** | PFT-DDH | Distressed Dad Hat | 96.99 PLN | Differentiated product (distressed), under 100 PLN psychological barrier |

PFT-OBH (119.99 PLN) excluded due to highest price — harder to convert without reviews/sales history.

---

## Allegro Ads Setup Instructions (Manual)

### Step 1: Access Allegro Ads Panel
- Go to **https://ads.allegro.pl** (log in with your Allegro seller account)
- Or: Allegro Centrum Sprzedawcy → left menu → "Allegro Ads"

### Step 2: Create Campaign
1. Click **"Dodaj kampanię"** (Add Campaign)
2. Select **"Oferty sponsorowane"** (Sponsored Offers)
3. Name: `POD Czapki - Launch Q1 2026`

### Step 3: Create Ad Group
1. Name: `Top 3 Dad Hat + Bucket Hat`
2. Select **"Ręcznie wybiorę oferty"** (Manual selection)
3. Add offers by ID:
   - `18414728279` (PFT-WDH — Washed Dad Hat)
   - `18414808073` (PFT-BBH — Bucket Hat)
   - `18414731024` (PFT-DDH — Distressed Dad Hat)

### Step 4: Set Budget & Bids
| Setting | Recommended Value | Notes |
|---------|-------------------|-------|
| Daily budget | **15 PLN** per ad group | Min is 3 PLN; 15 PLN gives enough data |
| CPC bid | **0.30 PLN** (manual) | Min for Fashion = 0.10-0.20 PLN; 0.30 gives competitive placement |
| Campaign budget cap | **450 PLN/month** | 15 PLN × 30 days |
| Bid strategy | **Manual CPC** initially | Switch to Dynamic CPC after 2 weeks of data |

### Step 5: ROAS Target Validation
- Average hat price: ~95.66 PLN
- Target ROAS: 3x → max acceptable CPA = 31.89 PLN
- At 0.30 PLN/click → need 1 sale per ~106 clicks (0.94% conversion rate)
- Fashion average CR on Allegro: 1-3% → **target is achievable**

### Step 6: Launch & Monitor
1. Set campaign to **active immediately** (no end date for now)
2. Check stats after 3 days at ads.allegro.pl → Statistics tab
3. After 1 week: review CPC vs CTR, adjust bids
4. After 2 weeks: switch to Dynamic CPC if ROAS > 3x

---

## ROAS Projections

| Scenario | CPC | Daily Budget | Daily Clicks | CR | Daily Sales | Daily Revenue | Daily ROAS |
|----------|-----|-------------|-------------|-----|------------|--------------|------------|
| Conservative | 0.30 PLN | 15 PLN | 50 | 1.0% | 0.5 | 47.83 PLN | 3.2x |
| Moderate | 0.25 PLN | 15 PLN | 60 | 1.5% | 0.9 | 86.09 PLN | 5.7x |
| Optimistic | 0.20 PLN | 15 PLN | 75 | 2.0% | 1.5 | 143.49 PLN | 9.6x |

---

## Blockers & Action Items

### Blockers (require manual action by owner)
1. **[ ] Set up campaign at ads.allegro.pl** — cannot be done via API (see instructions above)
2. **[ ] Investigate PFT-CCH duplicate** — check if there's another active corduroy cap listing causing the conflict, merge or remove the duplicate

### Follow-up Tasks
3. **[ ] Re-check stats after 3 days** — once offers accumulate organic visits, re-evaluate top 3 selection
4. **[ ] Review Allegro Ads stats after 1 week** — adjust bids based on actual CTR and CR data
5. **[ ] Consider applying "emphasized" renewal** — current emphasized10d package will expire in ~8 days

---

## API Limitations Documented

| Feature | API Available? | Alternative |
|---------|---------------|-------------|
| Promo packages (emphasized) | YES — `PUT /sale/offers/promo-options-commands/{id}` | N/A |
| Loyalty promotions (discounts) | YES — `POST /sale/loyalty/promotions` | N/A |
| Offer statistics (views/watchers) | YES — `GET /sale/offers?offer.id=...` | N/A |
| **CPC Ads (Oferty Promowane)** | **NO** — returns 404 | **Manual via ads.allegro.pl** |
| **Campaign management** | **NO** — restricted to certified agencies | **Manual via ads.allegro.pl** |
| Badge campaigns | Partial — `GET /sale/badge-campaigns` (read only) | N/A |

---

## Files Generated
- `/nesell-analytics/reports/allegro-offer-stats-2026-03-16.json` — raw API data for all 5 offers
- `/nesell-analytics/reports/allegro-ads-launch.md` — this report
- `/allegro-mcp/get_offer_stats.py` — reusable stats collection script
- `/allegro-mcp/reactivate_cch.py` — reactivation script (blocked by duplicate violation)
