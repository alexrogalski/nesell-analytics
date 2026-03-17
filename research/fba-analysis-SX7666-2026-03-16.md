# FBA Viability Analysis — SX7666-010 (Nike Everyday Cushion Crew Socks 6-pack)
**Date:** 2026-03-16
**Task:** task_K9B5PdiVOgMW
**Analyst:** Developer Agent

---

## 1. Product Overview

| Field | Value |
|-------|-------|
| SKU (M) | SX7666-010-M |
| SKU (L) | SX7666-010-L |
| EAN (M) | 888408282750 |
| EAN (L) | 888408282767 |
| ASIN (M) | B09999W47M |
| ASIN (L) | B09999X3TH |
| Baselinker parent product ID | 153976189 |
| Baselinker variant ID (M) | 153589542 |
| Baselinker variant ID (L) | 153589544 |
| Inventory ID | 30229 |
| Product | Nike Everyday Cushion Crew Socks 6-pack Black |
| Q1 2026 Volume | 341 units/quarter (M+L combined) |
| Monthly Run Rate | ~114 units/month |
| Current M price (Baselinker) | 94.22 PLN |
| Current L price (Baselinker) | 47.50 PLN ⚠️ CRITICAL BUG |
| COGS (M, from hurtownia-sportowa) | ~49.59 PLN (94.22 / 1.9 formula) |
| COGS (L, from hurtownia-sportowa) | ~25.00 PLN (47.50 / 1.9 formula) ⚠️ WRONG MAPPING? |

---

## 2. Critical Findings

### ⚠️ BLOCKER: Price Sync Overriding Manual Updates

**Root cause discovered:** Both SKUs have their prices controlled by the `hurtownia-sportowa` wholesale catalog integration (warehouse ID: 5013609). Price group 30673 uses a formula: `netto × 1.9`. Manual API writes return `SUCCESS` but prices revert immediately.

**Implications:**
- **M variant:** `netto × 1.9 = 94.22 PLN` → hurtownia netto cost ~49.59 PLN (correct for Nike 6-pack)
- **L variant:** `netto × 1.9 = 47.50 PLN` → hurtownia netto cost ~25 PLN — this is either:
  - **Wrong product mapping** in hurtownia-sportowa (L is mapped to a cheaper Nike variant)
  - **Different COGS** for L size in the supplier feed (unlikely — same product)
  - **Data error** in the supplier catalog

**Action required (OWNER, UI only):**
Go to Baselinker → Catalogue → Integrations → hurtownia-sportowa → Price settings → disable price sync for SKUs `SX7666-010-M` and `SX7666-010-L`, or override pricing formula for these two variants.

---

## 3. Current FBM P&L (Baseline)

### Per-Unit Economics

| Scenario | M at 94.22 PLN | L at 47.50 PLN (current bug) | L at 94.22 PLN (corrected) |
|----------|---------------|------------------------------|---------------------------|
| Revenue | 94.22 | 47.50 | 94.22 |
| COGS | −50.00 | −50.00 | −50.00 |
| Amazon Referral Fee (15%) | −14.13 | −7.13 | −14.13 |
| FBM Shipping | −26.00 | −26.00 | −26.00 |
| **Net Profit/unit** | **+4.09** | **−35.63** | **+4.09** |
| **Margin** | **4.3%** | **−75%** | **4.3%** |

> If L has been selling at 47.50 PLN with real COGS ~50 PLN, every unit sold was a **−35.63 PLN loss**. Urgent to verify actual L sales history.

---

## 4. FBA Fee Estimate

### Product Specs (Nike SX7666-010 6-pack)
- Dimensions (packaged): ~22 × 15 × 8 cm
- Weight (6 pairs crew socks + packaging): ~500–600g
- Amazon size tier: **Standard S2** (400–600g clothing)

### Amazon EU FBA Fee Schedule 2025 — Standard Clothing
| Scenario | FBA Fee Estimate | Basis |
|----------|-----------------|-------|
| Optimistic | 10–12 PLN | Amazon FBA Revenue Calculator estimate (verify via ASIN B09999W47M) |
| Base Case | 14–16 PLN | Standard S2 tier at ~500g |
| Conservative | 18–20 PLN | Standard S3 tier at ~600g + packaging |

> **Must verify** via Amazon Seller Central → FBA Revenue Calculator → ASIN B09999W47M (M) and B09999X3TH (L).

### FBA Additional Costs Per Unit
| Item | PLN |
|------|-----|
| FBA Storage (20×15×8cm = 0.0024 m³, ~35 PLN/m³/month) | ~0.08 |
| Prep costs (FNSKU label only — Nike socks in sealed retail pack) | ~0.50 |
| Inbound shipping to FBA warehouse | ~0.80 |
| **Total FBA Overhead** | **~1.40 → round to 2 PLN** |

---

## 5. FBM vs FBA P&L Comparison (at corrected price basis)

### Scenario Matrix — Per Unit at Various Price Points

| Scenario | Price | Shipping/FBA fee | Ref Fee (15%) | COGS | FBA Overhead | **Net** | **Margin** |
|----------|-------|-----------------|---------------|------|-------------|---------|------------|
| FBM current (M only) | 94.22 | −26.00 | −14.13 | −50.00 | — | **+4.09** | **4.3%** |
| FBM repriced 115 | 115.00 | −26.00 | −17.25 | −50.00 | — | **+21.75** | **18.9%** |
| FBM repriced 120 | 120.00 | −26.00 | −18.00 | −50.00 | — | **+26.00** | **21.7%** |
| FBA @ 97.55, optimistic (10 PLN) | 97.55 | −10.00 | −14.63 | −50.00 | −2.00 | **+20.92** | **21.4%** |
| FBA @ 97.55, base (15 PLN) | 97.55 | −15.00 | −14.63 | −50.00 | −2.00 | **+15.92** | **16.3%** |
| FBA @ 97.55, conservative (19 PLN) | 97.55 | −19.00 | −14.63 | −50.00 | −2.00 | **+11.92** | **12.2%** |
| **FBA @ 115, optimistic** | 115.00 | −10.00 | −17.25 | −50.00 | −2.00 | **+35.75** | **31.1%** |
| **FBA @ 115, base** | 115.00 | −15.00 | −17.25 | −50.00 | −2.00 | **+30.75** | **26.7%** |
| **FBA @ 115, conservative** | 115.00 | −19.00 | −17.25 | −50.00 | −2.00 | **+26.75** | **23.3%** |
| FBA @ 120, base | 120.00 | −15.00 | −18.00 | −50.00 | −2.00 | **+35.00** | **29.2%** |

---

## 6. Monthly Impact (at 114 units/month)

| Scenario | Monthly Profit | vs Current FBM | Monthly Uplift |
|----------|----------------|----------------|----------------|
| FBM current (baseline, M only data) | 466 PLN | — | — |
| FBM repriced to 115 | 2,480 PLN | +2,014 PLN | **+2,014** |
| FBM repriced to 120 | 2,964 PLN | +2,498 PLN | **+2,498** |
| FBA @ 115, base case | 3,506 PLN | +3,040 PLN | **+3,040** |
| FBA @ 115, conservative | 3,050 PLN | +2,584 PLN | **+2,584** |
| FBA @ 120, base case | 3,990 PLN | +3,524 PLN | **+3,524** |

> The +676–776 PLN/month estimate in the original task was the incremental FBA vs FBM improvement **at the same repriced price**. Total uplift from baseline is much larger.

---

## 7. Decision

### ✅ DECISION: Switch to FBA + Reprice to 115 PLN → 120 PLN

**Rationale:**
1. **All FBA scenarios exceed 12% margin threshold** — even conservative FBA at current price = 12.2%.
2. **FBA at repriced 115 PLN delivers 23–31% margin** regardless of fee uncertainty.
3. **FBA + reprice dominates FBM + reprice**: Prime badge improves conversion; shipping risk eliminated.
4. **Nike socks retail at 119–139 PLN** on Zalando/Nike.pl — 115 PLN is competitive with Prime.
5. **SX7666-010-L price bug is critical**: If L sold at 47.50 PLN with 50 PLN COGS, every unit was a ~36 PLN loss. Must fix immediately.

---

## 8. Implementation Checklist — Action Owner Mapping

### 🚨 URGENT (OWNER — Baselinker UI)
- [ ] **Disable price sync** for SX7666-010-M and SX7666-010-L in hurtownia-sportowa integration
  - Go to: Baselinker → Catalogue → Sources/Integrations → hurtownia-sportowa → price override for these SKUs
- [ ] **Check L variant sales history** — verify if it ever sold at 47.50 PLN and how many units (likely a loss)
- [ ] **Reprice M to 115 PLN** once price sync disabled
- [ ] **Reprice L to 115 PLN** once price sync disabled

### VERIFY (OWNER — Amazon Seller Central)
- [ ] **FBA Revenue Calculator:** ASIN B09999W47M → enter 115 PLN → record actual FBA fee
- [ ] **Confirm no listing issues** for M (54 units in stock in bl_51139) or L (0 stock)

### OWNER — FBA Shipment
- [ ] Create FBA shipment: 50 M + 50 L units
  - Inbound to: Amazon.pl warehouse (Łódź/Szczecin)
  - Prep: FNSKU labels only (retail packaging already suitable)
- [ ] Monitor week 1: conversion rate vs FBM period
- [ ] Week 3: raise to 120 PLN if conversion holds

### DEVELOPER (Baselinker API — after owner disables price sync)
- [ ] Update M price via API: variant 153589542, inventory 30229, price group 30673 → 115 PLN
- [ ] Update L price via API: variant 153589544, inventory 30229, price group 30673 → 115 PLN

---

## 9. Expected P&L Impact Summary

| Horizon | Current State | Post-FBA @ 115 PLN (base case) | Uplift |
|---------|---------------|-------------------------------|--------|
| Monthly profit | ~466 PLN | 3,506 PLN | **+3,040 PLN/month** |
| Annual profit | ~5,592 PLN | 42,072 PLN | **+36,480 PLN/year** |
| Margin | 4.3% | 26.7% | +22.4 pp |

*Assumes stable volume 114 units/month. FBA + Prime badge may increase volume.*

---

## 10. Data Appendix

### Baselinker Product Data
| Field | M | L |
|-------|---|---|
| Baselinker variant ID | 153589542 | 153589544 |
| Parent product ID | 153976189 | 153976189 |
| EAN | 888408282750 | 888408282767 |
| ASIN | B09999W47M | B09999X3TH |
| Stock (bl_51139) | 54 szt. | 0 szt. |
| Current price (group 30673) | 94.22 PLN | 47.50 PLN |
| Price formula | netto × 1.9 | netto × 1.9 |
| Implied netto/COGS | 49.59 PLN | 25.00 PLN ⚠️ |

### Key Concern: L Variant COGS Anomaly
The L variant shows implied COGS of ~25 PLN in hurtownia-sportowa. If accurate, L has a different (lower) COGS than M. This would mean at 115 PLN with FBA base fee (15 PLN):
- L at 115 PLN FBA (COGS 25 PLN): 115 − 15 − 17.25 − 25 − 2 = **+55.75 PLN/unit (48.5% margin)**

However, this is likely a **wrong product mapping** in the supplier catalog. Before assuming lower COGS for L, verify by checking the actual supplier invoice.
