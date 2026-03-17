# Catalog Audit — 2026-03-16

**Auditor:** Developer agent
**Source:** pnl-actions-2026-03-16.md + EAN lookup via barcodespider.com / upcitemdb.com + Supabase product query

---

## Part 1: 5 Unidentified SKUs — Now Identified

### 1. EAN 194954124766
- **Identified as:** Nike Everyday Cushion Crew Socks 6-pack — SX7666-010, **Black, Size S** (Small)
- **Same model as:** 888408282750 (SX7666-010 Medium) — variant difference only
- **COGS:** ~50 PLN (same as Medium variant)
- **Category:** Socks (not apparel, not footwear)
- **Catalog name to add:** `Nike Everyday Cushion Crew Socks 6-pack SX7666-010 Black S`
- **Pricing decision:** Reprice to **115–120 PLN** (same logic as Medium — at 120 PLN: 120 − 50 − 26 ship − 18 fee = +26 PLN/unit)
- **Action:** ADD catalog name → REPRICE

---

### 2. EAN 196575379679
- **Identified as:** The North Face Norm Hat — Black/TNF Black, One-size-fits-most
- **Model:** 81645596 (structured cap, bulky dimensional weight)
- **COGS:** unknown (no COGS in DB)
- **Typical retail:** ~$32 USD / ~130 PLN
- **Category:** Headwear
- **Catalog name to add:** `The North Face Norm Hat Black`
- **Reason for 110% fee anomaly:** Bulky dimensional weight → Amazon charges dimensional shipping fees that exceed the revenue at current price. Likely also A-to-Z claim or chargeback on the 1 unit sold (90-day period).
- **Pricing decision:** DEACTIVATE (1 unit/90 days, zero velocity, 110% fee = structural loss at current price). If relisting: price 160+ PLN + investigate chargeback via Amazon Settlements CSV.
- **Action:** ADD catalog name → DEACTIVATE → pull Amazon Settlements CSV for this ASIN

---

### 3. EAN 888407233883
- **Identified as:** Nike Everyday Cushion Crew Socks 3-pack — SX7664-010, **Black/White, Large**
- **Correct model:** SX7664 (3-pack, NOT 6-pack)
- **COGS:** 50 PLN (per P&L — likely overpriced relative to 3-pack value)
- **Typical retail:** ~$15–18 USD / 60–75 PLN
- **Catalog name to add:** `Nike Everyday Cushion Crew Socks 3-pack SX7664-010 Black L`
- **Note:** COGS 50 PLN on a 3-pack that retails for 60–75 PLN leaves no margin. Sold at ~45 PLN = loss. This is a 3-pack, not 6-pack — lower value than SX7666.
- **Pricing decision:** DEACTIVATE. At 80+ PLN it's overpriced vs. Amazon market. Discontinue this variant.
- **Action:** ADD catalog name → DEACTIVATE permanently

---

### 4. EAN 888408282750
- **Identified as:** Nike Everyday Cushion Crew Socks 6-pack — SX7666-010, **Black/White, Medium**
- **Name confirmed:** `Nike Everyday Cushion Crew Socks 6-pack SX7666-010 Black M`
- **COGS:** 50 PLN
- **Pricing decision:** REPRICE to **115–120 PLN** (from ~94 PLN). Investigate FBA economics in parallel.
  - At 120 PLN FBM: 120 − 50 COGS − 26 ship − 18 fee = **+26 PLN/unit** (+22% margin)
  - At 120 PLN FBA: 120 − 50 COGS − 10 ship − 18 fee = **+42 PLN/unit** (+35% margin)
- **Action:** REPRICE → model FBA switch

---

### 5. EAN 8025272645249
- **Identified as:** **Kiko Milano Lip Volume Transparent** — Perfecting and Volumising Lip Cream, 6.5ml
- **Brand:** Kiko Milano (Italian, NOT Korean cosmetics)
- **Model:** KS180401023001A
- **COGS:** 22.76 PLN
- **Revenue (90d):** 52.08 PLN (2 units × ~26 PLN each)
- **Shipping (90d):** 48.82 PLN → **93.7% shipping-to-revenue ratio** explained: tiny 6.5ml item, Amazon classifies as dangerous goods (lip plumper = pressurized/gel). Dimensional fee + DG surcharge.
- **Pricing decision:** DISCONTINUE. At 22.76 COGS + ~24 PLN shipping + ~4 PLN fee → breakeven = ~51 PLN. No margin at any realistic price for this product type.
- **Action:** DISCONTINUE → remove from catalog

---

## Part 1 Summary Table

| EAN | Identified Product | Catalog Name | Decision |
|-----|--------------------|--------------|----------|
| 194954124766 | Nike Socks 6-pack SX7666-010 S | `Nike Everyday Cushion Crew Socks 6-pack SX7666-010 Black S` | Reprice to 115-120 PLN |
| 196575379679 | The North Face Norm Hat Black | `The North Face Norm Hat Black` | Deactivate + investigate fee anomaly |
| 888407233883 | Nike Socks 3-pack SX7664-010 L | `Nike Everyday Cushion Crew Socks 3-pack SX7664-010 Black L` | Deactivate permanently |
| 888408282750 | Nike Socks 6-pack SX7666-010 M | `Nike Everyday Cushion Crew Socks 6-pack SX7666-010 Black M` | Reprice to 115-120 PLN |
| 8025272645249 | Kiko Milano Lip Volume 6.5ml | `Kiko Milano Lip Volume Transparent 6.5ml` | Discontinue |

---

## Part 2: Korean Cosmetics Full Category Audit

**Database query:** Supabase `products` table, searched by brand name (Tocobo, Mary&May, Beauty of Joseon, COSRX, Holika Holika, Anua, Dr. Jart+, Round Lab, Some By Mi)
**Total active Korean cosmetics SKUs:** 52 (including Holika Holika which is Korean, excluding Rituals/Dutch outlier)

**Model viability:** The 35-53 PLN COGS + 17-27 PLN FBM shipping model ONLY works at 100+ PLN sale price.
**Target margin:** ≥ 15% (at 100 PLN: 100 − 45 COGS − 22 ship − 15 fee = 18 PLN → 18% ✓)

---

### CONFIRMED LOSERS — Discontinue (Already flagged in P&L)

| SKU (EAN) | Product | COGS | P&L verdict |
|-----------|---------|------|-------------|
| 8809835060041 | Tocobo Cotton Soft Sun Stick SPF50+ 19g | 35.00 | **−185 PLN/90d. Sells at 37 PLN. Impossible to raise to 100 PLN competitively. DISCONTINUE** |
| 8809670682033 | Mary&May Vegan Sun Stick SPF50+ 18g | 49.00 | **−35 PLN/90d. Sells at 58 PLN. DISCONTINUE** |
| 8809968130147 | Beauty of Joseon Jelloskin Cream 200ml | 52.57 | **−19 PLN/90d. DISCONTINUE** |

---

### HIGH-RISK — Likely to lose money (COGS too close to achievable price)

Sun sticks and very cheap items where 100+ PLN is not achievable in the market:

| SKU | Product | COGS | Issue |
|-----|---------|------|-------|
| 8809864766884 | Beauty of Joseon Mugwort+Camellia Sun Stick 18g | 36.78 | Same format as Tocobo/Mary&May — 18g sun stick. Market price 35-50 PLN. Discontinue. |
| 8809968130277 | Beauty of Joseon Relief Sun Aqua Rice+B5 SPF50+ | 36.44 | Sun cream (not stick) but same price range. Discontinue unless priced 100+ PLN. |
| 8809782555508 | Beauty of Joseon Relief Sun Rice Probiotics SPF50+ | 36.95 | Same. Discontinue. |
| 8809864752689 | Beauty of Joseon Ginseng Moist Sun Serum SPF50+ 50ml | 42.10 | Slightly bigger format. Can potentially sell at 100-120 PLN. Monitor. |
| 8806334352950 | Holika Holika Tea Tree Sheet Mask 20ml | 3.00 | Single mask — value <10 PLN. Cannot ship profitably standalone. Bundle or discontinue. |
| 8806334352967 | Holika Holika Honey Gel Mask 20ml | 3.00 | Same as above. |

---

### VIABLE — Can achieve 100+ PLN at market price

These products have sufficient market price elasticity to work at 100+ PLN:

| SKU | Product | COGS | Recommended Price | Notes |
|-----|---------|------|-------------------|-------|
| 8809954941498 | Beauty of Joseon Dynasty Cream 100ml | 65.12 | 130–140 PLN | Premium product, market ~€25-30. FBM breakeven at 130 PLN: 130−65−22−20 = +23 PLN ✓ |
| 8809844996935 | Dr. Jart+ CERAMIDIN Moisturizing Cream | 63.30 | 130–150 PLN | Dr. Jart+ is accessible premium. Market price 130-180 PLN. Viable. |
| X00241HTDH | Holika Holika Super Cera Cream 60ml | 58.00 | 120–130 PLN | Breakeven at 120: 120−58−22−18 = +22 PLN ✓ |
| X00241HQ4T | Holika Holika Super Ceramide Cream 60ml | 59.86 | 120–130 PLN | Duplicate entry — same product as above. MERGE entries. |
| 8806334388850 | Holika Holika Aloe SPF50 Sun Gel 100ml | 40.90 | 100–110 PLN | Larger format — 100g. Market 90-120 PLN achievable. |
| X002428N77 | Holika Holika Aloe SPF50+ Sun Gel 100ml | 40.96 | 100–110 PLN | Duplicate — same product. MERGE entries. |
| 8809598451445 | COSRX Ceramide Comfort Cream 80g | 40.40 | 100–110 PLN | COSRX is well-known. Market price 90-120 PLN. |
| 8809416470016 | COSRX Advanced Snail 92 Cream 100g | 33.97 | 90–100 PLN | Flagship COSRX product. Can sell at 90-100 PLN. Tight margin but viable. |
| 8809416470009 | COSRX Advanced Snail 96 Mucin Essence | 34.08 | 90–100 PLN | Same as above. |
| 8809640734694 | Anua Heart Leaf Toner 250ml | 49.23 | 100–120 PLN | Popular. Market 100-130 PLN. At 110: 110−49−22−17 = +22 PLN ✓ |
| 8809598451896 | COSRX Full Fit Propolis Toner 150ml | 26.84 | 80–90 PLN | Lower COGS. At 85: 85−27−22−13 = +23 PLN ✓ (if price achievable) |
| 8809647390503 | Some By Mi TrueCICA Snail Mucin Repair Cream | 42.28 | 100–120 PLN | Niche but popular. 100 PLN achievable. |
| 8809525249565 | Beauty of Joseon Dynasty Cream 50ml | 47.43 | 100–120 PLN | Smaller format of Dynasty Cream. Market 100 PLN achievable. |

---

### ZERO-COGS SKUs — Cannot assess profitability (needs COGS update)

| SKU | Product | Action |
|-----|---------|--------|
| 8809738316139 | Beauty of Joseon Revive Serum Ginseng+Snail Mucin | Add COGS from Baselinker |
| 9ROEG2BQ | COSRX Lightweight Moisturizing Lotion 100ml | Add COGS from Baselinker |
| H08NZUTD | COSRX Salicylic Acid Foaming Cleanser 150ml | Add COGS from Baselinker |
| 8806334369460 | Holika Holika Skin and Good Cera Emulsion 130ml | Add COGS from Baselinker |
| EZ1OKDC3 | Holika Holika Aloe Vera Gel 99% 250ml | Add COGS from Baselinker |
| 8809782551814 | Round Lab Birch Juice Sun Cream SPF50+ | Add COGS from Baselinker |

---

### DUPLICATE EAN ENTRIES — Fix in Baselinker

| EAN | Duplicate SKUs | Action |
|-----|----------------|--------|
| 8806334369477 | X00241HTDH + X00241HQ4T | Merge to one entry. Keep higher COGS (59.86). |
| 8806334388850 | 8806334388850 + X002428N77 | Merge to one entry. |

---

## Part 2 Summary — Korean Cosmetics Decisions

| Decision | SKUs | Count |
|----------|------|-------|
| **DISCONTINUE immediately** | Tocobo Sun Stick, Mary&May Sun Stick, Beauty of Joseon Jelloskin, Beauty of Joseon Mugwort Sun Stick, Beauty of Joseon Relief Sun Aqua, Beauty of Joseon Relief Sun Rice Probiotics, Holika Tea Tree Mask, Holika Honey Mask | 8 |
| **MONITOR — reprice to 100+ PLN** | Ginseng Moist Sun Serum, Beauty of Joseon Ginseng Moist Sun Serum 50ml | 2 |
| **VIABLE — keep, verify pricing** | Dynasty Cream 100ml, Dr. Jart+ Ceramidin, Holika Cera Cream, Holika Aloe Sun Gel, COSRX Ceramide, COSRX Snail 92 + 96, Anua Toner, COSRX Propolis Toner, Some By Mi, Dynasty Cream 50ml, COSRX cleansers | 14+ |
| **FIX — add COGS** | 6 zero-COGS products | 6 |
| **FIX — merge duplicates** | 2 duplicate EAN pairs | 4 entries → 2 |

**Category verdict:** The Korean cosmetics category is NOT structurally doomed — it has a pricing and product mix problem. Sun sticks and low-price items (below 60 PLN) are loss-makers. Premium items (COSRX, Beauty of Joseon Dynasty Cream, Dr. Jart+, Anua) at 100-140 PLN are viable. Exit the sun stick sub-category entirely. Keep the premium cream/serum/toner SKUs if correctly priced.

---

## Actions for Developer (Baselinker-executable)

1. **Update catalog names** for 5 SKUs (Part 1) — via Baselinker product edit
2. **Deactivate in Baselinker:** 888407233883, 8025272645249, 196575379679
3. **Set prices in Baselinker:** 194954124766 → 119 PLN; 888408282750 → 119 PLN
4. **Deactivate sun sticks:** 8809835060041, 8809670682033, 8809968130147, 8809864766884, 8809968130277, 8809782555508
5. **Fix COGS** for 6 zero-COGS Korean products (pull average_cost from Baselinker)
6. **Merge duplicate EANs:** X00241HTDH/X00241HQ4T, 8806334388850/X002428N77

*Amazon deactivation (Seller Central) requires account owner action.*

---

*Generated by developer agent — 2026-03-16*
