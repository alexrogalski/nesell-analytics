# P&L Deep Dive — Top 10 Products Losing Money
Date: 2026-03-16
Analysis Period: Last 90 days (2025-12-17 to 2026-03-16)
Data Source: Supabase `daily_metrics` — live query executed 2026-03-16

## Executive Summary

Portfolio overall performance: **1 906 units sold, 214 801 PLN revenue, 64 238 PLN net profit (29.9% margin)**.

However, within that healthy average hides a tail of SKUs actively destroying value. The **top 10 worst products lost a combined 1 131 PLN** in net profit over 90 days. An additional 5 products (ranks 11–15) lost a further 102 PLN. Most losses stem from four root causes:

1. **COGS exceeds sell price** — Nike Dunk Low (194502876079) and Nike Force 1 kids (194954229836) are priced below landed cost.
2. **Excessive per-unit shipping on low-value items** — Socks (~94 PLN avg. price) with ~26 PLN/unit FBM seller shipping makes unit economics impossible.
3. **Fee data anomalies** — Three SKUs show fee rates of 60–110% of revenue, indicating chargebacks, A-to-Z claims, or data import bugs.
4. **Korean cosmetics structural problem** — Low sell price (~37–60 PLN) combined with ~17–27 PLN per-unit shipping creates irreversible losses.

Additionally, **two high-volume SKUs (SX7666-010-M, SX7666-010-L)** together represent 341 units and ~33 264 PLN revenue but only 6–7% margin — the largest absolute PLN opportunity in the portfolio.

---

## Methodology

- **Source:** Supabase `daily_metrics` table, 540 rows aggregated by SKU
- **Revenue:** `revenue_pln` — converted to PLN at NBP FX rates on order date
- **COGS:** `cogs` — unit cost from `products.cost_pln` × units sold
- **Fees:** `platform_fees` — real Amazon Finances data where available; fallback rate 34.73% (FBA EUR), 15.45% (FBM), 10% (Allegro), 15% (Empik)
- **Shipping:** `shipping_cost` — actual seller DPD invoice cost where available; buyer delivery price as fallback
- **Net Profit (REAL PROFIT):** `gross_profit` = revenue_pln − COGS − fees − shipping
- **Margin:** gross_profit / revenue_pln × 100
- **Filter:** real products only (excludes `temu_discount`, `rabat` system lines); min 1 unit sold, min 10 PLN revenue
- **Primary sort:** absolute net profit loss (PLN)

---

## Top 10 Products Losing Money

| # | SKU | Product Name | Units | Revenue (PLN) | COGS (PLN) | Fees (PLN) | Shipping (PLN) | **Net Profit (PLN)** | **Margin %** |
|---|-----|-------------|-------|--------------|-----------|-----------|---------------|---------------------|-------------|
| 1 | 194502876079 | Nike Dunk Low Retro 44.5 EU | 4 | 1 713.26 | 1 515.00 | 386.62 | 208.62 | **−396.98** | **−23.2%** |
| 2 | 8809835060041 | Tocobo Cotton Soft Sun Stick SPF50+ 19g | 9 | 332.50 | 315.00 | 51.47 | 151.85 | **−185.82** | **−55.9%** |
| 3 | 888408282750 | Nike Socks 6-pack (name unmapped) | 19 | 1 782.65 | 950.00 | 499.09 | 503.72 | **−170.16** | **−9.5%** |
| 4 | 194954124766 | Product 194954124766 (unmapped) | 6 | 587.88 | 300.00 | 218.37 | 167.14 | **−97.63** | **−16.6%** |
| 5 | L_socks_SX7666-100_888408282804 | Nike Everyday Cushioned 6-pack (duplicate SKU) | 2 | 215.13 | 100.00 | 182.18 | 0.00 | **−67.05** | **−31.2%** |
| 6 | 194954229836 | Buty Nike Force 1 LE PS DH2925-111 r.28 | 1 | 164.82 | 200.00 | 30.48 | 0.00 | **−65.66** | **−39.8%** |
| 7 | 196575379679 | Product 196575379679 (unmapped) | 1 | 90.22 | 42.27 | 99.40 | 0.00 | **−51.45** | **−57.0%** |
| 8 | brak1 | Nike Skarpetki SX7667-100 (orphan SKU) | 1 | 55.87 | 32.50 | 34.48 | 26.62 | **−37.73** | **−67.5%** |
| 9 | 8809670682033 | Mary&May Vegan Peptide Bakuchiol Sun Stick 18g | 1 | 58.11 | 49.00 | 17.01 | 26.60 | **−34.50** | **−59.4%** |
| 10 | 888407233883 | Unknown Nike apparel (unmapped) | 2 | 90.00 | 100.00 | 13.90 | 0.00 | **−23.90** | **−26.6%** |

**Combined loss from top 10: −1 130.88 PLN over 90 days (−376.96 PLN/month)**

---

## Recommendations Per Product

### 1. Nike Dunk Low Retro 44.5 EU (194502876079) — Loss: −396.98 PLN
**Recommendation: Deactivate Amazon.de → test Allegro at 499+ PLN**

At 428 PLN/unit avg. sell price, cost breakdown is 379 PLN COGS + 97 PLN fees + 52 PLN shipping = 528 PLN total → **every unit sold loses ~99 PLN**. A 36% price increase to 580–600 PLN is unrealistic on Amazon.de where Nike sneakers are highly competitive. However, Allegro's 10% fee + FBM ~15 PLN shipping makes a 499 PLN listing potentially viable (499 − 379 − 50 − 15 = +55 PLN, 11% margin). Deactivate Amazon.de immediately.

### 2. Tocobo Cotton Soft Sun Stick SPF50+ (8809835060041) — Loss: −185.82 PLN
**Recommendation: Discontinue immediately**

Sell price 37 PLN barely covers COGS 35 PLN. Per-unit shipping averaged 17 PLN — at 9 units sold, shipping alone was 152 PLN against 332 PLN revenue. To break even at current cost structure, price would need to reach 60–65 PLN (+76%), making the listing non-competitive. Zero prospect for profitable operation. Remove listing.

### 3. Nike Socks 6-pack 888408282750 — Loss: −170.16 PLN
**Recommendation: Raise price to 115–120 PLN AND investigate FBA model**

19 units sold with 26.51 PLN/unit shipping cost is the core problem. Revenue 94 PLN − COGS 50 PLN − shipping 26 PLN − fees 26 PLN = −8 PLN/unit. Two paths to profitability: (a) raise price to 115–120 PLN (+23%) to reach ~5–7% margin at current shipping cost, or (b) investigate FBA — prepaid inventory in Amazon warehouse would cut per-unit shipping from ~26 PLN to ~8–12 PLN, potentially achieving 15%+ margin without a price increase. Note: sister SKUs SX7666-010-M/L sell at similar prices with similarly thin margins, indicating a systemic category problem.

### 4. Product 194954124766 (unnamed) — Loss: −97.63 PLN
**Recommendation: Identify product → raise price to 140+ PLN**

37.2% fee rate (FBA) on top of 50 PLN COGS and 28 PLN shipping makes breakeven require ~140 PLN (current avg: 98 PLN). Priority zero: fill in the catalog name for this EAN — without knowing what it is, no informed pricing or sourcing decision is possible. If it's a mid-value Nike item, 140 PLN may be achievable; if it's a commodity, discontinue.

### 5. L_socks_SX7666-100_888408282804 (duplicate SKU) — Loss: −67.05 PLN
**Recommendation: Deactivate this listing; investigate 84.7% fee anomaly**

The fee rate of 84.7% of revenue is mathematically impossible under any standard Amazon fee structure (max ~35% FBA). This strongly indicates an A-to-Z claim refund or chargeback being allocated here. The compound SKU name is a Baselinker-generated duplicate of the canonical `SX7666-010` family. Action: pull Amazon Settlements CSV for this SKU's order dates and identify the fee source; then deactivate this listing in favor of SX7666-010-L.

### 6. Buty Nike Force 1 LE PS r.28 (194954229836) — Loss: −65.66 PLN
**Recommendation: Discontinue immediately (COGS > sell price)**

COGS is 200 PLN but sold for 165 PLN — fundamental pricing error. Every unit sold loses money before any fees are counted. If remaining inventory exists, price at 280+ PLN minimum (200 COGS + 31 fees + ~15 shipping = 246 PLN breakeven, 280 PLN = 12% margin). Alternatively liquidate via Vinted or OLX at >200 PLN.

### 7. Product 196575379679 (unnamed) — Loss: −51.45 PLN
**Recommendation: Investigate fee data; deactivate listing**

Fees at 110% of revenue is impossible. Same anomaly as #5 — likely A-to-Z claim or chargeback. Only 1 unit sold in 90 days = zero velocity. Deactivate pending investigation; cross-reference with Amazon Settlements CSV for this ASIN.

### 8. brak1 (orphan SKU) — Loss: −37.73 PLN
**Recommendation: Fix SKU mapping; reprice to 100+ PLN**

SKU "brak1" is a Baselinker placeholder (Polish "brak" = "missing"). This order was processed without a valid catalog entry, resulting in a 61.7% effective fee rate (also anomalous — likely same chargeback pattern). Map the order to the correct SKU (likely SX7667-100 Nike socks), fix pricing to 100+ PLN minimum (32.50 COGS + 10 PLN fee + 15 PLN ship = 58 PLN breakeven; 100 PLN = 42% margin).

### 9. Mary&May Sun Stick SPF50+ (8809670682033) — Loss: −34.50 PLN
**Recommendation: Discontinue**

Identical structural problem to Tocobo (#2): low-price Korean cosmetic (58 PLN sell) with 27 PLN per-unit shipping. Breakeven requires ~100 PLN (+72%). Zero repeat velocity (1 unit in 90 days). Remove listing and consider the entire Korean cosmetics portfolio for the same analysis.

### 10. Unknown SKU 888407233883 — Loss: −23.90 PLN
**Recommendation: Discontinue + fix catalog entry**

COGS 50 PLN > Revenue 45 PLN — sold at a guaranteed loss. Blank catalog name means this was auto-imported without pricing review. Deactivate, update catalog with product name, and reprice at 80+ PLN before relisting if inventory remains.

---

## Additional Loss Products (Ranks 11–15)

These products also lost money over the period but with smaller absolute impact:

| # | SKU | Product Name | Units | Revenue | COGS | Fees | Shipping | Net Profit | Margin |
|---|-----|-------------|-------|---------|------|------|----------|------------|--------|
| 11 | XL_sx7664-100_888407233890_t4 | Nike Everyday Cushioned 6-pack (XL duplicate) | 5 | 356.92 | 250.00 | 130.57 | 0.00 | −23.65 | −6.6% |
| 12 | X0021CXV8L | Estée Lauder Pleasures 30ml EDP | 1 | 101.00 | 65.50 | 29.89 | 25.57 | −19.96 | −19.8% |
| 13 | DC4244-010 | Plecak Nike Heritage Backpack czarny | 4 | 354.95 | 300.00 | 51.20 | 23.48 | −19.73 | −5.6% |
| 14 | 8809968130147 | Beauty of Joseon Jelloskin Cream 200ml | 1 | 59.99 | 52.57 | 16.40 | 10.49 | −19.47 | −32.5% |
| 15 | 8025272645249 | Unknown product (EAN unmapped) | 2 | 52.08 | 0.00 | 22.20 | 48.82 | −18.94 | −36.4% |

**Notes:**
- Rank 11 (XL_sx7664-100_888407233890_t4): Another Baselinker duplicate SKU, same root cause as rank 5 — fee anomaly at 36.6% on socks with COGS ≥ price.
- Rank 12 (X0021CXV8L — Estée Lauder): Fragrance category. COGS 65.50 PLN, shipping 25.57 PLN. Sold for 101 PLN — marginal at best. Raise price to 130+ PLN.
- Rank 13 (DC4244-010 — Nike Backpack): COGS 75 PLN/unit × 4 = 300 PLN vs 354.95 revenue. 14.4% fee + 23.48 shipping = just barely losing. Raise price to 130+ PLN (from current ~89 PLN avg).
- Rank 14 (Beauty of Joseon): Same Korean cosmetics problem — low sell price + shipping = structural loss.
- Rank 15 (8025272645249): COGS = 0 in database (unmapped product), but 48.82 PLN shipping on 52.08 PLN revenue is catastrophic — 93.7% shipping ratio. Likely a very heavy/bulky item shipped FBM. Requires immediate identification and listing review.

---

## Recommendations Summary

| Product | Action | Expected Monthly Impact |
|---------|--------|------------------------|
| 194502876079 — Nike Dunk Low | Deactivate Amazon.de; test Allegro at 499 PLN | +133 PLN/month |
| 8809835060041 — Tocobo Sun Stick | Deactivate immediately | +62 PLN/month |
| 194954229836 — Nike Force 1 Kids | Deactivate immediately (COGS > price) | +22 PLN/month |
| 888407233883 — Unknown Nike | Deactivate immediately (COGS > price) | +8 PLN/month |
| 888408282750 — Nike Socks 6-pack | Raise price to 115–120 PLN OR investigate FBA | +57 PLN/month |
| 8809670682033 — Mary&May Sun Stick | Deactivate | +12 PLN/month |
| 196575379679 — Unmapped product | Investigate fee anomaly; deactivate if unresolved | +17 PLN/month |
| L_socks_SX7666-100_888408282804 | Investigate fee anomaly; merge into canonical SKU | +22 PLN/month |
| brak1 — Orphan SKU | Fix SKU mapping; reprice to 100+ PLN | +13 PLN/month |
| 194954124766 — Unmapped product | Identify product; raise price to 140+ PLN | +33 PLN/month |
| X0021CXV8L — Estée Lauder Pleasures | Raise price to 130+ PLN | +7 PLN/month |
| DC4244-010 — Nike Backpack | Raise price to 130+ PLN | +7 PLN/month |
| 8025272645249 — Unknown EAN | Identify product; review FBM shipping cost | +6 PLN/month |
| **SX7666-010-M** *(strategic)* | **Switch to FBA or raise price to 105+ PLN** | **+295 PLN/month potential** |
| **SX7666-010-L** *(strategic)* | **Switch to FBA or raise price to 105+ PLN** | **+381 PLN/month potential** |
| **KR-V8K9-5QUG** *(strategic)* | **Investigate 3.7% margin on 33 units; raise price** | **+100+ PLN/month potential** |

**Total immediate recoverable loss (top 10 deactivations/repricing): ~379 PLN/month**
**Total strategic opportunity (SX7666 FBA switch + KR-V8K9-5QUG): ~776 PLN/month additional**

---

## Next Steps

### This Week (Days 1–7)
1. **Deactivate 4 listings immediately**: 194502876079 (Amazon.de only), 8809835060041, 194954229836, 888407233883 — combined projected savings ~225 PLN/month.
2. **Pull Amazon Settlements CSV** for fee-anomaly SKUs: `L_socks_SX7666-100_888408282804`, `196575379679`, `brak1`, `XL_sx7664-100_888407233890_t4` — identify whether fees are A-to-Z claims, chargebacks, or data errors.
3. **Fill catalog blanks** for EAN-named SKUs: 194954124766, 196575379679, 888407233883, 888408282750, 8025272645249 — required for informed decisions.

### This Month (Days 8–30)
4. **Raise prices on 888408282750** (Nike socks) from ~94 PLN to 115–120 PLN. Monitor conversion for 2 weeks.
5. **Model FBA economics for SX7666-010-M/L** — if FBA per-unit shipping drops from ~25 PLN to ~10 PLN, margin jumps from 7% to 16–18%. Highest-leverage opportunity: 341 units/quarter, ~33 264 PLN revenue.
6. **Test Allegro for Nike Dunk Low** (194502876079) at 499 PLN — Allegro 10% fee + ~15 PLN FBM shipping is structurally viable vs Amazon 35% + 52 PLN shipping.
7. **Korean cosmetics audit** — both sun sticks + Beauty of Joseon are loss-making. Review entire Korean cosmetics category: the ~35–53 PLN COGS + high per-unit shipping model only works at 100+ PLN sell price.
8. **Raise DC4244-010 (Nike Backpack) price** from ~89 PLN to 130+ PLN — COGS 75 PLN + ~13 PLN fees + ~6 PLN shipping = 94 PLN breakeven; 130 PLN = 27% margin.

### Structural (Month 2+)
9. **Shipping cost optimization** — seller FBM shipping ~25–30 PLN/unit for socks. Investigate InPost vs DPD contract rates, or Allegro One Shipping for FBM orders.
10. **COGS coverage gap** — 215/848 products (25%) have cost_pln = 0/NULL. These appear as 100% margin in P&L. Prioritize filling COGS for high-revenue SKUs.
11. **Fee anomaly detection automation** — add alert when `platform_fees / revenue_pln > 0.50` to catch chargebacks/A-to-Z claims in real time.
12. **Estée Lauder / fragrance category review** — perfumes with 65 PLN COGS + 25 PLN shipping require 110+ PLN list price minimum. Check all fragrance SKUs.

---

## Notable Finding: Fee Data Quality Issues

Four SKUs show anomalous platform fee rates (36–110% of revenue): `L_socks_SX7666-100_888408282804`, `196575379679`, `brak1`, `XL_sx7664-100_888407233890_t4`. This pattern suggests:
- Amazon A-to-Z claim refund costs being allocated to platform_fee
- Advertising spend being misclassified as platform fee
- Data import bug from Baselinker fee fields

**Recommended fix:** Cross-reference `orders.platform_fee` values against Amazon Settlements CSV for these order dates. If confirmed as data errors, correct `daily_metrics` and exclude from P&L.

---

## Portfolio Context

| Metric | Value |
|--------|-------|
| Analysis period | 2025-12-17 to 2026-03-16 (90 days) |
| Total SKUs analyzed | 155 (with ≥1 unit AND ≥10 PLN revenue) |
| Total units sold | 1 906 |
| Total revenue | 214 800.95 PLN |
| Total COGS | 98 606.84 PLN |
| Total platform fees | 38 801.08 PLN |
| Total shipping costs | 13 155.13 PLN |
| Total net profit | 64 237.90 PLN |
| **Overall margin** | **29.9%** |
| SKUs with negative profit | 15 (real products) |
| System discount lines excluded | temu_discount, rabat |

---

## Data Sources

| Source | Details |
|--------|---------|
| **Primary DB** | Supabase `daily_metrics` table, 540 rows — queried live 2026-03-16 |
| **Revenue** | `revenue_pln` — NBP FX rates applied at order date |
| **COGS** | `products.cost_pln` × `units_sold` (coverage: ~633/848 products) |
| **Platform fees** | Real Amazon Finances data where available; fallback rates: FBA 34.73%, FBM 15.45%, Allegro 10%, Empik 15%, Temu 0% |
| **Shipping** | Actual DPD seller invoice costs from `orders.seller_shipping_cost_pln`; buyer delivery price as fallback |
| **Products catalog** | Supabase `products` table (848 products; ~215 without COGS) |
| **Database connection** | PostgREST API at `igyceeknivjdbvjqxcdi.supabase.co` |

**Data caveats:**
- COGS coverage gap: ~215/848 products (25%) have cost_pln = 0 or NULL. These appear as 100% gross margin — verify high-margin SKUs without COGS filled in.
- FX rates: NBP daily rates used. Weekend/holiday orders use nearest available rate.
- Amazon FBA fee rate (34.73%) is a calculated average from real orders — individual SKU rates may vary by size/weight tier.
- `temu_discount` and `rabat` are system-level discount/coupon lines (negative revenue entries), not real product SKUs — excluded from ranking.
