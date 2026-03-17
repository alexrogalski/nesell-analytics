# P&L Action Decisions — 2026-03-16

**Business Analyst:** Reviewed pnl-analysis-2026-03-16.md
**Decision date:** 2026-03-16
**Expected monthly recovery:** ~379 PLN (immediate) + ~776 PLN (strategic, SX7666 FBA)

---

## Tier 1: DEACTIVATE IMMEDIATELY (COGS > price or structural loss)

### ✅ DECISION: Deactivate

| # | SKU | Action | Reason | Expected +PLN/month |
|---|-----|--------|--------|---------------------|
| 1 | **194502876079** Nike Dunk Low Retro 44.5 | Deactivate Amazon.de **only** → list Allegro at 499 PLN | Every Amazon unit loses ~99 PLN. Allegro viable: 499 − 379 COGS − 50 fee − 15 ship = +55 PLN | +133 |
| 2 | **8809835060041** Tocobo Cotton Soft Sun Stick | Discontinue permanently | Sell 37 PLN, ship 17 PLN — breakeven requires 76% price increase, not competitive | +62 |
| 3 | **194954229836** Nike Force 1 LE PS r.28 | Deactivate NOW. Reprice at 280+ PLN before relisting OR liquidate via Vinted/OLX >200 PLN | COGS 200 PLN > sell 165 PLN — structural pricing error | +22 |
| 4 | **888407233883** Unknown Nike apparel | Deactivate + fill catalog name. Only relist if price ≥ 80 PLN | COGS 50 PLN > sell 45 PLN. Blank name = imported without pricing review | +8 |
| 5 | **8809670682033** Mary&May Vegan Sun Stick | Discontinue permanently | Same structural problem as Tocobo: 58 PLN sell + 27 PLN shipping → breakeven at 100 PLN (+72%) | +12 |

**Tier 1 total: +237 PLN/month**

---

## Tier 2: DEACTIVATE + INVESTIGATE (fee anomalies)

These SKUs show mathematically impossible fee rates (60–110% of revenue), suggesting A-to-Z claims, chargebacks, or Baselinker data bugs.

| # | SKU | Action | Investigation needed |
|---|-----|--------|---------------------|
| 6 | **L_socks_SX7666-100_888408282804** | Deactivate (Baselinker duplicate of SX7666-010-L) | Pull Amazon Settlements CSV — identify the 84.7% fee source. If chargeback, file counter-claim if eligible |
| 7 | **196575379679** Unmapped product | Deactivate (1 unit/90 days, zero velocity) | Same: 110% fee anomaly. Cross-reference Amazon Settlements for this ASIN. Identify product first |
| 8 | **brak1** Nike Socks orphan SKU | Fix mapping to SX7667-100 in Baselinker; reprice to 100+ PLN | 61.7% fee anomaly likely same chargeback pattern. Fix catalog entry before relisting |

**Estimated +52 PLN/month after cleanup**

---

## Tier 3: REPRICE (viable products, wrong price)

| # | SKU | Action | Economics |
|---|-----|--------|-----------|
| 9 | **888408282750** Nike Socks 6-pack | Raise price to **115–120 PLN** (from ~94 PLN). In parallel: model FBA economics | At 120 PLN: 120 − 50 COGS − 26 ship − 18 fee = +26 PLN/unit (+22% margin). FBA would drop shipping 26→10 PLN → 15%+ margin without price change |
| 10 | **194954124766** Unmapped product | **First: identify EAN in Baselinker/Amazon catalog.** If Nike mid-value: raise to 140+ PLN. If commodity: discontinue | 37.2% FBA fee + 50 COGS + 28 ship requires 140+ PLN to break even |

---

## Tier 4: SECONDARY (ranks 11–15, included for completeness)

| SKU | Decision |
|-----|----------|
| XL_sx7664-100_888407233890_t4 | Deactivate (Baselinker duplicate, fee anomaly) |
| X0021CXV8L Estée Lauder Pleasures | Raise price to 130+ PLN |
| DC4244-010 Nike Backpack | Raise price to 130+ PLN (from ~89 PLN) |
| 8809968130147 Beauty of Joseon | Discontinue (same Korean cosmetics problem) |
| 8025272645249 Unknown EAN | Identify product + investigate 93.7% shipping ratio |

---

## Strategic Opportunity (highest-leverage)

| SKU | Action | Potential |
|-----|--------|-----------|
| **SX7666-010-M + SX7666-010-L** (341 units/90d) | Model FBA: shipping drops from ~25 PLN → ~8–12 PLN/unit. If viable, switch to FBA | **+676 PLN/month** |
| **KR-V8K9-5QUG** (33 units, 3.7% margin) | Investigate + raise price | +100 PLN/month |

---

## Implementation Priority Queue

**This week (urgent):**
1. Deactivate Amazon.de listing for 194502876079 (Nike Dunk Low 44.5) → create Allegro listing at 499 PLN
2. Deactivate/remove: 8809835060041, 194954229836, 888407233883, 8809670682033
3. Deactivate Baselinker duplicates: L_socks_SX7666-100_888408282804, XL_sx7664-100_888407233890_t4
4. Pull Amazon Settlements CSV for fee-anomaly SKUs

**This month:**
5. Raise 888408282750 Nike Socks to 115–120 PLN; monitor conversion
6. Identify 194954124766 + set correct price
7. FBA economics model for SX7666-010-M/L
8. Audit full Korean cosmetics category

---

*Actions requiring Amazon Seller Central access must be executed by account owner.*
*Baselinker changes (SKU mapping, price updates) can be handled by developer agent.*
