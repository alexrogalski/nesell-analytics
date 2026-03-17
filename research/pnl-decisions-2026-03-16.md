# P&L Action Decisions — 2026-03-16
Based on: pnl-analysis-2026-03-16.md
Decided by: business-analyst
Total recoverable: ~379 PLN/month (immediate) + ~776 PLN/month (strategic)

## Decision Matrix — 10 Losing SKUs

| # | SKU | Loss/90d | Decision | Action | Expected Impact |
|---|-----|---------|----------|--------|-----------------|
| 1 | 194502876079 — Nike Dunk Low 44.5 | −396.98 PLN | **DEACTIVATE + MIGRATE** | Deactivate Amazon.de immediately. Create Allegro listing at 499 PLN (viable: 499 − 379 COGS − 50 fee − 15 ship = +55 PLN, 11% margin) | +133 PLN/month |
| 2 | 8809835060041 — Tocobo Sun Stick | −185.82 PLN | **DISCONTINUE** | Remove listing. Structural loss: 37 PLN sell vs 52 PLN break-even. Korean cosmetics at this price tier are unviable FBM. | +62 PLN/month |
| 3 | 888408282750 — Nike Socks 6-pack | −170.16 PLN | **REPRICE** | Raise to 115–120 PLN. Current 94 PLN loses 8 PLN/unit due to 26 PLN FBM shipping. 115 PLN = ~5% margin. Strategic next: model FBA. | +57 PLN/month |
| 4 | 194954124766 — Unmapped product | −97.63 PLN | **IDENTIFY + REPRICE** | Urgent: identify what this EAN is (check Baselinker catalog). Then reprice to 140+ PLN. If commodity, discontinue. Cannot decide without knowing the product. | +33 PLN/month |
| 5 | L_socks_SX7666-100_888408282804 — Duplicate SKU | −67.05 PLN | **DEACTIVATE + INVESTIGATE** | Deactivate this duplicate Baselinker SKU — it's the same product as SX7666-010-L. Pull Amazon Settlements CSV for fee anomaly (84.7% fee rate = A-to-Z claim or chargeback). | +22 PLN/month |
| 6 | 194954229836 — Nike Force 1 r.28 | −65.66 PLN | **REPRICE OR LIQUIDATE** | COGS 200 PLN > sell price 165 PLN. If inventory on hand: reprice to 280+ PLN on all platforms. If zero inventory (already sold): mark as "do not reorder". Liquidate via Vinted/OLX at 200+ PLN if stuck. | +22 PLN/month |
| 7 | 196575379679 — Unmapped product | −51.45 PLN | **DEACTIVATE + INVESTIGATE** | Fee rate 110% of revenue = impossible. Deactivate immediately. Investigate: pull Amazon Settlements CSV for this ASIN. 1 unit/90 days = zero velocity anyway. | +17 PLN/month |
| 8 | brak1 — Orphan SKU | −37.73 PLN | **FIX SKU MAPPING** | Map "brak1" → correct SKU (likely SX7667-100). Fix in Baselinker. Reprice to 100+ PLN. Investigate 61.7% fee anomaly (likely A-to-Z claim). | +13 PLN/month |
| 9 | 8809670682033 — Mary&May Sun Stick | −34.50 PLN | **DISCONTINUE** | Identical structural problem to Tocobo (#2): 58 PLN sell vs ~100 PLN break-even. 1 unit in 90 days. Remove listing. | +12 PLN/month |
| 10 | 888407233883 — Unknown Nike | −23.90 PLN | **IDENTIFY + REPRICE** | COGS 50 PLN > Revenue 45 PLN. Fix catalog (blank name). Reprice to 80+ PLN before any future sales. If stock exists, deactivate until repriced. | +8 PLN/month |

## Rationale Summary

**4 immediate deactivations (SKUs 1, 2, 6, 9):** Either COGS > price (structural pricing error) or FBM shipping makes break-even impossible without 50–76% price increase. Non-competitive at viable price point.

**3 fee anomaly investigations (SKUs 5, 7, 8):** Fee rates of 61–110% indicate Amazon A-to-Z claims, chargebacks, or Baselinker data import errors — not real operating losses. Deactivate listings, then resolve root cause.

**2 repricing actions (SKUs 3, 10):** Viable with price increase of 20–30%. Monitor conversion rate for 2 weeks before further action.

**1 catalog gap (SKU 4):** Cannot make a final decision without knowing what this EAN maps to. Identify first, then reprice or discontinue.

## Strategic Flags (Outside Top 10)

- **SX7666-010-M/L**: 341 units/quarter at 6–7% margin. FBA model could push to 16–18%. Highest ROI opportunity in portfolio: ~776 PLN/month.
- **Korean cosmetics category**: 4 SKUs (Tocobo, Mary&May, Beauty of Joseon, 8025272645249) all losing money. FBM shipping model is structurally broken for <60 PLN items. Either raise all to 100+ PLN or exit category.
- **Nike Backpack DC4244-010**: Raise from 89 PLN to 130+ PLN. Minor fix, +7 PLN/month.
- **Estée Lauder X0021CXV8L**: Raise from 101 PLN to 130+ PLN. Minor fix, +7 PLN/month.

## Implementation Priority

### Week 1 — Immediate (5 actions)
1. Deactivate 194502876079 on Amazon.de → create Allegro listing at 499 PLN
2. Deactivate 8809835060041 (Tocobo) all platforms
3. Deactivate 8809670682033 (Mary&May) all platforms
4. Deactivate 194954229836 (Nike Force 1 r.28) → reprice to 280+ PLN if inventory exists
5. Pull Amazon Settlements CSV for SKUs: L_socks_SX7666-100_888408282804, 196575379679, brak1

### Week 2 — Repricing
6. Raise 888408282750 (Nike Socks 6-pack) to 115–120 PLN
7. Raise 888407233883 (Unknown Nike) to 80+ PLN after catalog fix
8. Deactivate L_socks_SX7666-100_888408282804 duplicate SKU

### Week 3–4 — Catalog Fixes
9. Identify EAN 194954124766 → reprice to 140+ PLN or discontinue
10. Fix brak1 SKU mapping in Baselinker → SX7667-100

### Month 2 — Strategic
11. Model FBA for SX7666-010-M/L (341 units/quarter, ~+776 PLN/month potential)
12. Raise DC4244-010 Nike Backpack to 130+ PLN
13. Raise X0021CXV8L Estée Lauder to 130+ PLN
14. Full Korean cosmetics category audit
