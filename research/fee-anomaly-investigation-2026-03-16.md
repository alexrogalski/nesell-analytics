# Fee Anomaly Investigation — 2026-03-16

## Task
3 SKUs showed impossible fee rates (60–110% of revenue) in pnl-analysis-2026-03-16.md.
Determine if real losses (A-to-Z claims) or data errors.

## Verdict: All 3 are DATA ERRORS — no A-to-Z claims found

---

## SKU 1: `L_socks_SX7666-100_888408282804` — claimed 84.7% fee rate

### Root Cause: Aggregator null unit_price bug
The aggregator computed `revenue += float(item["unit_price"] or 0) * qty`.
When `unit_price` was NULL in `order_items`, revenue → 0 but fees were still allocated.
2 rows were generated with `units_sold=0, revenue_pln=0, fees>0`:
- 2025-12-17: fees=33.72 PLN, revenue=0 (real order: 20.72 EUR, fee=8.01 EUR)
- 2026-01-02: fees=37.52 PLN, revenue=0 (real order: 25.49 EUR, fee=8.89 EUR)

### Real economics (after fix)
- 11 rows, fee rate **35.5%** (normal FBA: ~15% referral + ~19% FBA)
- Margin 16–20% on normal days
- 6 legitimate returns (not chargebacks)

### Actions taken
- `active=False` in products table (duplicate compound SKU — real product: `SX7666-010-L`)
- Stale zero-revenue rows deleted from `daily_metrics`

### Residual anomaly
- 2026-01-15: 1u, rev=107.31 PLN, fees=73.34 PLN (68.3%) — two orders aggregated to same day/sku key
  but only 1 unit price was valid. Investigate item_ids 2101+2103 in order_items.

---

## SKU 2: `196575379679` — claimed 110% fee rate (THE NORTH FACE Norm Hoed)

### Root Cause: Same aggregator null unit_price bug
2 rows with `units_sold=0, revenue_pln=0, fees>0`:
- 2026-01-07: fees=32.84 PLN (7.80 EUR × 4.21)
- 2026-01-12: fees=32.88 PLN (7.81 EUR × 4.21)

### Real economics (after fix)
- 5 rows, fee rate **37.3%** (normal FBA)
- Margin 15–19%
- 3 legitimate returns

### Note on velocity
Original task said "1 unit/90 days" — this was the corrupted daily_metrics figure.
Real data: 9 units sold (Dec 2025). **Low but not zero velocity.**
Deactivated in products table as instructed (inventory risk: 1 unit/30d in Jan 2026).

### Actions taken
- `active=False` in products table
- Stale zero-revenue rows deleted from `daily_metrics`

---

## SKU 3: `brak1` — claimed 61.7% fee rate

### Root Cause: Revenue under-count (FBM with separate shipping)
Single order (item_id=542, 2026-01-12, Amazon.IT via Baselinker/FBM):
- `unit_price` = 13.27 EUR (item only)
- `order.total_paid` = 28.71 EUR (item 13.27 + shipping 15.44 EUR)
- Aggregator uses `unit_price`, NOT `total_paid` → shipping revenue (15.44 EUR = ~65 PLN) excluded

Real economics with shipping included:
- Revenue: 28.71 EUR = 120.87 PLN
- Platform fee: 8.19 EUR = 34.48 PLN (**28.5% of total** — reasonable FBM Italy)
- Shipping cost (DPD): 26.62 PLN
- COGS: 32.50 PLN
- **Gross profit: +27.27 PLN (+22.6% margin)**

Additionally, `brak1` was an orphan SKU with no product catalog entry.

### Actions taken
- `brak1` product record updated: `active=False`, name updated to `[ORPHAN] ... — mapped to SX7667-100`
- `SX7667-100` product record created: `cost_pln=32.50, active=True`
- Single historical row left in `daily_metrics` (deactivated SKU, one-time order)

---

## Systemic Fix: Aggregator Bug (`etl/aggregator.py`)

### The Bug
`items_per_order` counted ALL items including null-price ones.
Revenue sum used `float(item["unit_price"] or 0)` → 0 for nulls.
Fee allocation used `order_fee * qty / items_per_order` → fees always allocated.
Result: fee-without-revenue rows in `daily_metrics`.

### The Fix (applied 2026-03-16)
```python
# First pass: only count items with valid unit_price
items_per_order = defaultdict(int)
for item in all_items:
    if item.get("unit_price") is not None and float(item["unit_price"] or 0) > 0:
        items_per_order[item["order_id"]] += int(item["quantity"] or 1)

# In aggregation loop: skip null-price items entirely
if item.get("unit_price") is None or float(item.get("unit_price") or 0) == 0:
    continue
```

This prevents new fee-without-revenue rows. Existing stale rows were deleted via direct API DELETE.

---

## Amazon Settlements CSV
Not downloaded — **not necessary**.
Root causes confirmed via `order_items` + `orders` table queries.
All anomalies are aggregator artifacts, not Amazon financial events (chargebacks/A-to-Z).

---

## Files Changed
- `etl/aggregator.py` — null unit_price guard
- `scripts/fix_fee_anomaly_skus.py` — deactivation + mapping script (new)
- Supabase `products` table — 3 SKUs updated
- Supabase `daily_metrics` table — 4 stale rows deleted

## Re-aggregation Result
- Before: 84.7% / 110% / 61.7% fee rates on 3 SKUs
- After: 35.5% / 37.3% fee rates (normal FBA); brak1 deactivated
