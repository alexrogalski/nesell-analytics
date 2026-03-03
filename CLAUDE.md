# nesell-analytics

E-commerce profit tracking for **nesell** brand. Multi-platform seller (Amazon 8 EU markets, Allegro, Temu, Empik) with Baselinker as central hub.

## Stack
- **Database**: Supabase (PostgREST API, no direct PostgreSQL — IPv6 issue from Mac)
- **ETL**: Python 3.11 modules in `etl/`
- **Dashboard**: Streamlit + Plotly, deployed on Streamlit Cloud
- **Repo**: GitHub `alexrogalski/nesell-analytics` (private)

## Project Structure
```
├── .env                    # Supabase credentials (gitignored)
├── schema.sql              # DB schema (deployed to Supabase SQL Editor)
├── dashboard.py            # Streamlit dashboard (reads st.secrets on Cloud, etl/config locally)
├── requirements.txt        # requests, streamlit, plotly, pandas
├── .streamlit/config.toml  # Dark theme
└── etl/
    ├── config.py           # Loads creds from ~/.keys/, marketplace mappings
    ├── db.py               # Supabase REST API client (PostgREST upserts)
    ├── baselinker.py       # BL orders + products sync
    ├── amazon.py           # Amazon SP-API FBA-only orders (AFN filter)
    ├── fx_rates.py         # NBP API (EUR/GBP/SEK/USD → PLN)
    ├── aggregator.py       # Compute daily_metrics from orders+items+costs
    └── run.py              # CLI runner
```

## Commands
```bash
# Full ETL sync
cd ~/nesell-analytics && python3.11 -m etl.run

# Individual steps
python3.11 -m etl.run --fx          # FX rates
python3.11 -m etl.run --orders      # Baselinker orders
python3.11 -m etl.run --fba         # Amazon FBA orders
python3.11 -m etl.run --products    # Product catalog
python3.11 -m etl.run --aggregate   # Re-aggregate daily metrics
python3.11 -m etl.run --days 30     # Custom lookback

# Dashboard (local)
streamlit run dashboard.py --server.port 8510
```

## Key Technical Details
- **Supabase REST API only** — direct PostgreSQL and pooler connections fail from this Mac
- **PostgREST upsert**: `Prefer: resolution=merge-duplicates` header + `on_conflict` param, batch 500
- **Baselinker pagination**: use `date_confirmed` as cursor (NOT `log_id` — always None)
- **Amazon FBA only**: `FulfillmentChannels=AFN` — BL already has FBM orders, no duplicates
- **Amazon rate limits**: very aggressive, getOrderItems throttled after ~100 requests
- **Dashboard on Streamlit Cloud**: secrets in st.secrets (SUPABASE_URL, SUPABASE_KEY)
- Push to GitHub auto-deploys dashboard

## Database Tables
platforms (13), products (848), orders (941), order_items (1088), fx_rates (236), daily_metrics (543), cost_history, alerts

## Important Business Context
- Main warehouse: BL "test-exportivo" (inv_id=30229) — has COGS as average_cost
- Printful inventory: inv_id=52954
- Platform fees: Amazon 15.45%, Allegro 10%, Empik 15%, Temu 0%
- COGS coverage: ~633/848 products (rest show 0)
- User communicates in Polish

## Git
- Push requires token: set remote URL with token before push, remove after
- Commit convention: conventional commits (feat:, fix:, chore:)
- Don't auto-push, only on user request
- Don't kill streamlit processes on other ports — other Claude Code instances may be running them
