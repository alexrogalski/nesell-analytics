#!/bin/bash
# nesell-analytics daily ETL sync
cd /Users/alexanderrogalski/nesell-analytics

# Load environment variables (launchd doesn't source shell profiles)
set -a
source /Users/alexanderrogalski/nesell-analytics/.env 2>/dev/null
source /Users/alexanderrogalski/.keys/baselinker.env 2>/dev/null
set +a

# Full daily sync: orders, fees, reports, data, aggregate
PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.11 -m etl.run --days 7 >> /tmp/nesell-etl.log 2>&1
echo "------- $(date) -------" >> /tmp/nesell-etl.log
