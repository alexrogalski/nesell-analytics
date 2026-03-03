#!/bin/bash
# nesell-analytics daily ETL sync
cd /Users/alexanderrogalski/nesell-analytics
# Full daily sync: orders, fees, reports, data, aggregate
PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.11 -m etl.run --days 7 >> /tmp/nesell-etl.log 2>&1
echo "------- $(date) -------" >> /tmp/nesell-etl.log
