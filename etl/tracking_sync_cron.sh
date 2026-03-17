#!/bin/bash
# Printful → Amazon tracking sync (runs every 2h via launchd)
cd /Users/alexanderrogalski/nesell-analytics

set -a
source /Users/alexanderrogalski/nesell-analytics/.env 2>/dev/null
source /Users/alexanderrogalski/.keys/printful.env 2>/dev/null
source /Users/alexanderrogalski/.keys/baselinker.env 2>/dev/null
set +a

echo "------- $(date) — tracking sync -------" >> /tmp/nesell-tracking.log
PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.11 -m etl.tracking_sync >> /tmp/nesell-tracking.log 2>&1
echo "------- done -------" >> /tmp/nesell-tracking.log
