#!/bin/bash
# nesell-analytics message center polling (every 30 min)
cd /Users/alexanderrogalski/nesell-analytics

# Add paths that launchd doesn't have (needed for claude CLI, node, etc.)
export PATH="/Users/alexanderrogalski/.local/bin:/Users/alexanderrogalski/.nvm/versions/node/v24.3.0/bin:/opt/homebrew/bin:$PATH"

# Load environment variables (launchd doesn't source shell profiles)
set -a
source /Users/alexanderrogalski/nesell-analytics/.env 2>/dev/null
source /Users/alexanderrogalski/.keys/baselinker.env 2>/dev/null
source /Users/alexanderrogalski/.keys/allegro.env 2>/dev/null
source /Users/alexanderrogalski/.keys/nesell-support-gmail.env 2>/dev/null
source /Users/alexanderrogalski/.keys/anthropic.env 2>/dev/null
set +a

PYTHONUNBUFFERED=1 /opt/homebrew/bin/python3.11 -m etl.run --messages --days 7 >> /tmp/nesell-messages.log 2>&1
echo "------- $(date) -------" >> /tmp/nesell-messages.log
