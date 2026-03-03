"""Configuration: load all credentials from ~/.keys/ and env."""
import os, json
from pathlib import Path

KEYS_DIR = Path.home() / ".keys"

# --- Baselinker ---
def _load_env_file(path):
    """Read KEY=VALUE from .env file."""
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip().replace('\r', '')
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                vals[k.strip()] = v.strip()
    return vals

_bl = _load_env_file(KEYS_DIR / "baselinker.env")
BASELINKER_TOKEN = _bl.get("BASELINKER_API_TOKEN", "")
BASELINKER_URL = "https://api.baselinker.com/connector.php"

# --- Amazon SP-API ---
_amz_path = KEYS_DIR / "amazon-sp-api.json"
if _amz_path.exists():
    AMZ_CREDS = json.loads(_amz_path.read_text())
else:
    AMZ_CREDS = {}
AMZ_SELLER_ID = AMZ_CREDS.get("seller_id", "A1IZH6PW7A624A")
AMZ_API_BASE = "https://sellingpartnerapi-eu.amazon.com"

# --- Printful ---
_pf = _load_env_file(KEYS_DIR / "printful.env")
PRINTFUL_V1_TOKEN = _pf.get("PRINTFUL_API_KEY", "")
PRINTFUL_STORE_ID = "15269225"

# --- Supabase ---
# Load from project .env or environment
_supa = _load_env_file(Path(__file__).parent.parent / ".env")
SUPABASE_URL = os.environ.get("SUPABASE_URL", _supa.get("SUPABASE_URL", ""))
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", _supa.get("SUPABASE_KEY", ""))
SUPABASE_DB_URL = os.environ.get("SUPABASE_DB_URL", _supa.get("SUPABASE_DB_URL", ""))

# --- Telegram ---
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", _supa.get("TELEGRAM_BOT_TOKEN", ""))
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", _supa.get("TELEGRAM_CHAT_ID", ""))

# --- Marketplace mapping ---
MARKETPLACE_TO_PLATFORM = {
    "A1PA6795UKMFR9": "amazon_de",
    "A13V1IB3VIYZZH": "amazon_fr",
    "A1RKKUPIHCS9HS": "amazon_es",
    "APJ6JRA9NG5V4":  "amazon_it",
    "A1805IZSGTT6HS": "amazon_nl",
    "A1C3SOZRARQ6R3": "amazon_pl",
    "A2NODRKZP88ZB9": "amazon_se",
    "AMEN7PMS3EDWL":  "amazon_be",
    "A1F83G8C2ARO7P": "amazon_gb",
}

# Baselinker source mapping (order_source_id or order_source name → platform code)
BL_SOURCE_TO_PLATFORM = {
    "amazon":   None,  # determined by marketplace_id
    "allegro":  "allegro",
    "temu":     "temu",
    "empik":    "empik",
}


def get_amazon_token():
    """Get fresh Amazon access token."""
    import requests
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": AMZ_CREDS.get("refresh_token", ""),
        "client_id": AMZ_CREDS.get("client_id", ""),
        "client_secret": AMZ_CREDS.get("client_secret", ""),
    })
    return r.json()["access_token"]
