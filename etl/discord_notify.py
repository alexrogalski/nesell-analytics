"""Discord notification helper — posts messages to #agents-log channel."""
import json
import os
import requests
from pathlib import Path


def _load_env(path):
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                vals[k.strip()] = v.strip()
    return vals


_keys = _load_env(Path.home() / ".keys" / "discord.env")
BOT_TOKEN = _keys.get("DISCORD_BOT_TOKEN", "")
CHANNEL_ID = _keys.get("DISCORD_CHANNEL_ID", "1482878095009648794")
API_BASE = "https://discord.com/api/v10"


def send(content: str = None, embeds: list = None, channel_id: str = None) -> bool:
    """Send message to Discord channel. Returns True on success."""
    if not BOT_TOKEN:
        print("[Discord] No BOT_TOKEN — skipping notification")
        return False

    cid = channel_id or CHANNEL_ID
    payload = {}
    if content:
        payload["content"] = content[:2000]
    if embeds:
        payload["embeds"] = embeds[:10]

    try:
        r = requests.post(
            f"{API_BASE}/channels/{cid}/messages",
            headers={"Authorization": f"Bot {BOT_TOKEN}", "Content-Type": "application/json"},
            json=payload,
            timeout=10,
        )
        if r.status_code == 200:
            return True
        print(f"[Discord] Error {r.status_code}: {r.text[:200]}")
        return False
    except Exception as e:
        print(f"[Discord] Exception: {e}")
        return False


def send_embed(title: str, description: str, color: int = 0x5865F2,
               fields: list = None, footer: str = None) -> bool:
    """Send rich embed message."""
    embed = {"title": title, "description": description[:4096], "color": color}
    if fields:
        embed["fields"] = fields[:25]
    if footer:
        embed["footer"] = {"text": footer[:2048]}
    return send(embeds=[embed])


# Color constants
GREEN = 0x57F287
RED = 0xED4245
YELLOW = 0xFEE75C
BLUE = 0x5865F2
ORANGE = 0xFFA500
