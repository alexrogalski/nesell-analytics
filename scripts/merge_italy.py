#!/usr/bin/env python3.11
"""
Merge two Italy Hat families into ONE listing with 2D variation: STYLE_NAME/COLOR_NAME

Current state (already partially merged):
  - Parent PFT-MEGA-IT already exists with STYLE_NAME/COLOR_NAME on DE
  - Flag children (PFT-93854948-*) already linked to PFT-MEGA-IT, style="Italien Flagge"
  - No-flag children (PFT-93856295-*) already linked to PFT-MEGA-IT, style="Italien"
  - Old parents PFT-93854948, PFT-93856295 still active (DISCOVERABLE)

What this script does:
  1. Inspect current state
  2. Ensure parent PFT-MEGA-IT exists on all 8 marketplaces (PUT if missing)
  3. Update flag children: style -> localized "mit Flagge"/"con Bandiera"/etc., color -> clean
  4. Update no-flag children: style -> localized "ohne Flagge"/"senza Bandiera"/etc., color -> clean
  5. Deactivate old parents PFT-93854948 + PFT-93856295 on all 8 marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_italy.py --dry-run
  python3.11 scripts/merge_italy.py --step inspect
  python3.11 scripts/merge_italy.py --step ensure-parent
  python3.11 scripts/merge_italy.py --step update-flag
  python3.11 scripts/merge_italy.py --step update-noflag
  python3.11 scripts/merge_italy.py --step deactivate-parents
  python3.11 scripts/merge_italy.py          # full run
"""

import argparse
import json
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# ── Credentials ──────────────────────────────────────────────────────
KEYS_DIR = Path.home() / ".keys"
AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

# ── Constants ────────────────────────────────────────────────────────

NEW_PARENT_SKU = "PFT-MEGA-IT"          # already exists on DE with STYLE_NAME/COLOR_NAME

FLAG_PARENT_SKU = "PFT-93854948"       # old flag parent (to deactivate)
NOFLAG_PARENT_SKU = "PFT-93856295"     # old no-flag parent (to deactivate)

VARIANT_SUFFIXES = ["12735", "12736", "7853", "7854", "7855", "7856", "7857", "7858", "7859", "8745", "9794"]

FLAG_CHILDREN = [f"PFT-93854948-{s}" for s in VARIANT_SUFFIXES]
NOFLAG_CHILDREN = [f"PFT-93856295-{s}" for s in VARIANT_SUFFIXES]

MARKETPLACE_IDS = {
    "DE": "A1PA6795UKMFR9",
    "FR": "A13V1IB3VIYZZH",
    "IT": "APJ6JRA9NG5V4",
    "ES": "A1RKKUPIHCS9HS",
    "NL": "A1805IZSGTT6HS",
    "PL": "A1C3SOZRARQ6R3",
    "SE": "A2NODRKZP88ZB9",
    "BE": "AMEN7PMS3EDWL",
}

LANG_TAGS = {
    "DE": "de_DE", "FR": "fr_FR", "IT": "it_IT", "ES": "es_ES",
    "NL": "nl_NL", "PL": "pl_PL", "SE": "sv_SE", "BE": "nl_BE",
}

CURRENCIES = {
    "DE": "EUR", "FR": "EUR", "IT": "EUR", "ES": "EUR",
    "NL": "EUR", "PL": "PLN", "SE": "SEK", "BE": "EUR",
}

SIZE_SYSTEMS = {
    "DE": "as3", "FR": "as4", "IT": "as6", "ES": "as4",
    "NL": "as3", "PL": "as3", "SE": "as3", "BE": "as4",
}

# Localized style values for STYLE_NAME dimension
STYLE_LABELS = {
    "DE": {"flag": "mit Flagge",      "noflag": "ohne Flagge"},
    "FR": {"flag": "avec Drapeau",    "noflag": "sans Drapeau"},
    "IT": {"flag": "con Bandiera",    "noflag": "senza Bandiera"},
    "ES": {"flag": "con Bandera",     "noflag": "sin Bandera"},
    "NL": {"flag": "met Vlag",        "noflag": "zonder Vlag"},
    "PL": {"flag": "z Flaga",         "noflag": "bez Flagi"},
    "SE": {"flag": "med Flagga",      "noflag": "utan Flagga"},
    "BE": {"flag": "avec Drapeau",    "noflag": "sans Drapeau"},
}

# Color names per marketplace per variant suffix
COLORS = {
    "DE": {
        "12735": "Rot", "12736": "Dunkelgrau",
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau",
        "8745": "Cranberry", "9794": "Gruenes Tarnmuster",
    },
    "FR": {
        "12735": "Rouge", "12736": "Gris Fonce",
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Canneberge", "9794": "Camouflage Vert",
    },
    "IT": {
        "12735": "Rosso", "12736": "Grigio Scuro",
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra",
        "8745": "Cranberry", "9794": "Mimetico Verde",
    },
    "ES": {
        "12735": "Rojo", "12736": "Gris Oscuro",
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra",
        "8745": "Arandano", "9794": "Camuflaje Verde",
    },
    "NL": {
        "12735": "Rood", "12736": "Donkergrijs",
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs",
        "8745": "Cranberry", "9794": "Groen Camouflage",
    },
    "PL": {
        "12735": "Czerwony", "12736": "Ciemnoszary",
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien",
        "8745": "Zurawina", "9794": "Zielony Kamuflaz",
    },
    "SE": {
        "12735": "Rod", "12736": "Morkgra",
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra",
        "8745": "Tranbarsrod", "9794": "Gron kamouflage",
    },
    "BE": {
        "12735": "Rouge", "12736": "Gris Fonce",
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Canneberge", "9794": "Camouflage Vert",
    },
}

# Parent listing title per marketplace (no "nesell" in title, no model numbers)
PARENT_TITLES = {
    "DE": "Make Italy Great Again Trucker Cap Dad Hat Bestickt Unisex",
    "FR": "Make Italy Great Again Casquette Trucker Dad Hat Brode Unisexe",
    "IT": "Make Italy Great Again Cappello Trucker Dad Hat Ricamato Unisex",
    "ES": "Make Italy Great Again Gorra Trucker Dad Hat Bordado Unisex",
    "NL": "Make Italy Great Again Trucker Pet Dad Hat Geborduurd Unisex",
    "PL": "Make Italy Great Again Czapka Trucker Dad Hat Haftowana Unisex",
    "SE": "Make Italy Great Again Trucker Keps Dad Hat Broderad Unisex",
    "BE": "Make Italy Great Again Casquette Trucker Dad Hat Brode Unisexe",
}

PARENT_BULLETS = {
    "DE": [
        "Make Italy Great Again besticktes Dad Hat im klassischen Trucker-Stil",
        "Hochwertige 3D-Stickerei mit italienischem Stolz-Design",
        "Verstellbarer Snapback-Verschluss fuer universelle Passform",
        "100% Baumwolle Frontpanel, atmungsaktives Mesh hinten",
        "Perfektes Geschenk fuer Italien-Fans und Patrioten",
    ],
    "FR": [
        "Make Italy Great Again casquette Dad Hat style trucker brodee",
        "Broderie 3D haute qualite avec design de fierte italienne",
        "Fermeture snapback ajustable pour taille universelle",
        "Panneau avant 100% coton, maille respirante a l'arriere",
        "Cadeau parfait pour les fans de l'Italie et les patriotes",
    ],
    "IT": [
        "Make Italy Great Again cappello Dad Hat stile trucker ricamato",
        "Ricamo 3D di alta qualita con design orgoglio italiano",
        "Chiusura snapback regolabile per vestibilita universale",
        "Pannello frontale 100% cotone, rete traspirante sul retro",
        "Regalo perfetto per fan dell'Italia e patrioti",
    ],
    "ES": [
        "Make Italy Great Again gorra Dad Hat estilo trucker bordada",
        "Bordado 3D de alta calidad con diseno de orgullo italiano",
        "Cierre snapback ajustable para talla universal",
        "Panel frontal 100% algodon, malla transpirable en la parte trasera",
        "Regalo perfecto para fans de Italia y patriotas",
    ],
    "NL": [
        "Make Italy Great Again geborduurde Dad Hat trucker stijl",
        "Hoogwaardige 3D borduurwerk met Italiaans trots-ontwerp",
        "Verstelbare snapback sluiting voor universele pasvorm",
        "100% katoenen voorpaneel, ademend mesh achterkant",
        "Perfect cadeau voor Italia-fans en patriotten",
    ],
    "PL": [
        "Make Italy Great Again haftowana czapka Dad Hat trucker",
        "Wysokiej jakosci haft 3D z wloskim patriotycznym wzorem",
        "Regulowane zapiecie snapback dla uniwersalnego dopasowania",
        "100% bawelniany panel przedni, oddychajaca siatka z tylu",
        "Idealny prezent dla fanow Wloch i patriotow",
    ],
    "SE": [
        "Make Italy Great Again broderad Dad Hat trucker stil",
        "Hogkvalitativt 3D-broderi med italiensk stolthet-design",
        "Justerbar snapback-stangning for universell passform",
        "100% bomull frontpanel, andningsbar mesh baksida",
        "Perfekt present for Italien-fans och patrioter",
    ],
    "BE": [
        "Make Italy Great Again casquette Dad Hat style trucker brodee",
        "Broderie 3D haute qualite avec design de fierte italienne",
        "Fermeture snapback ajustable pour taille universelle",
        "Panneau avant 100% coton, maille respirante a l'arriere",
        "Cadeau parfait pour les fans de l'Italie et les patriotes",
    ],
}

PARENT_DESCRIPTIONS = {
    "DE": "Make Italy Great Again Trucker Cap in zwei Stilen: mit und ohne italienischer Flagge. Hochwertige 3D-Stickerei, verstellbarer Snapback-Verschluss, Einheitsgroesse. Unisex Dad Hat fuer jeden Tag.",
    "FR": "Make Italy Great Again Casquette Trucker en deux styles: avec et sans drapeau italien. Broderie 3D haute qualite, fermeture snapback ajustable, taille unique. Casquette Dad Hat unisexe pour tous les jours.",
    "IT": "Make Italy Great Again Cappello Trucker in due stili: con e senza bandiera italiana. Ricamo 3D di alta qualita, chiusura snapback regolabile, taglia unica. Cappello Dad Hat unisex per ogni giorno.",
    "ES": "Make Italy Great Again Gorra Trucker en dos estilos: con y sin bandera italiana. Bordado 3D de alta calidad, cierre snapback ajustable, talla unica. Gorra Dad Hat unisex para cada dia.",
    "NL": "Make Italy Great Again Trucker Pet in twee stijlen: met en zonder Italiaanse vlag. Hoogwaardig 3D borduurwerk, verstelbare snapback sluiting, one size. Unisex Dad Hat voor elke dag.",
    "PL": "Make Italy Great Again Czapka Trucker w dwoch stylach: z i bez wloskiej flagi. Wysokiej jakosci haft 3D, regulowane zapiecie snapback, rozmiar uniwersalny. Czapka Dad Hat unisex na co dzien.",
    "SE": "Make Italy Great Again Trucker Keps i tva stilar: med och utan italiensk flagga. Hogkvalitativt 3D-broderi, justerbar snapback-stangning, one size. Unisex Dad Hat for varje dag.",
    "BE": "Make Italy Great Again Casquette Trucker en deux styles: avec et sans drapeau italien. Broderie 3D haute qualite, fermeture snapback ajustable, taille unique. Casquette Dad Hat unisexe pour tous les jours.",
}

KEYWORDS = {
    "DE": "make italy great again trucker cap dad hat bestickt italien flagge patriotisch baseball kappe unisex geschenk",
    "FR": "make italy great again casquette trucker dad hat brode italie drapeau patriotique baseball unisexe cadeau",
    "IT": "make italy great again cappello trucker dad hat ricamato italia bandiera patriottico baseball unisex regalo",
    "ES": "make italy great again gorra trucker dad hat bordado italia bandera patriotico baseball unisex regalo",
    "NL": "make italy great again trucker pet dad hat geborduurd italie vlag patriottisch baseball unisex cadeau",
    "PL": "make italy great again czapka trucker dad hat haftowana wlochy flaga patriotyczna baseballowa unisex prezent",
    "SE": "make italy great again trucker keps dad hat broderad italien flagga patriotisk baseball unisex present",
    "BE": "make italy great again casquette trucker dad hat brode italie drapeau patriotique baseball unisexe cadeau",
}

SLEEP_BETWEEN = 0.6


# ── Amazon SP-API Client ─────────────────────────────────────────────

class AmazonAPI:
    def __init__(self, creds):
        self.creds = creds
        self._token = None
        self._token_time = 0

    def _refresh(self):
        r = requests.post("https://api.amazon.com/auth/o2/token", data={
            "grant_type": "refresh_token",
            "refresh_token": self.creds["refresh_token"],
            "client_id": self.creds["client_id"],
            "client_secret": self.creds["client_secret"],
        })
        data = r.json()
        self._token = data.get("access_token")
        self._token_time = time.time()
        if not self._token:
            print(f"  [AUTH ERROR] {data}")
            sys.exit(1)
        print(f"  Token obtained: {self._token[:20]}...")

    def _headers(self):
        if not self._token or time.time() - self._token_time > 3000:
            self._refresh()
        return {"x-amz-access-token": self._token, "Content-Type": "application/json"}

    def get_listing(self, sku, mp_id, retries=5):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        params = {"marketplaceIds": mp_id, "issueLocale": "en_US",
                  "includedData": "summaries,attributes,issues"}
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=self._headers(), params=params, timeout=30)
            except Exception as e:
                print(f"    [GET {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(3)
                continue
            if r.status_code == 429:
                wait = min(3 * (2 ** attempt), 30)
                print(f"    [GET {sku}] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(1)
                continue
            if r.status_code >= 500:
                time.sleep(5)
                continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

    def put_listing(self, sku, mp_id, attributes, product_type="HAT", retries=8):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        body = {"productType": product_type, "requirements": "LISTING", "attributes": attributes}
        params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
        for attempt in range(retries):
            try:
                r = requests.put(url, headers=self._headers(), json=body,
                                 params=params, timeout=30)
            except Exception as e:
                print(f"    [PUT {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429:
                wait = min(5 * (2 ** attempt), 60)
                print(f"    [PUT {sku}] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(3)
                continue
            if r.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            resp = r.json() if r.text else {}
            status_str = resp.get("status", "?")
            issues = resp.get("issues", [])
            errors = [i for i in issues if i.get("severity") == "ERROR"]
            if errors:
                issue_msgs = [f"{i.get('code','')}: {i.get('message','')[:120]}" for i in errors[:3]]
                print(f"    [{r.status_code}] PUT {sku} -> {status_str} | {'; '.join(issue_msgs)}")
            else:
                print(f"    [{r.status_code}] PUT {sku} -> {status_str}")
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}

    def patch_listing(self, sku, mp_id, patches, product_type="HAT", retries=8):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        body = {"productType": product_type, "patches": patches}
        params = {"marketplaceIds": mp_id, "issueLocale": "en_US"}
        for attempt in range(retries):
            try:
                r = requests.patch(url, headers=self._headers(), json=body,
                                   params=params, timeout=30)
            except Exception as e:
                print(f"    [PATCH {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429:
                wait = min(5 * (2 ** attempt), 60)
                print(f"    [PATCH {sku}] 429 rate limited, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(3)
                continue
            if r.status_code >= 500:
                time.sleep(5 * (attempt + 1))
                continue
            resp = r.json() if r.text else {}
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}

    def delete_listing(self, sku, mp_id, retries=5):
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
        params = {"marketplaceIds": mp_id}
        for attempt in range(retries):
            try:
                r = requests.delete(url, headers=self._headers(), params=params, timeout=30)
            except Exception as e:
                print(f"    [DELETE {sku}] connection error: {e}, retry {attempt+1}/{retries}")
                time.sleep(5)
                continue
            if r.status_code == 429:
                wait = min(3 * (2 ** attempt), 30)
                print(f"    [DELETE {sku}] 429, waiting {wait}s")
                time.sleep(wait)
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(2)
                continue
            resp = {}
            try:
                resp = r.json()
            except Exception:
                pass
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}


# ── Report Helpers ────────────────────────────────────────────────────

def log_result(results, action, sku, mkt, status_code, resp):
    issues = resp.get("issues", []) if isinstance(resp, dict) else []
    errors = [i for i in issues if i.get("severity") == "ERROR"]
    warnings = [i for i in issues if i.get("severity") == "WARNING"]
    status = resp.get("status", "?") if isinstance(resp, dict) else "?"

    raw_errors = resp.get("errors", []) if isinstance(resp, dict) else []
    if raw_errors and not errors:
        errors = raw_errors

    entry = {
        "action": action,
        "sku": sku,
        "marketplace": mkt,
        "http_status": status_code,
        "api_status": status,
        "errors": [f"{e.get('code','')}: {e.get('message','')}" for e in errors],
        "warnings": [f"{w.get('code','')}: {w.get('message','')}" for w in warnings],
    }
    results.append(entry)

    error_str = ""
    if entry["errors"]:
        error_str = f" | ERRORS: {'; '.join(entry['errors'][:3])}"
    elif entry["warnings"]:
        error_str = f" | WARN: {'; '.join(entry['warnings'][:2])}"

    icon = "OK" if status_code in (200, 204) and not entry["errors"] else "ERR"
    print(f"  [{icon}] {action} {sku} on {mkt}: HTTP {status_code} -> {status}{error_str}")


# ═══════════════════════════════════════════════════════════════════════
# STEP: INSPECT
# ═══════════════════════════════════════════════════════════════════════

def step_inspect(api):
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Check flag parent
    print(f"\n  --- Flag Parent: {FLAG_PARENT_SKU} ---")
    code, data = api.get_listing(FLAG_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        print(f"  parentage_level: {attrs.get('parentage_level', [])}")
        print(f"  style: {attrs.get('style', [])}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
            print(f"  status: {summaries[0].get('status','?')}")
    time.sleep(SLEEP_BETWEEN)

    # Check no-flag parent
    print(f"\n  --- No-Flag Parent: {NOFLAG_PARENT_SKU} ---")
    code, data = api.get_listing(NOFLAG_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        print(f"  parentage_level: {attrs.get('parentage_level', [])}")
        print(f"  style: {attrs.get('style', [])}")
        summaries = data.get("summaries", [])
        if summaries:
            print(f"  ASIN: {summaries[0].get('asin','?')}")
            print(f"  status: {summaries[0].get('status','?')}")
    time.sleep(SLEEP_BETWEEN)

    # Check if MEGA-ITALY already exists
    print(f"\n  --- New Parent (MEGA-ITALY) ---")
    code, data = api.get_listing(NEW_PARENT_SKU, de_mp)
    print(f"  HTTP {code}")
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {attrs.get('variation_theme', [])}")
        print(f"  Already exists!")
    elif code == 404:
        print(f"  Does not exist yet (will be created)")
    time.sleep(SLEEP_BETWEEN)

    # Sample flag children
    for sku in FLAG_CHILDREN[:2]:
        print(f"\n  --- Flag child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {attrs.get('color', [])}")
            print(f"  style: {attrs.get('style', [])}")
            print(f"  variation_theme: {attrs.get('variation_theme', [])}")
            print(f"  child_parent_sku_relationship: {attrs.get('child_parent_sku_relationship', [])}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}")
                print(f"  status: {summaries[0].get('status','?')}")
        time.sleep(SLEEP_BETWEEN)

    # Sample no-flag children
    for sku in NOFLAG_CHILDREN[:2]:
        print(f"\n  --- No-flag child: {sku} ---")
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  color: {attrs.get('color', [])}")
            print(f"  style: {attrs.get('style', [])}")
            print(f"  variation_theme: {attrs.get('variation_theme', [])}")
            print(f"  child_parent_sku_relationship: {attrs.get('child_parent_sku_relationship', [])}")
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}")
                print(f"  status: {summaries[0].get('status','?')}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# ═══════════════════════════════════════════════════════════════════════
# STEP: CREATE PARENT (MEGA-ITALY) ON ALL MARKETPLACES
# ═══════════════════════════════════════════════════════════════════════

def build_parent_attrs(mkt_code):
    """Build MEGA-ITALY parent listing attributes for a marketplace."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]

    attrs = {
        "item_name": [{"value": PARENT_TITLES[mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "STYLE_NAME/COLOR_NAME"}],
        "parentage_level": [{"marketplace_id": mkt_id, "value": "parent"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt_id, "child_relationship_type": "variation"}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in PARENT_BULLETS[mkt_code]],
        "product_description": [{"value": PARENT_DESCRIPTIONS[mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": KEYWORDS[mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt_id}],
        "age_range_description": [{"value": "Erwachsene" if mkt_code == "DE" else ("Adultes" if mkt_code in ("FR","BE") else ("Adulti" if mkt_code == "IT" else ("Adultos" if mkt_code == "ES" else ("Volwassenen" if mkt_code == "NL" else ("Dorosli" if mkt_code == "PL" else "Vuxna"))))), "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "material": [{"value": "Baumwolle" if mkt_code == "DE" else ("Coton" if mkt_code in ("FR","BE") else ("Cotone" if mkt_code == "IT" else ("Algodon" if mkt_code == "ES" else ("Katoen" if mkt_code == "NL" else ("Bawelna" if mkt_code == "PL" else "Bomull"))))), "language_tag": lang, "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS[mkt_code], "size_class": "alpha", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mkt_id}],
    }
    return attrs


def step_ensure_parent(api, results, dry_run=False):
    """Ensure PFT-MEGA-IT parent exists on all 8 marketplaces. PUT only where missing."""
    print("\n" + "=" * 70)
    print(f"  STEP: ENSURE PARENT ({NEW_PARENT_SKU}) ON ALL MARKETPLACES")
    print(f"  variation_theme: STYLE_NAME/COLOR_NAME")
    print("=" * 70)

    total = 0
    skipped = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        # Check if parent already exists on this marketplace
        code, data = api.get_listing(NEW_PARENT_SKU, mp_id)
        if code == 200 and data.get("summaries"):
            print(f"  [SKIP] {NEW_PARENT_SKU} already exists on {mkt_code}")
            skipped += 1
            time.sleep(SLEEP_BETWEEN)
            continue

        attrs = build_parent_attrs(mkt_code)
        if dry_run:
            print(f"  [DRY] Would PUT {NEW_PARENT_SKU} on {mkt_code} with STYLE_NAME/COLOR_NAME")
            total += 1
            continue

        code, resp = api.put_listing(NEW_PARENT_SKU, mp_id, attrs)
        log_result(results, "ensure_parent", NEW_PARENT_SKU, mkt_code, code, resp)
        total += 1
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Created/ensured parent: {total} new, {skipped} already existed.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# STEP: RE-LINK CHILDREN
# ═══════════════════════════════════════════════════════════════════════

def update_children_style(api, results, children_skus, style_key, group_name, dry_run=False):
    """Update children style + color + ensure linked to PFT-MEGA-IT parent."""
    print("\n" + "=" * 70)
    print(f"  STEP: UPDATE {group_name} CHILDREN (style + color + parent={NEW_PARENT_SKU})")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        style_value = STYLE_LABELS[mkt_code][style_key]
        mkt_colors = COLORS.get(mkt_code, COLORS["DE"])

        print(f"\n  --- {mkt_code}: style='{style_value}' ---")

        for sku in children_skus:
            # Extract variant suffix from SKU
            suffix = sku.rsplit("-", 1)[-1]
            color_value = mkt_colors.get(suffix, "")

            if not color_value:
                print(f"  [WARN] No color for suffix {suffix} on {mkt_code}, skipping {sku}")
                continue

            patches = [
                {
                    "op": "replace",
                    "path": "/attributes/child_parent_sku_relationship",
                    "value": [{
                        "child_relationship_type": "variation",
                        "parent_sku": NEW_PARENT_SKU,
                        "marketplace_id": mp_id
                    }]
                },
                {
                    "op": "replace",
                    "path": "/attributes/parentage_level",
                    "value": [{"marketplace_id": mp_id, "value": "child"}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/variation_theme",
                    "value": [{"name": "STYLE_NAME/COLOR_NAME"}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": style_value, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/color",
                    "value": [{"value": color_value, "language_tag": lang, "marketplace_id": mp_id}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku} on {mkt_code}: style='{style_value}', color='{color_value}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, f"update_{style_key}", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Updated {total} {group_name} children across all marketplaces.")
    return total


def step_update_flag(api, results, dry_run=False):
    return update_children_style(api, results, FLAG_CHILDREN, "flag", "FLAG", dry_run)


def step_update_noflag(api, results, dry_run=False):
    return update_children_style(api, results, NOFLAG_CHILDREN, "noflag", "NO-FLAG", dry_run)


# ═══════════════════════════════════════════════════════════════════════
# STEP: DEACTIVATE OLD PARENTS
# ═══════════════════════════════════════════════════════════════════════

def step_deactivate_parents(api, results, dry_run=False):
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE OLD PARENTS ({FLAG_PARENT_SKU}, {NOFLAG_PARENT_SKU})")
    print("=" * 70)

    total = 0
    for parent_sku in [FLAG_PARENT_SKU, NOFLAG_PARENT_SKU]:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, "deactivate_parent", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated parents on {total} marketplace slots.")
    return total


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Merge Italy Flag/No-Flag into 2D STYLE_NAME/COLOR_NAME variation")
    parser.add_argument("--dry-run", action="store_true", help="Print what would happen without API calls")
    parser.add_argument("--step", choices=["inspect", "ensure-parent", "update-flag", "update-noflag", "deactivate-parents"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE ITALY FAMILIES (2D: STYLE_NAME/COLOR_NAME)")
    print(f"  Parent (keep/ensure): {NEW_PARENT_SKU}")
    print(f"  Old flag parent (deactivate): {FLAG_PARENT_SKU}")
    print(f"  Old no-flag parent (deactivate): {NOFLAG_PARENT_SKU}")
    print(f"  Flag children: {len(FLAG_CHILDREN)} SKUs -> style='flag' localized")
    print(f"  No-flag children: {len(NOFLAG_CHILDREN)} SKUs -> style='noflag' localized")
    print(f"  Total children: {len(FLAG_CHILDREN) + len(NOFLAG_CHILDREN)}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect" or args.step is None:
        step_inspect(api)
        if args.step == "inspect":
            return

    if args.step == "ensure-parent" or args.step is None:
        step_ensure_parent(api, results, args.dry_run)
        if args.step == "ensure-parent":
            print_report(results)
            return

    if args.step == "update-flag" or args.step is None:
        step_update_flag(api, results, args.dry_run)
        if args.step == "update-flag":
            print_report(results)
            return

    if args.step == "update-noflag" or args.step is None:
        step_update_noflag(api, results, args.dry_run)
        if args.step == "update-noflag":
            print_report(results)
            return

    if args.step == "deactivate-parents" or args.step is None:
        step_deactivate_parents(api, results, args.dry_run)
        if args.step == "deactivate-parents":
            print_report(results)
            return

    print_report(results)


def print_report(results):
    print("\n" + "=" * 70)
    print("  FINAL REPORT")
    print("=" * 70)

    if not results:
        print("  No API calls were made (dry run or inspect only).")
        return

    actions = {}
    errors_list = []
    for r in results:
        action = r["action"]
        actions.setdefault(action, {"total": 0, "ok": 0, "err": 0})
        actions[action]["total"] += 1
        if r["http_status"] in (200, 204) and not r["errors"]:
            actions[action]["ok"] += 1
        else:
            actions[action]["err"] += 1
            if r["errors"]:
                errors_list.append(r)

    for action, counts in actions.items():
        print(f"  {action}: {counts['ok']}/{counts['total']} OK, {counts['err']} errors")

    if errors_list:
        print(f"\n  --- ERRORS ({len(errors_list)}) ---")
        for r in errors_list:
            print(f"  {r['action']} {r['sku']} on {r['marketplace']}: {'; '.join(r['errors'][:3])}")

    parent_ok = sum(1 for r in results
                    if r["action"] == "ensure_parent"
                    and r["http_status"] in (200, 204) and not r["errors"])
    flag_ok = sum(1 for r in results
                  if r["action"] == "update_flag"
                  and r["http_status"] in (200, 204) and not r["errors"])
    noflag_ok = sum(1 for r in results
                    if r["action"] == "update_noflag"
                    and r["http_status"] in (200, 204) and not r["errors"])
    deactivated = sum(1 for r in results
                      if r["action"] == "deactivate_parent"
                      and r["http_status"] in (200, 204) and not r["errors"])

    print(f"\n  Summary:")
    print(f"    Parent ensured on new marketplaces: {parent_ok}")
    print(f"    Flag children updated (style+color): {flag_ok}")
    print(f"    No-flag children updated (style+color): {noflag_ok}")
    print(f"    Old parents deactivated: {deactivated}")
    print(f"    Total API calls: {len(results)}")

    results_path = Path(__file__).parent / "merge_italy_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
