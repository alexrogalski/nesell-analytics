#!/usr/bin/env python3.11
"""
Merge America Dad Hat (flag) + Trucker Cap into one 2D variation (STYLE_NAME/COLOR_NAME).

Current state:
  - Dad Hat (flag): Parent PFT-90202876, 11 children (PFT-90202876-*), price 29.99 EUR
  - Trucker Cap:    Parent PFT-90229846, 18 children (PFT-90229846-*), price 22.99 EUR

New parent: MEGA-AMERICA, productType: HAT, variation_theme: STYLE_NAME/COLOR_NAME

Steps:
  1. Inspect: read current state of both parents + sample children on DE
  2. Create parent: PUT MEGA-AMERICA as parent with STYLE_NAME/COLOR_NAME theme on all 8 marketplaces
  3. Relink Dad Hat children: set child_parent_sku_relationship -> MEGA-AMERICA,
     style = "Dad Hat" (universal), color = localized color
  4. Relink Trucker Cap children: same but style = "Trucker Cap" (universal),
     color = localized color
  5. Deactivate old parents (PFT-90202876, PFT-90229846) on all 8 marketplaces

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/merge_america.py --dry-run
  python3.11 scripts/merge_america.py --step inspect
  python3.11 scripts/merge_america.py --step create-parent
  python3.11 scripts/merge_america.py --step relink
  python3.11 scripts/merge_america.py --step deactivate
  python3.11 scripts/merge_america.py --step verify
  python3.11 scripts/merge_america.py                      # full run
"""

import argparse
import json
import sys
import time
import urllib.parse
from datetime import datetime
from pathlib import Path

import requests

# -- Credentials -------------------------------------------------------------
KEYS_DIR = Path.home() / ".keys"
AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

# -- Constants ----------------------------------------------------------------

NEW_PARENT_SKU = "MEGA-AMERICA"
DAD_HAT_PARENT_SKU = "PFT-90202876"       # Dad Hat with flag (to deactivate)
TRUCKER_PARENT_SKU = "PFT-90229846"       # Trucker Cap (to deactivate)

# Dad Hat children (11 variants)
DAD_HAT_SUFFIXES = [
    "12735", "12736", "7853", "7854", "7855",
    "7856", "7857", "7858", "7859", "8745", "9794",
]
DAD_HAT_CHILDREN = [f"PFT-90202876-{s}" for s in DAD_HAT_SUFFIXES]

# Trucker Cap children (18 variants -- 7 solid + 9 two-tone + 2 extra)
TRUCKER_SUFFIXES = [
    "8747", "8748", "8749", "8750", "8751", "8752", "8753",
    "10933", "10934", "10935", "10936", "10937", "10938", "10939", "10940", "10941",
    "12220",
]
TRUCKER_CHILDREN = [f"PFT-90229846-{s}" for s in TRUCKER_SUFFIXES]

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

# Style values -- "Dad Hat" and "Trucker Cap" are universal terms
STYLE_DAD_HAT = "Dad Hat"
STYLE_TRUCKER = "Trucker Cap"

# Localized parent title per marketplace
PARENT_TITLES = {
    "DE": "Bestickte Kappe Make America Great Again - Unisex",
    "FR": "Casquette Brodee Make America Great Again - Unisex",
    "IT": "Cappellino Ricamato Make America Great Again - Unisex",
    "ES": "Gorra Bordada Make America Great Again - Unisex",
    "NL": "Geborduurde Pet Make America Great Again - Unisex",
    "PL": "Haftowana Czapka Make America Great Again - Unisex",
    "SE": "Broderad Keps Make America Great Again - Unisex",
    "BE": "Geborduurde Pet Make America Great Again - Unisex",
}

# -- Dad Hat color names per marketplace per suffix --
DAD_HAT_COLORS = {
    "DE": {
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau",
        "8745": "Tannengruen", "9794": "Gruenes Tarnmuster",
        "12735": "Cranberry", "12736": "Dunkelgrau",
    },
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Vert Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra",
        "8745": "Verde Abete", "9794": "Mimetico Verde",
        "12735": "Mirtillo Rosso", "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra",
        "8745": "Verde Abeto", "9794": "Camuflaje Verde",
        "12735": "Arandano", "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs",
        "8745": "Spargroen", "9794": "Groen Camouflage",
        "12735": "Cranberry", "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien",
        "8745": "Ciemnozielony", "9794": "Zielony Kamuflaz",
        "12735": "Zurawinowy", "12736": "Ciemnoszary",
    },
    "SE": {
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra",
        "8745": "Grangren", "9794": "Gron kamouflage",
        "12735": "Tranbar", "12736": "Morkgra",
    },
    "BE": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre",
        "8745": "Vert Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
    },
}

# -- Trucker Cap color names per marketplace per suffix --
TRUCKER_COLORS = {
    "DE": {
        "8747": "Weiss", "8748": "Schwarz", "8749": "Anthrazit", "8750": "Grau",
        "8751": "Marineblau", "8752": "Rot", "8753": "Koenigsblau",
        "10933": "Braun/Beige", "10934": "Gruen/Weiss", "10935": "Graumeliert/Weiss",
        "10936": "Schwarz/Weiss", "10937": "Marineblau/Weiss", "10938": "Rot/Weiss",
        "10939": "Koenigsblau/Weiss", "10940": "Anthrazit/Weiss", "10941": "Tuerkis/Weiss",
        "12220": "Rost/Beige",
    },
    "FR": {
        "8747": "Blanc", "8748": "Noir", "8749": "Anthracite", "8750": "Gris",
        "8751": "Bleu Marine", "8752": "Rouge", "8753": "Bleu Royal",
        "10933": "Marron/Beige", "10934": "Vert/Blanc", "10935": "Gris Chine/Blanc",
        "10936": "Noir/Blanc", "10937": "Bleu Marine/Blanc", "10938": "Rouge/Blanc",
        "10939": "Bleu Royal/Blanc", "10940": "Anthracite/Blanc", "10941": "Turquoise/Blanc",
        "12220": "Rouille/Beige",
    },
    "IT": {
        "8747": "Bianco", "8748": "Nero", "8749": "Antracite", "8750": "Grigio",
        "8751": "Blu Marina", "8752": "Rosso", "8753": "Blu Reale",
        "10933": "Marrone/Beige", "10934": "Verde/Bianco", "10935": "Grigio Melange/Bianco",
        "10936": "Nero/Bianco", "10937": "Blu Marina/Bianco", "10938": "Rosso/Bianco",
        "10939": "Blu Reale/Bianco", "10940": "Antracite/Bianco", "10941": "Turchese/Bianco",
        "12220": "Ruggine/Beige",
    },
    "ES": {
        "8747": "Blanco", "8748": "Negro", "8749": "Antracita", "8750": "Gris",
        "8751": "Azul Marino", "8752": "Rojo", "8753": "Azul Real",
        "10933": "Marron/Beige", "10934": "Verde/Blanco", "10935": "Gris Jaspeado/Blanco",
        "10936": "Negro/Blanco", "10937": "Azul Marino/Blanco", "10938": "Rojo/Blanco",
        "10939": "Azul Real/Blanco", "10940": "Antracita/Blanco", "10941": "Turquesa/Blanco",
        "12220": "Oxido/Beige",
    },
    "NL": {
        "8747": "Wit", "8748": "Zwart", "8749": "Antraciet", "8750": "Grijs",
        "8751": "Marineblauw", "8752": "Rood", "8753": "Koningsblauw",
        "10933": "Bruin/Beige", "10934": "Groen/Wit", "10935": "Grijsgemeleerd/Wit",
        "10936": "Zwart/Wit", "10937": "Marineblauw/Wit", "10938": "Rood/Wit",
        "10939": "Koningsblauw/Wit", "10940": "Antraciet/Wit", "10941": "Turquoise/Wit",
        "12220": "Roest/Beige",
    },
    "PL": {
        "8747": "Bialy", "8748": "Czarny", "8749": "Antracytowy", "8750": "Szary",
        "8751": "Granatowy", "8752": "Czerwony", "8753": "Krolewski Niebieski",
        "10933": "Brazowy/Bezowy", "10934": "Zielony/Bialy", "10935": "Szary Melanz/Bialy",
        "10936": "Czarny/Bialy", "10937": "Granatowy/Bialy", "10938": "Czerwony/Bialy",
        "10939": "Krolewski Niebieski/Bialy", "10940": "Antracytowy/Bialy", "10941": "Turkusowy/Bialy",
        "12220": "Rdzawy/Bezowy",
    },
    "SE": {
        "8747": "Vit", "8748": "Svart", "8749": "Antracit", "8750": "Gra",
        "8751": "Marinbla", "8752": "Rod", "8753": "Kungsbla",
        "10933": "Brun/Beige", "10934": "Gron/Vit", "10935": "Gramelerad/Vit",
        "10936": "Svart/Vit", "10937": "Marinbla/Vit", "10938": "Rod/Vit",
        "10939": "Kungsbla/Vit", "10940": "Antracit/Vit", "10941": "Turkos/Vit",
        "12220": "Rost/Beige",
    },
    "BE": {
        "8747": "Wit", "8748": "Zwart", "8749": "Antraciet", "8750": "Grijs",
        "8751": "Marineblauw", "8752": "Rood", "8753": "Koningsblauw",
        "10933": "Bruin/Beige", "10934": "Groen/Wit", "10935": "Grijsgemeleerd/Wit",
        "10936": "Zwart/Wit", "10937": "Marineblauw/Wit", "10938": "Rood/Wit",
        "10939": "Koningsblauw/Wit", "10940": "Antraciet/Wit", "10941": "Turquoise/Wit",
        "12220": "Roest/Beige",
    },
}

SLEEP_BETWEEN = 0.6


# -- Amazon SP-API Client ----------------------------------------------------

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

    def put_listing(self, sku, mp_id, body, retries=8):
        """PUT (full create/replacement) for parent listings."""
        encoded = urllib.parse.quote(sku, safe="")
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{encoded}"
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


# -- Logging ------------------------------------------------------------------

def log_result(results, action, sku, mkt, status_code, resp):
    """Append to results list and print."""
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


# =============================================================================
# STEP: INSPECT
# =============================================================================

def step_inspect(api):
    """Inspect current state of both parents and sample children on DE."""
    print("\n" + "=" * 70)
    print("  STEP: INSPECT CURRENT STATE (DE)")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    for parent_sku, label in [(DAD_HAT_PARENT_SKU, "Dad Hat parent"),
                               (TRUCKER_PARENT_SKU, "Trucker parent"),
                               (NEW_PARENT_SKU, "New merged parent")]:
        print(f"\n  --- {label}: {parent_sku} ---")
        code, data = api.get_listing(parent_sku, de_mp)
        print(f"  HTTP {code}")
        if code == 200 and data:
            attrs = data.get("attributes", {})
            print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
            print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
            summaries = data.get("summaries", [])
            if summaries:
                s = summaries[0]
                print(f"  ASIN: {s.get('asin','?')}")
                print(f"  status: {json.dumps(s.get('status', []))}")
            issues = data.get("issues", [])
            if issues:
                print(f"  Issues ({len(issues)}):")
                for issue in issues[:3]:
                    print(f"    [{issue.get('severity','')}] {issue.get('code','')}: {issue.get('message','')[:100]}")
        time.sleep(SLEEP_BETWEEN)

    # Sample Dad Hat children
    print(f"\n  --- DAD HAT CHILDREN (sample) ---")
    for sku in DAD_HAT_CHILDREN[:3]:
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            color = attrs.get("color", [{}])[0].get("value", "?") if attrs.get("color") else "?"
            style = attrs.get("style", [{}])[0].get("value", "?") if attrs.get("style") else "?"
            parent = attrs.get("child_parent_sku_relationship", [{}])[0].get("parent_sku", "?") if attrs.get("child_parent_sku_relationship") else "?"
            summaries = data.get("summaries", [])
            status = summaries[0].get("status", "?") if summaries else "?"
            asin = summaries[0].get("asin", "?") if summaries else "?"
            print(f"  {sku}: color={color}, style={style}, parent={parent}, asin={asin}, status={status}")
        else:
            print(f"  {sku}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    # Sample Trucker children
    print(f"\n  --- TRUCKER CHILDREN (sample) ---")
    for sku in TRUCKER_CHILDREN[:3]:
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            color = attrs.get("color", [{}])[0].get("value", "?") if attrs.get("color") else "?"
            style = attrs.get("style", [{}])[0].get("value", "?") if attrs.get("style") else "?"
            parent = attrs.get("child_parent_sku_relationship", [{}])[0].get("parent_sku", "?") if attrs.get("child_parent_sku_relationship") else "?"
            summaries = data.get("summaries", [])
            status = summaries[0].get("status", "?") if summaries else "?"
            asin = summaries[0].get("asin", "?") if summaries else "?"
            print(f"  {sku}: color={color}, style={style}, parent={parent}, asin={asin}, status={status}")
        else:
            print(f"  {sku}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    print("\n  Inspect complete.\n")


# -- Localized parent content --------------------------------------------------

PARENT_BULLETS = {
    "DE": [
        "PREMIUM MASCHINENSTICKEREI - Professionell gestickter Schriftzug auf der Vorderseite. Stickerei verblasst nicht, blaettert nicht ab und reisst nicht wie Drucke",
        "ZWEI STILE VERFUEGBAR - Dad Hat (100% Baumwolle, Metallschnalle) oder Trucker Cap (Baumwoll-Twill Vorderseite mit Mesh-Netz, Snapback)",
        "VERSTELLBARE PASSFORM - Einheitsgroesse fuer Damen und Herren. Metall- oder Snapback-Verschluss je nach Stil",
        "PERFEKTES GESCHENK - Ideales Geschenk fuer Patrioten, Geburtstage, Feiertage oder einfach als Statement-Accessoire",
        "VIELSEITIG EINSETZBAR - Perfekt fuer Freizeit, Sport, Reisen, Festivals und den taeglichen Gebrauch",
    ],
    "FR": [
        "BRODERIE MACHINE PREMIUM - Texte brode professionnellement sur le devant. La broderie ne decolore pas et ne se dechire pas comme les impressions",
        "DEUX STYLES DISPONIBLES - Dad Hat (100% coton, boucle metallique) ou Trucker Cap (coton twill avec mesh, snapback)",
        "TAILLE AJUSTABLE - Taille unique pour femme et homme. Fermeture metallique ou snapback selon le style",
        "CADEAU PARFAIT - Ideal pour les patriotes, anniversaires, fetes ou simplement comme accessoire tendance",
        "POLYVALENTE - Parfaite pour les loisirs, le sport, les voyages, les festivals et un usage quotidien",
    ],
    "IT": [
        "RICAMO A MACCHINA PREMIUM - Testo ricamato professionalmente sulla parte anteriore. Il ricamo non sbiadisce e non si stacca come le stampe",
        "DUE STILI DISPONIBILI - Dad Hat (100% cotone, fibbia metallica) o Trucker Cap (cotone twill con mesh, snapback)",
        "TAGLIA REGOLABILE - Taglia unica per donna e uomo. Chiusura metallica o snapback a seconda dello stile",
        "REGALO PERFETTO - Ideale per patrioti, compleanni, festivita o semplicemente come accessorio di tendenza",
        "VERSATILE - Perfetto per tempo libero, sport, viaggi, festival e uso quotidiano",
    ],
    "ES": [
        "BORDADO A MAQUINA PREMIUM - Texto bordado profesionalmente en la parte delantera. El bordado no destine y no se rompe como las impresiones",
        "DOS ESTILOS DISPONIBLES - Dad Hat (100% algodon, hebilla metalica) o Trucker Cap (algodon twill con malla, snapback)",
        "TALLA AJUSTABLE - Talla unica para mujer y hombre. Cierre metalico o snapback segun el estilo",
        "REGALO PERFECTO - Ideal para patriotas, cumpleanos, fiestas o simplemente como accesorio de moda",
        "VERSATIL - Perfecta para ocio, deporte, viajes, festivales y uso diario",
    ],
    "NL": [
        "PREMIUM MACHINEBORDUURWERK - Professioneel geborduurde tekst aan de voorkant. Borduurwerk vervaagt niet en scheurt niet zoals prints",
        "TWEE STIJLEN BESCHIKBAAR - Dad Hat (100% katoen, metalen gesp) of Trucker Pet (katoenen twill met mesh, snapback)",
        "VERSTELBARE PASVORM - One size voor dames en heren. Metalen of snapback-sluiting afhankelijk van de stijl",
        "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of gewoon als stijlvol accessoire",
        "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
    ],
    "PL": [
        "HAFT MASZYNOWY PREMIUM - Profesjonalnie wyhaftowany napis na przodzie. Haft nie blaknie i nie luszy sie jak nadruki",
        "DWA STYLE DOSTEPNE - Dad Hat (100% bawelna, metalowe zapiecie) lub Trucker Cap (bawelniany twill z siatka, snapback)",
        "REGULOWANY ROZMIAR - Rozmiar uniwersalny dla kobiet i mezczyzn. Metalowe lub snapback zapiecie w zaleznosci od stylu",
        "IDEALNY PREZENT - Idealny dla patriotow, na urodziny, swieta lub po prostu jako stylowy dodatek",
        "WSZECHSTRONNA - Idealna na co dzien, sport, podroze, festiwale i codzienne uzytkowanie",
    ],
    "SE": [
        "PREMIUM MASKINBRODERI - Professionellt broderad text pa framsidan. Broderier bleknar inte och rivs inte som tryck",
        "TVA STILAR TILLGANGLIGA - Dad Hat (100% bomull, metallspanne) eller Trucker Cap (bomulls-twill med mesh, snapback)",
        "JUSTERBAR PASSFORM - En storlek for dam och herr. Metall- eller snapback-spanne beroende pa stil",
        "PERFEKT PRESENT - Idealisk for patrioter, fodelsedagar, hogtider eller som stiligt tillbehor",
        "MANGSIDIG - Perfekt for fritid, sport, resor, festivaler och dagligt bruk",
    ],
    "BE": [
        "PREMIUM MACHINEBORDUURWERK - Professioneel geborduurde tekst aan de voorkant. Borduurwerk vervaagt niet en scheurt niet zoals prints",
        "TWEE STIJLEN BESCHIKBAAR - Dad Hat (100% katoen, metalen gesp) of Trucker Pet (katoenen twill met mesh, snapback)",
        "VERSTELBARE PASVORM - One size voor dames en heren. Metalen of snapback-sluiting afhankelijk van de stijl",
        "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of gewoon als stijlvol accessoire",
        "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
    ],
}

PARENT_DESCRIPTIONS = {
    "DE": "Hochwertige bestickte Kappe mit dem Schriftzug Make America Great Again. Erhaeltlich als Dad Hat (100% Baumwolle) oder Trucker Cap (Baumwolle mit Mesh). Professionelle Stickerei, verstellbare Einheitsgroesse.",
    "FR": "Casquette brodee de qualite avec le texte Make America Great Again. Disponible en Dad Hat (100% coton) ou Trucker Cap (coton avec mesh). Broderie professionnelle, taille ajustable.",
    "IT": "Cappellino ricamato di qualita con il testo Make America Great Again. Disponibile come Dad Hat (100% cotone) o Trucker Cap (cotone con mesh). Ricamo professionale, taglia regolabile.",
    "ES": "Gorra bordada de calidad con el texto Make America Great Again. Disponible como Dad Hat (100% algodon) o Trucker Cap (algodon con malla). Bordado profesional, talla ajustable.",
    "NL": "Hoogwaardige geborduurde pet met de tekst Make America Great Again. Beschikbaar als Dad Hat (100% katoen) of Trucker Pet (katoen met mesh). Professioneel borduurwerk, verstelbare pasvorm.",
    "PL": "Wysokiej jakosci haftowana czapka z napisem Make America Great Again. Dostepna jako Dad Hat (100% bawelna) lub Trucker Cap (bawelna z siatka). Profesjonalny haft, regulowany rozmiar.",
    "SE": "Hogkvalitativ broderad keps med texten Make America Great Again. Tillganglig som Dad Hat (100% bomull) eller Trucker Cap (bomull med mesh). Professionellt broderi, justerbar passform.",
    "BE": "Hoogwaardige geborduurde pet met de tekst Make America Great Again. Beschikbaar als Dad Hat (100% katoen) of Trucker Pet (katoen met mesh). Professioneel borduurwerk, verstelbare pasvorm.",
}

PARENT_KEYWORDS = {
    "DE": "bestickte kappe amerika usa patriot geschenk baseball cap dad hat trucker cap snapback mesh baumwolle verstellbar make america great again unisex maga",
    "FR": "casquette brodee amerique usa patriote cadeau dad hat trucker cap snapback mesh coton ajustable make america great again unisex maga",
    "IT": "cappellino ricamato america usa patriota regalo dad hat trucker cap snapback mesh cotone regolabile make america great again unisex maga",
    "ES": "gorra bordada america usa patriota regalo dad hat trucker cap snapback malla algodon ajustable make america great again unisex maga",
    "NL": "geborduurde pet amerika usa patriot cadeau dad hat trucker cap snapback mesh katoen verstelbaar make america great again unisex maga",
    "PL": "haftowana czapka ameryka usa patriota prezent dad hat trucker cap snapback siatka bawelna regulowana make america great again unisex maga",
    "SE": "broderad keps amerika usa patriot present dad hat trucker cap snapback mesh bomull justerbar make america great again unisex maga",
    "BE": "geborduurde pet amerika usa patriot cadeau dad hat trucker cap snapback mesh katoen verstelbaar make america great again unisex maga",
}

PARENT_FABRIC = {
    "DE": "Baumwolle, Polyester Mesh",
    "FR": "Coton, Polyester Mesh",
    "IT": "Cotone, Poliestere Mesh",
    "ES": "Algodon, Poliester Mesh",
    "NL": "Katoen, Polyester Mesh",
    "PL": "Bawelna, Poliester Mesh",
    "SE": "Bomull, Polyester Mesh",
    "BE": "Katoen, Polyester Mesh",
}

PARENT_MATERIAL = {
    "DE": "Baumwolle, Polyester",
    "FR": "Coton, Polyester",
    "IT": "Cotone, Poliestere",
    "ES": "Algodon, Poliester",
    "NL": "Katoen, Polyester",
    "PL": "Bawelna, Poliester",
    "SE": "Bomull, Polyester",
    "BE": "Katoen, Polyester",
}

PARENT_PATTERN = {
    "DE": "Buchstabenmuster", "FR": "Lettres", "IT": "Lettere", "ES": "Letras",
    "NL": "Letters", "PL": "Litery", "SE": "Bokstaver", "BE": "Letters",
}

PARENT_STYLE_ATTR = {
    "DE": "Klassisch", "FR": "Classique", "IT": "Classico", "ES": "Clasico",
    "NL": "Klassiek", "PL": "Klasyczny", "SE": "Klassisk", "BE": "Klassiek",
}

PARENT_AGE = {
    "DE": "Erwachsener", "FR": "Adulte", "IT": "Adulto", "ES": "Adulto",
    "NL": "Volwassene", "PL": "Dorosly", "SE": "Vuxen", "BE": "Volwassene",
}

PARENT_CARE = {
    "DE": "Handwaesche", "FR": "Lavage a la main", "IT": "Lavaggio a mano", "ES": "Lavado a mano",
    "NL": "Handwas", "PL": "Pranie reczne", "SE": "Handtvatt", "BE": "Handwas",
}

SIZE_SYSTEMS = {
    "DE": "as3", "FR": "as4", "IT": "as6", "ES": "as4",
    "NL": "as3", "PL": "as3", "SE": "as3", "BE": "as4",
}


def _build_parent_attrs(mkt_code, mp_id, lang, title):
    """Build full parent attributes for a given marketplace."""
    return {
        "condition_type": [{"value": "new_new", "marketplace_id": mp_id}],
        "parentage_level": [{"value": "parent", "marketplace_id": mp_id}],
        "variation_theme": [{"name": "STYLE_NAME/COLOR_NAME", "marketplace_id": mp_id}],
        "child_parent_sku_relationship": [{"marketplace_id": mp_id, "child_relationship_type": "variation"}],
        "item_name": [{"value": title, "language_tag": lang, "marketplace_id": mp_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mp_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mp_id}],
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mp_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mp_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mp_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mp_id} for b in PARENT_BULLETS[mkt_code]],
        "product_description": [{"value": PARENT_DESCRIPTIONS[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "generic_keyword": [{"value": PARENT_KEYWORDS[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "fabric_type": [{"value": PARENT_FABRIC[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "material": [{"value": PARENT_MATERIAL[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "pattern": [{"value": PARENT_PATTERN[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "style": [{"value": PARENT_STYLE_ATTR[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "age_range_description": [{"value": PARENT_AGE[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "care_instructions": [{"value": PARENT_CARE[mkt_code], "language_tag": lang, "marketplace_id": mp_id}],
        "color": [{"value": "Mehrfarbig" if mkt_code == "DE" else "Multicolor", "language_tag": lang, "marketplace_id": mp_id}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mp_id}],
        "model_name": [{"value": "Make America Great Again Cap", "language_tag": lang, "marketplace_id": mp_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS[mkt_code], "size_class": "alpha", "marketplace_id": mp_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mp_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mp_id}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mp_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mp_id}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mp_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mp_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mp_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mp_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mp_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mp_id}],
        "batteries_required": [{"value": False, "marketplace_id": mp_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mp_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mp_id}],
    }


# =============================================================================
# STEP: CREATE PARENT
# =============================================================================

def step_create_parent(api, results, dry_run=False):
    """Create MEGA-AMERICA parent with STYLE_NAME/COLOR_NAME variation theme on all marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: CREATE PARENT ({NEW_PARENT_SKU}) ON ALL MARKETPLACES")
    print("=" * 70)

    total = 0
    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        title = PARENT_TITLES[mkt_code]

        # Build localized parent attributes
        parent_attrs = _build_parent_attrs(mkt_code, mp_id, lang, title)

        body = {
            "productType": "HAT",
            "requirements": "LISTING",
            "attributes": parent_attrs,
        }

        if dry_run:
            print(f"  [DRY] Would PUT {NEW_PARENT_SKU} on {mkt_code} (parent, STYLE_NAME/COLOR_NAME, title='{title}')")
            total += 1
            continue

        code, resp = api.put_listing(NEW_PARENT_SKU, mp_id, body)
        log_result(results, "create_parent", NEW_PARENT_SKU, mkt_code, code, resp)
        total += 1
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Created parent on {total} marketplaces.")
    return total


# =============================================================================
# STEP: RELINK CHILDREN
# =============================================================================

def step_relink_children(api, results, dry_run=False):
    """
    Re-link all 28+ children to MEGA-AMERICA with proper 2D variation attributes.
    Dad Hat children:   style = "Dad Hat", color = localized color
    Trucker children:   style = "Trucker Cap", color = localized color
    """
    print("\n" + "=" * 70)
    print("  STEP: RELINK ALL CHILDREN TO MEGA-AMERICA")
    print("=" * 70)

    total = 0

    for mkt_code, mp_id in MARKETPLACE_IDS.items():
        lang = LANG_TAGS[mkt_code]
        dad_hat_colors = DAD_HAT_COLORS.get(mkt_code, DAD_HAT_COLORS["DE"])
        trucker_colors = TRUCKER_COLORS.get(mkt_code, TRUCKER_COLORS["DE"])

        print(f"\n  ===== {mkt_code} =====")
        print(f"  style values: '{STYLE_DAD_HAT}' / '{STYLE_TRUCKER}'")

        # --- Dad Hat children ---
        print(f"\n  --- {mkt_code}: {len(DAD_HAT_CHILDREN)} Dad Hat children (style='{STYLE_DAD_HAT}') ---")
        for suffix_id in DAD_HAT_SUFFIXES:
            sku = f"PFT-90202876-{suffix_id}"
            base_color = dad_hat_colors.get(suffix_id, "")
            if not base_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping for suffix {suffix_id}")
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
                    "path": "/attributes/color",
                    "value": [{"value": base_color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": STYLE_DAD_HAT, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/variation_theme",
                    "value": [{"name": "STYLE_NAME/COLOR_NAME"}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{base_color}', style='{STYLE_DAD_HAT}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_dadhat_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

        # --- Trucker children ---
        print(f"\n  --- {mkt_code}: {len(TRUCKER_CHILDREN)} Trucker children (style='{STYLE_TRUCKER}') ---")
        for suffix_id in TRUCKER_SUFFIXES:
            sku = f"PFT-90229846-{suffix_id}"
            base_color = trucker_colors.get(suffix_id, "")
            if not base_color:
                print(f"  [SKIP] {sku} on {mkt_code}: no color mapping for suffix {suffix_id}")
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
                    "path": "/attributes/color",
                    "value": [{"value": base_color, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/style",
                    "value": [{"value": STYLE_TRUCKER, "language_tag": lang, "marketplace_id": mp_id}]
                },
                {
                    "op": "replace",
                    "path": "/attributes/variation_theme",
                    "value": [{"name": "STYLE_NAME/COLOR_NAME"}]
                }
            ]

            if dry_run:
                print(f"  [DRY] {sku}: parent={NEW_PARENT_SKU}, color='{base_color}', style='{STYLE_TRUCKER}'")
                total += 1
                continue

            code, resp = api.patch_listing(sku, mp_id, patches)
            log_result(results, "relink_trucker_child", sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Re-linked {total} children across all marketplaces.")
    return total


# =============================================================================
# STEP: DEACTIVATE OLD PARENTS
# =============================================================================

def step_deactivate_old_parents(api, results, dry_run=False):
    """Deactivate PFT-90202876 and PFT-90229846 parents on all 8 marketplaces."""
    print("\n" + "=" * 70)
    print(f"  STEP: DEACTIVATE OLD PARENTS")
    print(f"  Dad Hat parent: {DAD_HAT_PARENT_SKU}")
    print(f"  Trucker parent: {TRUCKER_PARENT_SKU}")
    print("=" * 70)

    total = 0
    for parent_sku in [DAD_HAT_PARENT_SKU, TRUCKER_PARENT_SKU]:
        for mkt_code, mp_id in MARKETPLACE_IDS.items():
            if dry_run:
                print(f"  [DRY] Would DELETE {parent_sku} on {mkt_code}")
                total += 1
                continue

            code, resp = api.delete_listing(parent_sku, mp_id)
            log_result(results, "deactivate_old_parent", parent_sku, mkt_code, code, resp)
            total += 1
            time.sleep(SLEEP_BETWEEN)

    print(f"\n  Deactivated {total} parent-marketplace combinations.")
    return total


# =============================================================================
# STEP: VERIFY
# =============================================================================

def step_verify(api):
    """Verify the merged 2D variation is correctly set up on DE."""
    print("\n" + "=" * 70)
    print("  STEP: VERIFY 2D VARIATION ON DE")
    print("=" * 70)

    de_mp = MARKETPLACE_IDS["DE"]

    # Check new parent
    print(f"\n  --- New parent: {NEW_PARENT_SKU} ---")
    code, data = api.get_listing(NEW_PARENT_SKU, de_mp)
    if code == 200 and data:
        attrs = data.get("attributes", {})
        print(f"  variation_theme: {json.dumps(attrs.get('variation_theme', []))}")
        print(f"  parentage_level: {json.dumps(attrs.get('parentage_level', []))}")
        summaries = data.get("summaries", [])
        if summaries:
            s = summaries[0]
            print(f"  ASIN: {s.get('asin','?')}, status: {json.dumps(s.get('status', []))}")
    time.sleep(SLEEP_BETWEEN)

    # Check old parents should be gone
    for old_parent in [DAD_HAT_PARENT_SKU, TRUCKER_PARENT_SKU]:
        print(f"\n  --- Old parent: {old_parent} (should be deactivated) ---")
        code, data = api.get_listing(old_parent, de_mp)
        if code == 200 and data:
            summaries = data.get("summaries", [])
            if summaries:
                print(f"  ASIN: {summaries[0].get('asin','?')}, status: {json.dumps(summaries[0].get('status', []))}")
            else:
                print(f"  No summaries (likely deleted)")
        else:
            print(f"  HTTP {code} (expected -- deleted)")
        time.sleep(SLEEP_BETWEEN)

    # Verify all Dad Hat children
    print(f"\n  --- Dad Hat children ---")
    ok_count = 0
    err_count = 0
    for sku in DAD_HAT_CHILDREN:
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            parent = attrs.get("child_parent_sku_relationship", [{}])[0].get("parent_sku", "?") if attrs.get("child_parent_sku_relationship") else "?"
            color = attrs.get("color", [{}])[0].get("value", "?") if attrs.get("color") else "?"
            style = attrs.get("style", [{}])[0].get("value", "?") if attrs.get("style") else "?"
            summaries = data.get("summaries", [])
            status = summaries[0].get("status", "?") if summaries else "?"

            is_ok = parent == NEW_PARENT_SKU and style == STYLE_DAD_HAT
            icon = "OK" if is_ok else "ERR"
            if is_ok:
                ok_count += 1
            else:
                err_count += 1
            print(f"  [{icon}] {sku}: parent={parent}, style='{style}', color='{color}', status={status}")
        else:
            err_count += 1
            print(f"  [ERR] {sku}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    # Verify all Trucker children
    print(f"\n  --- Trucker children ---")
    for sku in TRUCKER_CHILDREN:
        code, data = api.get_listing(sku, de_mp)
        if code == 200 and data:
            attrs = data.get("attributes", {})
            parent = attrs.get("child_parent_sku_relationship", [{}])[0].get("parent_sku", "?") if attrs.get("child_parent_sku_relationship") else "?"
            color = attrs.get("color", [{}])[0].get("value", "?") if attrs.get("color") else "?"
            style = attrs.get("style", [{}])[0].get("value", "?") if attrs.get("style") else "?"
            summaries = data.get("summaries", [])
            status = summaries[0].get("status", "?") if summaries else "?"

            is_ok = parent == NEW_PARENT_SKU and style == STYLE_TRUCKER
            icon = "OK" if is_ok else "ERR"
            if is_ok:
                ok_count += 1
            else:
                err_count += 1
            print(f"  [{icon}] {sku}: parent={parent}, style='{style}', color='{color}', status={status}")
        else:
            err_count += 1
            print(f"  [ERR] {sku}: HTTP {code}")
        time.sleep(SLEEP_BETWEEN)

    print(f"\n  Verification: {ok_count} OK, {err_count} errors")
    print("  Verify complete.\n")


# =============================================================================
# REPORT
# =============================================================================

def print_report(results):
    """Print summary report and save to JSON."""
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

    # Summaries
    dadhat_relinked = sum(1 for r in results
                          if r["action"] == "relink_dadhat_child"
                          and r["http_status"] in (200, 204) and not r["errors"])
    trucker_relinked = sum(1 for r in results
                           if r["action"] == "relink_trucker_child"
                           and r["http_status"] in (200, 204) and not r["errors"])
    parents_created = sum(1 for r in results
                          if r["action"] == "create_parent"
                          and r["http_status"] in (200, 204) and not r["errors"])
    deactivated = sum(1 for r in results
                      if r["action"] == "deactivate_old_parent"
                      and r["http_status"] in (200, 204) and not r["errors"])

    print(f"\n  Summary:")
    print(f"    Parent created on: {parents_created}/8 marketplaces")
    print(f"    Dad Hat children re-linked: {dadhat_relinked}/{len(DAD_HAT_CHILDREN) * 8}")
    print(f"    Trucker children re-linked: {trucker_relinked}/{len(TRUCKER_CHILDREN) * 8}")
    print(f"    Old parents deactivated: {deactivated}/16")
    print(f"    Total API calls: {len(results)}")

    # Save results
    results_path = Path(__file__).parent / "merge_america_results.json"
    with open(results_path, "w") as f:
        json.dump({"timestamp": datetime.now().isoformat(), "results": results}, f, indent=2)
    print(f"  Results saved to: {results_path}")


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Merge America Dad Hat + Trucker Cap into 2D variation (STYLE_NAME/COLOR_NAME)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print what would happen without making API calls")
    parser.add_argument("--step",
                        choices=["inspect", "create-parent", "relink", "deactivate", "verify"],
                        help="Run only a specific step")
    args = parser.parse_args()

    print("=" * 70)
    print("  MERGE AMERICA: Dad Hat + Trucker Cap -> 2D Variation")
    print(f"  New parent (create): {NEW_PARENT_SKU}")
    print(f"  Dad Hat parent (deactivate): {DAD_HAT_PARENT_SKU}")
    print(f"  Trucker parent (deactivate): {TRUCKER_PARENT_SKU}")
    print(f"  Dad Hat children (style -> '{STYLE_DAD_HAT}'): {len(DAD_HAT_CHILDREN)} SKUs")
    print(f"  Trucker children (style -> '{STYLE_TRUCKER}'): {len(TRUCKER_CHILDREN)} SKUs")
    print(f"  Total children: {len(DAD_HAT_CHILDREN) + len(TRUCKER_CHILDREN)}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Marketplaces: {', '.join(MARKETPLACE_IDS.keys())}")
    print("=" * 70)

    api = AmazonAPI(AMZ_CREDS)
    results = []

    if args.step == "inspect" or args.step is None:
        step_inspect(api)
        if args.step == "inspect":
            return

    if args.step == "create-parent" or args.step is None:
        step_create_parent(api, results, args.dry_run)
        if args.step == "create-parent":
            print_report(results)
            return

    if args.step == "relink" or args.step is None:
        step_relink_children(api, results, args.dry_run)
        if args.step == "relink":
            print_report(results)
            return

    if args.step == "deactivate" or args.step is None:
        step_deactivate_old_parents(api, results, args.dry_run)
        if args.step == "deactivate":
            print_report(results)
            return

    if args.step == "verify":
        step_verify(api)
        return

    # Full run -- verify at the end
    step_verify(api)
    print_report(results)


if __name__ == "__main__":
    main()
