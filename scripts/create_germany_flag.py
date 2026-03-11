#!/usr/bin/env python3.11
"""
Full pipeline: Printful template 100032925 (Make Germany Great Again with Flag)
  -> Baselinker inventory 52954
  -> Amazon listings on 8 EU marketplaces

Steps:
  1. Get template details from Printful v2 API
  2. Generate mockups for all variants, upload to Printful File API for permanent CDN URLs
  3. Add products to Baselinker inventory 52954
  4. Create Amazon parent+child listings on DE, then replicate to 7 other marketplaces
  5. Upload images to Amazon via Listings API PATCH
  6. Report results

Usage:
  cd ~/nesell-analytics
  python3.11 scripts/create_germany_flag.py
  python3.11 scripts/create_germany_flag.py --step printful   # only step 1-2
  python3.11 scripts/create_germany_flag.py --step baselinker # only step 3
  python3.11 scripts/create_germany_flag.py --step amazon     # only steps 4-5
  python3.11 scripts/create_germany_flag.py --dry-run         # no API calls
"""

import argparse
import json
import os
import requests
import sys
import time
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

# ── Credentials ──────────────────────────────────────────────────────
KEYS_DIR = Path.home() / ".keys"

def load_env(path):
    vals = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                vals[k.strip()] = v.strip()
    return vals

PF_ENV = load_env(KEYS_DIR / "printful.env")
PF_TOKEN = PF_ENV.get("PRINTFUL_API_TOKEN_V2", "")
PF_STORE_ID = PF_ENV.get("PRINTFUL_STORE_ID", "15269225")

BL_TOKEN = load_env(KEYS_DIR / "baselinker.env").get("BASELINKER_API_TOKEN", "")

AMZ_CREDS = json.loads((KEYS_DIR / "amazon-sp-api.json").read_text())
SELLER_ID = AMZ_CREDS["seller_id"]
AMZ_BASE = "https://sellingpartnerapi-eu.amazon.com"

# ── Constants ────────────────────────────────────────────────────────
TEMPLATE_ID = 100032925
BL_INVENTORY_ID = 52954

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

# Flag hat prices (29.99 EUR base, converted for PL and SE)
FLAG_HAT_PRICES = {
    "DE": 29.99, "FR": 29.99, "IT": 29.99, "ES": 29.99,
    "NL": 29.99, "PL": 139.99, "SE": 349.00, "BE": 29.99,
}

# Browse nodes for hats/caps per marketplace
BROWSE_NODES = {
    "DE": "1981316031", "FR": "1981316031", "IT": "1981316031", "ES": "1981316031",
    "NL": "1981316031", "PL": "1981316031", "SE": "1981316031", "BE": "1981316031",
}

# ── Color Mappings (Printful variant suffix -> localized names) ──────
# Dad Hat (Yupoong 6245CM) variant suffixes
COLOR_EN = {
    "7853": "White", "7854": "Black", "7855": "Khaki", "7856": "Light Blue",
    "7857": "Navy", "7858": "Pink", "7859": "Stone", "8745": "Spruce",
    "9794": "Green Camo", "12735": "Cranberry", "12736": "Dark Grey",
}

COLORS = {
    "DE": {
        "7853": "Weiss", "7854": "Schwarz", "7855": "Beige", "7856": "Hellblau",
        "7857": "Marineblau", "7858": "Rosa", "7859": "Steingrau", "8745": "Tannengruen",
        "9794": "Gruenes Tarnmuster", "12735": "Cranberry", "12736": "Dunkelgrau",
    },
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre", "8745": "Vert Sapin",
        "9794": "Camouflage Vert", "12735": "Cranberry", "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige", "7856": "Azzurro",
        "7857": "Blu Navy", "7858": "Rosa", "7859": "Grigio Pietra", "8745": "Verde Abete",
        "9794": "Mimetico Verde", "12735": "Cranberry", "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige", "7856": "Azul Claro",
        "7857": "Azul Marino", "7858": "Rosa", "7859": "Gris Piedra", "8745": "Verde Abeto",
        "9794": "Camuflaje Verde", "12735": "Arandano", "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige", "7856": "Lichtblauw",
        "7857": "Marineblauw", "7858": "Roze", "7859": "Steengrijs", "8745": "Spargroen",
        "9794": "Groen Camouflage", "12735": "Cranberry", "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy", "7856": "Jasnoniebieski",
        "7857": "Granatowy", "7858": "Rozowy", "7859": "Szary Kamien", "8745": "Ciemnozielony",
        "9794": "Zielony Kamuflaz", "12735": "Zurawinowy", "12736": "Ciemnoszary",
    },
    "SE": {
        "7853": "Vit", "7854": "Svart", "7855": "Beige", "7856": "Ljusbla",
        "7857": "Marinbla", "7858": "Rosa", "7859": "Stengra", "8745": "Grangren",
        "9794": "Gron kamouflage", "12735": "Tranbar", "12736": "Morkgra",
    },
    "BE": {  # same as FR
        "7853": "Blanc", "7854": "Noir", "7855": "Beige", "7856": "Bleu Clair",
        "7857": "Bleu Marine", "7858": "Rose", "7859": "Gris Pierre", "8745": "Vert Sapin",
        "9794": "Camouflage Vert", "12735": "Cranberry", "12736": "Gris Fonce",
    },
}

# ── Localized content per marketplace ────────────────────────────────
TRANSLATIONS = {
    "DE": {
        "item_name_parent": "Bestickte Kappe Make Germany Great Again mit Flagge - Verstellbare Dad Hat aus Baumwolle Unisex Baseball Cap",
        "item_name_child": "Bestickte Kappe Make Germany Great Again mit Flagge - Verstellbare Dad Hat Unisex - {color}",
        "bullets": [
            'PREMIUM MASCHINENSTICKEREI - Professionell gestickter Schriftzug "Make Germany Great Again" mit deutscher Flagge auf der Vorderseite. Stickerei verblasst nicht und reisst nicht',
            "BEQUEM FUER DEN ALLTAG - Aus 100% weicher Baumwolle Chino Twill. Niedriges Profil und vorgebogener Schirm fuer den klassischen Dad-Hat-Look",
            "VERSTELLBARE PASSFORM - Metallschnallenverschluss hinten passt sich jedem Kopfumfang an. Einheitsgroesse fuer Damen und Herren",
            "PERFEKTES GESCHENK - Ideales Geschenk fuer Patrioten, Geburtstage, Feiertage oder einfach als Statement-Accessoire fuer Deutschland-Fans",
            "VIELSEITIG EINSETZBAR - Perfekt fuer Freizeit, Sport, Reisen, Festivals und den taeglichen Gebrauch bei jedem Wetter",
        ],
        "description": "Diese hochwertige bestickte Baseball Cap mit dem Schriftzug Make Germany Great Again und deutscher Flagge ist der perfekte Begleiter fuer jeden Tag. Gefertigt aus 100% Baumwolle Chino Twill bietet sie hervorragenden Tragekomfort und Atmungsaktivitaet.",
        "keywords": "bestickte kappe deutschland patriot geschenk baseball cap dad hat baumwolle verstellbar make germany great again flagge unisex deutsche fahne",
        "pattern": "Buchstabenmuster",
        "age": "Erwachsener",
        "care": "Handwaesche",
        "fabric": "100% Cotton",
        "material": "Baumwolle",
        "style": "Klassisch",
        "unit_type": "stueck",
    },
    "FR": {
        "item_name_parent": "Casquette Brodee Make Germany Great Again avec Drapeau - Dad Hat Reglable Unisex Coton Baseball Cap",
        "item_name_child": "Casquette Brodee Make Germany Great Again avec Drapeau - Dad Hat Reglable Unisex - {color}",
        "bullets": [
            'BRODERIE MACHINE PREMIUM - Texte "Make Germany Great Again" avec drapeau allemand brode professionnellement sur le devant. La broderie ne decolore pas et ne se dechire pas',
            "CONFORTABLE AU QUOTIDIEN - 100% coton chino twill doux et respirant. Profil bas et visiere pre-courbee pour le look classique Dad Hat",
            "TAILLE AJUSTABLE - Boucle metallique a l arriere pour un ajustement parfait. Taille unique pour femme et homme",
            "CADEAU PARFAIT - Ideal pour les patriotes, anniversaires, fetes ou comme accessoire tendance pour les fans de l Allemagne",
            "POLYVALENTE - Parfaite pour les loisirs, le sport, les voyages, les festivals et un usage quotidien",
        ],
        "description": "Cette casquette brodee de qualite avec le texte Make Germany Great Again et drapeau allemand est le compagnon parfait. En coton twill, broderie durable, boucle metallique reglable.",
        "keywords": "casquette brodee allemagne patriote cadeau baseball cap dad hat coton reglable make germany great again drapeau unisexe",
        "pattern": "Lettres",
        "age": "Adulte",
        "care": "Lavage a la main",
        "fabric": "100% Coton",
        "material": "Coton",
        "style": "Classique",
        "unit_type": "piece",
    },
    "IT": {
        "item_name_parent": "Cappellino Ricamato Make Germany Great Again con Bandiera - Dad Hat Regolabile Unisex Cotone Baseball Cap",
        "item_name_child": "Cappellino Ricamato Make Germany Great Again con Bandiera - Dad Hat Regolabile Unisex - {color}",
        "bullets": [
            'RICAMO A MACCHINA PREMIUM - Testo "Make Germany Great Again" con bandiera tedesca ricamato professionalmente. Il ricamo non sbiadisce e non si strappa',
            "COMODO PER OGNI GIORNO - 100% cotone chino twill morbido e traspirante. Profilo basso e visiera pre-curvata per il look classico Dad Hat",
            "TAGLIA REGOLABILE - Fibbia in metallo sul retro per una regolazione perfetta. Taglia unica per donna e uomo",
            "REGALO PERFETTO - Ideale per patrioti, compleanni, festivita o come accessorio trendy per i fan della Germania",
            "VERSATILE - Perfetto per tempo libero, sport, viaggi, festival e uso quotidiano",
        ],
        "description": "Questo cappellino ricamato di qualita con il testo Make Germany Great Again e bandiera tedesca e il compagno perfetto. In cotone twill, ricamo durevole, fibbia metallica regolabile.",
        "keywords": "cappellino ricamato germania patriota regalo baseball cap dad hat cotone regolabile make germany great again bandiera unisex",
        "pattern": "Lettere",
        "age": "Adulto",
        "care": "Lavaggio a mano",
        "fabric": "100% Cotone",
        "material": "Cotone",
        "style": "Classico",
        "unit_type": "pezzo",
    },
    "ES": {
        "item_name_parent": "Gorra Bordada Make Germany Great Again con Bandera - Dad Hat Ajustable Unisex Algodon Baseball Cap",
        "item_name_child": "Gorra Bordada Make Germany Great Again con Bandera - Dad Hat Ajustable Unisex - {color}",
        "bullets": [
            'BORDADO A MAQUINA PREMIUM - Texto "Make Germany Great Again" con bandera alemana bordado profesionalmente. El bordado no destine y no se rompe',
            "COMODA PARA EL DIA A DIA - 100% algodon chino twill suave y transpirable. Perfil bajo y visera pre-curvada para el look clasico Dad Hat",
            "TALLA AJUSTABLE - Hebilla metalica en la parte trasera para un ajuste perfecto. Talla unica para mujer y hombre",
            "REGALO PERFECTO - Ideal para patriotas, cumpleanos, fiestas o como accesorio de moda para fans de Alemania",
            "VERSATIL - Perfecta para ocio, deporte, viajes, festivales y uso diario",
        ],
        "description": "Esta gorra bordada de calidad con el texto Make Germany Great Again y bandera alemana es el companero perfecto. En algodon twill, bordado duradero, hebilla metalica ajustable.",
        "keywords": "gorra bordada alemania patriota regalo baseball cap dad hat algodon ajustable make germany great again bandera unisex",
        "pattern": "Letras",
        "age": "Adulto",
        "care": "Lavado a mano",
        "fabric": "100% Algodon",
        "material": "Algodon",
        "style": "Clasico",
        "unit_type": "pieza",
    },
    "NL": {
        "item_name_parent": "Geborduurde Pet Make Germany Great Again met Vlag - Verstelbare Dad Hat Unisex Katoen Baseball Cap",
        "item_name_child": "Geborduurde Pet Make Germany Great Again met Vlag - Verstelbare Dad Hat Unisex - {color}",
        "bullets": [
            'PREMIUM MACHINEBORDUURWERK - Tekst "Make Germany Great Again" met Duitse vlag professioneel geborduurd. Borduurwerk vervaagt niet en scheurt niet',
            "COMFORTABEL VOOR ELKE DAG - 100% katoenen chino twill zacht en ademend. Laag profiel en voorgebogen klep voor de klassieke Dad Hat-look",
            "VERSTELBARE PASVORM - Metalen gesp achter voor een perfecte pasvorm. One size voor dames en heren",
            "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of als stijlvol accessoire voor Duitsland-fans",
            "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
        ],
        "description": "Deze hoogwaardige geborduurde pet met de tekst Make Germany Great Again en Duitse vlag is de perfecte metgezel. Van katoenen twill, duurzaam borduurwerk, verstelbare metalen gesp.",
        "keywords": "geborduurde pet duitsland patriot cadeau baseball cap dad hat katoen verstelbaar make germany great again vlag unisex",
        "pattern": "Letters",
        "age": "Volwassene",
        "care": "Handwas",
        "fabric": "100% Katoen",
        "material": "Katoen",
        "style": "Klassiek",
        "unit_type": "stuk",
    },
    "PL": {
        "item_name_parent": "Czapka z Haftem Make Germany Great Again z Flaga - Regulowana Dad Hat Unisex Bawelna Baseball Cap",
        "item_name_child": "Czapka z Haftem Make Germany Great Again z Flaga - Regulowana Dad Hat Unisex - {color}",
        "bullets": [
            'HAFT MASZYNOWY PREMIUM - Napis "Make Germany Great Again" z niemiecka flaga profesjonalnie wyhaftowany. Haft nie blaknie i nie rwie sie',
            "WYGODNA NA CO DZIEN - 100% bawelna chino twill miekka i oddychajaca. Niski profil i wstepnie wygiety daszek dla klasycznego looku Dad Hat",
            "REGULOWANY ROZMIAR - Metalowa klamra z tylu dla idealnego dopasowania. Rozmiar uniwersalny dla kobiet i mezczyzn",
            "IDEALNY PREZENT - Idealny dla patriotow, na urodziny, swieta lub jako modny dodatek dla fanow Niemiec",
            "WSZECHSTRONNA - Idealna na co dzien, sport, podroze, festiwale i codzienne uzytkowanie",
        ],
        "description": "Ta wysokiej jakosci haftowana czapka z napisem Make Germany Great Again i niemiecka flaga to idealny towarzysz. Z bawelnianego twill, trwaly haft, regulowana metalowa klamra.",
        "keywords": "haftowana czapka niemcy patriota prezent baseball cap dad hat bawelna regulowana make germany great again flaga unisex",
        "pattern": "Litery",
        "age": "Dorosly",
        "care": "Pranie reczne",
        "fabric": "100% Bawelna",
        "material": "Bawelna",
        "style": "Klasyczny",
        "unit_type": "sztuka",
    },
    "SE": {
        "item_name_parent": "Broderad Keps Make Germany Great Again med Flagga - Justerbar Dad Hat Unisex Bomull Baseball Keps",
        "item_name_child": "Broderad Keps Make Germany Great Again med Flagga - Justerbar Dad Hat Unisex - {color}",
        "bullets": [
            'PREMIUM MASKINBRODERI - Texten "Make Germany Great Again" med tysk flagga professionellt broderad. Broderier bleknar inte och rivs inte',
            "BEKVAM FOR VARDAGEN - 100% bomull chino twill mjuk och andningsbar. Lag profil och forbojd skarm for klassisk Dad Hat-look",
            "JUSTERBAR PASSFORM - Metallspanne bak for perfekt passform. En storlek for dam och herr",
            "PERFEKT PRESENT - Idealisk for patrioter, fodelsedagar, hogtider eller som tillbehor for Tyskland-fans",
            "MANGSIDIG - Perfekt for fritid, sport, resor, festivaler och dagligt bruk",
        ],
        "description": "Denna hogkvalitativa broderade keps med texten Make Germany Great Again och tysk flagga ar den perfekta foljeslagaren. 100% bomull chino twill, hallbart broderi, justerbbart metallspanne.",
        "keywords": "broderad keps tyskland patriot present baseball cap dad hat bomull justerbar make germany great again flagga unisex",
        "pattern": "Bokstaver",
        "age": "Vuxen",
        "care": "Handtvatt",
        "fabric": "100% Bomull",
        "material": "Bomull",
        "style": "Klassisk",
        "unit_type": "piece",
    },
    "BE": {  # Uses nl_BE but content matches FR
        "item_name_parent": "Geborduurde Pet Make Germany Great Again met Vlag - Verstelbare Dad Hat Unisex Katoen Baseball Cap",
        "item_name_child": "Geborduurde Pet Make Germany Great Again met Vlag - Verstelbare Dad Hat Unisex - {color}",
        "bullets": [
            'PREMIUM MACHINEBORDUURWERK - Tekst "Make Germany Great Again" met Duitse vlag professioneel geborduurd. Borduurwerk vervaagt niet en scheurt niet',
            "COMFORTABEL VOOR ELKE DAG - 100% katoenen chino twill zacht en ademend. Laag profiel en voorgebogen klep voor de klassieke Dad Hat-look",
            "VERSTELBARE PASVORM - Metalen gesp achter voor een perfecte pasvorm. One size voor dames en heren",
            "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of als stijlvol accessoire voor Duitsland-fans",
            "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
        ],
        "description": "Deze hoogwaardige geborduurde pet met de tekst Make Germany Great Again en Duitse vlag is de perfecte metgezel. Van katoenen twill, duurzaam borduurwerk, verstelbare metalen gesp.",
        "keywords": "geborduurde pet duitsland patriot cadeau baseball cap dad hat katoen verstelbaar make germany great again vlag unisex",
        "pattern": "Letters",
        "age": "Volwassene",
        "care": "Handwas",
        "fabric": "100% Katoen",
        "material": "Katoen",
        "style": "Klassiek",
        "unit_type": "stuk",
    },
}


# ═══════════════════════════════════════════════════════════════════════
# PRINTFUL API
# ═══════════════════════════════════════════════════════════════════════

class PrintfulAPI:
    BASE = "https://api.printful.com/v2"

    def __init__(self, token, store_id):
        self.token = token
        self.store_id = store_id

    def _headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "X-PF-Store-Id": self.store_id,
            "Content-Type": "application/json",
        }

    def get(self, path, params=None, retries=3):
        for attempt in range(retries):
            try:
                r = requests.get(f"{self.BASE}{path}", headers=self._headers(),
                                 params=params, timeout=30)
            except Exception as e:
                print(f"  [PrintfulAPI] GET {path} error: {e}")
                time.sleep(3)
                continue
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

    def post(self, path, body=None, retries=3):
        for attempt in range(retries):
            try:
                r = requests.post(f"{self.BASE}{path}", headers=self._headers(),
                                  json=body or {}, timeout=30)
            except Exception as e:
                print(f"  [PrintfulAPI] POST {path} error: {e}")
                time.sleep(3)
                continue
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}

    def get_template(self, template_id):
        """Get product template details."""
        status, data = self.get(f"/product-templates/{template_id}")
        return data if status == 200 else {}

    def create_mockup_task(self, catalog_product_id, variant_ids, files):
        """Create a mockup generation task."""
        body = {
            "variant_ids": variant_ids,
            "format": "jpg",
            "files": files,
        }
        status, data = self.post(f"/mockup-generator/create-task/{catalog_product_id}", body)
        return data if status == 200 else {}

    def get_mockup_task(self, task_key):
        """Check mockup task status."""
        status, data = self.get(f"/mockup-generator/task/{task_key}")
        return data if status == 200 else {}

    def upload_file(self, url):
        """Upload a file to Printful File API (v1 endpoint)."""
        v1_url = "https://api.printful.com/files"
        headers = {"Authorization": f"Bearer {self.token}"}
        body = {"url": url}
        try:
            r = requests.post(v1_url, headers=headers, json=body, timeout=60)
            return r.json() if r.status_code == 200 else {}
        except Exception as e:
            print(f"  [PrintfulAPI] File upload error: {e}")
            return {}


# ═══════════════════════════════════════════════════════════════════════
# BASELINKER API
# ═══════════════════════════════════════════════════════════════════════

class BaselinkerAPI:
    BASE = "https://api.baselinker.com/connector.php"

    def __init__(self, token):
        self.token = token

    def call(self, method, params=None, retries=3):
        body = {"token": self.token, "method": method}
        if params:
            body["parameters"] = json.dumps(params)
        for attempt in range(retries):
            try:
                r = requests.post(self.BASE, data=body, timeout=30)
            except Exception as e:
                print(f"  [BL] {method} error: {e}")
                time.sleep(3)
                continue
            if r.status_code == 429:
                time.sleep(5 * (attempt + 1))
                continue
            return r.json()
        return {"status": "ERROR"}

    def add_inventory_product(self, inventory_id, product_id="0", **kwargs):
        params = {"inventory_id": str(inventory_id), "product_id": str(product_id)}
        params.update(kwargs)
        return self.call("addInventoryProduct", params)

    def get_inventory_products_list(self, inventory_id, filter_sku=None):
        params = {"inventory_id": inventory_id}
        if filter_sku:
            params["filter_text"] = filter_sku
        return self.call("getInventoryProductsList", params)


# ═══════════════════════════════════════════════════════════════════════
# AMAZON SP-API
# ═══════════════════════════════════════════════════════════════════════

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
            print(f"  [AMZ AUTH ERROR] {data}")

    def _headers(self):
        if not self._token or time.time() - self._token_time > 3000:
            self._refresh()
        return {"x-amz-access-token": self._token, "Content-Type": "application/json"}

    def put_listing(self, sku, mp_id, attributes, product_type="HAT", retries=8):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{quote(sku, safe='')}"
        body = {"productType": product_type, "requirements": "LISTING", "attributes": attributes}
        for attempt in range(retries):
            try:
                r = requests.put(url, headers=self._headers(), json=body,
                                 params={"marketplaceIds": mp_id, "issueLocale": "en_US"}, timeout=30)
            except Exception:
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429:
                time.sleep(min(5 * (2 ** attempt), 60))
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
                issue_msgs = [f"{i.get('code','')}: {i.get('message','')[:100]}" for i in errors[:3]]
                print(f"    [{r.status_code}] PUT {sku} -> {status_str} | {'; '.join(issue_msgs)}")
            else:
                print(f"    [{r.status_code}] PUT {sku} -> {status_str}")
            return r.status_code, resp
        return 0, {"error": "retries exhausted"}

    def patch_listing(self, sku, mp_id, patches, product_type="HAT", retries=8):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{quote(sku, safe='')}"
        body = {"productType": product_type, "patches": patches}
        for attempt in range(retries):
            try:
                r = requests.patch(url, headers=self._headers(), json=body,
                                   params={"marketplaceIds": mp_id, "issueLocale": "en_US"}, timeout=30)
            except Exception:
                time.sleep(5 * (attempt + 1))
                continue
            if r.status_code == 429:
                time.sleep(min(5 * (2 ** attempt), 60))
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

    def get_listing(self, sku, mp_id, retries=4):
        url = f"{AMZ_BASE}/listings/2021-08-01/items/{SELLER_ID}/{quote(sku, safe='')}"
        for attempt in range(retries):
            try:
                r = requests.get(url, headers=self._headers(),
                                 params={"marketplaceIds": mp_id, "issueLocale": "en_US",
                                         "includedData": "summaries,attributes"}, timeout=30)
            except Exception:
                time.sleep(3)
                continue
            if r.status_code == 429:
                time.sleep(min(3 * (2 ** attempt), 30))
                continue
            if r.status_code == 403:
                self._refresh()
                time.sleep(1)
                continue
            return r.status_code, r.json() if r.text else {}
        return 0, {}


# ═══════════════════════════════════════════════════════════════════════
# LISTING BUILDER
# ═══════════════════════════════════════════════════════════════════════

def build_parent_attrs(mkt_code):
    """Build Amazon parent listing attributes for a marketplace."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    currency = CURRENCIES[mkt_code]
    price = FLAG_HAT_PRICES[mkt_code]
    trans = TRANSLATIONS[mkt_code]

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": "Mehrfarbig" if mkt_code == "DE" else ("Flerfargad" if mkt_code == "SE" else "Multicolor"), "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": "Make Germany Great Again Dad Hat", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": BROWSE_NODES[mkt_code], "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS[mkt_code], "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": trans["item_name_parent"], "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": price, "currency": currency, "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "fabric_type": [{"value": trans["fabric"], "language_tag": lang, "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "material": [{"value": trans["material"], "language_tag": lang, "marketplace_id": mkt_id}],
        "style": [{"value": trans["style"], "language_tag": lang, "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt_id}],
        "care_instructions": [{"value": trans["care"], "language_tag": lang, "marketplace_id": mkt_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mkt_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt_id}],
        "parentage_level": [{"marketplace_id": mkt_id, "value": "parent"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt_id, "child_relationship_type": "variation"}],
    }
    return attrs


def build_child_attrs(mkt_code, parent_sku, variant_suffix, image_urls=None):
    """Build Amazon child listing attributes for a marketplace."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    currency = CURRENCIES[mkt_code]
    price = FLAG_HAT_PRICES[mkt_code]
    trans = TRANSLATIONS[mkt_code]

    color_local = COLORS.get(mkt_code, COLORS["DE"]).get(variant_suffix, "")
    # For color attribute value, use DE color name as standardized value (Amazon uses this for matching)
    color_de = COLORS["DE"].get(variant_suffix, color_local)

    item_name = trans["item_name_child"].format(color=color_local)

    # Sale price: ~15% off list price
    sale_price = round(price * 0.85, 2)

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": color_de, "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": "Make Germany Great Again Dad Hat", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": BROWSE_NODES[mkt_code], "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS[mkt_code], "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": price, "currency": currency, "marketplace_id": mkt_id}],
        "purchasable_offer": [{"currency": currency, "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": sale_price}]}], "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "fabric_type": [{"value": trans["fabric"], "language_tag": lang, "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "material": [{"value": trans["material"], "language_tag": lang, "marketplace_id": mkt_id}],
        "style": [{"value": trans["style"], "language_tag": lang, "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt_id}],
        "care_instructions": [{"value": trans["care"], "language_tag": lang, "marketplace_id": mkt_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mkt_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt_id}],
        "parentage_level": [{"marketplace_id": mkt_id, "value": "child"}],
        "child_parent_sku_relationship": [{"marketplace_id": mkt_id, "child_relationship_type": "variation", "parent_sku": parent_sku}],
    }

    # Add images if available
    if image_urls:
        if len(image_urls) > 0:
            attrs["main_product_image_locator"] = [{"media_location": image_urls[0], "marketplace_id": mkt_id}]
        for i, url in enumerate(image_urls[1:8], 1):
            attrs[f"other_product_image_locator_{i}"] = [{"media_location": url, "marketplace_id": mkt_id}]

    return attrs


# ═══════════════════════════════════════════════════════════════════════
# PIPELINE STEPS
# ═══════════════════════════════════════════════════════════════════════

def step1_printful_template(pf):
    """Step 1: Get template details from Printful."""
    print("\n" + "=" * 70)
    print("  STEP 1: Get Printful Template Details")
    print("=" * 70)

    data = pf.get_template(TEMPLATE_ID)
    if not data:
        print("  ERROR: Could not get template from Printful")
        return None

    template = data.get("data", data)
    if isinstance(template, dict) and "result" in template:
        template = template["result"]

    print(f"  Template ID: {TEMPLATE_ID}")

    # Extract variant info
    variants = template.get("variants", template.get("data", {}).get("variants", []))
    if not variants and "data" in template:
        inner = template["data"]
        if isinstance(inner, dict):
            variants = inner.get("variants", [])

    # Also check for placement info
    placements = template.get("placements", template.get("data", {}).get("placements", []))

    print(f"  Variants found: {len(variants)}")
    print(f"  Full response keys: {list(template.keys()) if isinstance(template, dict) else 'not a dict'}")

    # Save raw response for debugging
    results_path = Path(__file__).parent / "germany_flag_template.json"
    with open(results_path, "w") as f:
        json.dump(data, f, indent=2, default=str)
    print(f"  Raw response saved to: {results_path}")

    return data


def step2_mockups_and_images(pf, template_data):
    """Step 2: Generate mockups and collect image URLs."""
    print("\n" + "=" * 70)
    print("  STEP 2: Collect Mockup/Image URLs")
    print("=" * 70)

    # Parse template data to find variant images
    variant_images = {}  # suffix -> [url1, url2, ...]

    # Navigate to the actual template content
    if isinstance(template_data, dict):
        # Try different nesting levels
        template = template_data
        if "data" in template:
            template = template["data"]
        if "result" in template:
            template = template["result"]
        if "data" in template and isinstance(template["data"], dict):
            template = template["data"]

        # Get variants
        variants = template.get("variants", [])
        if not variants:
            # Try to find in nested structure
            for key in ("catalog_variants", "sync_variants", "variant_ids"):
                if key in template:
                    variants = template[key]
                    break

        print(f"  Found {len(variants)} variants in template data")

        # Check for template-level images/mockups/previews
        for key in ("thumbnail_url", "image_url", "preview_url"):
            if key in template and template[key]:
                print(f"  Template {key}: {template[key]}")

        # Extract images from template placements/options
        placements = template.get("placements", [])
        print(f"  Found {len(placements)} placements")

        # Look for mockup images in variant data
        for v in variants:
            v_id = str(v.get("id", v.get("variant_id", "")))
            images = []

            # Check for preview/mockup URLs at variant level
            for img_key in ("preview_url", "thumbnail_url", "mockup_url", "image_url",
                            "product_image", "variant_image"):
                url = v.get(img_key, "")
                if url and url not in images:
                    images.append(url)

            # Check for files/images arrays
            for arr_key in ("files", "images", "mockups"):
                arr = v.get(arr_key, [])
                for item in arr:
                    if isinstance(item, dict):
                        for url_key in ("preview_url", "thumbnail_url", "url", "image_url"):
                            u = item.get(url_key, "")
                            if u and u not in images:
                                images.append(u)
                    elif isinstance(item, str) and item.startswith("http"):
                        if item not in images:
                            images.append(item)

            if images:
                variant_images[v_id] = images
                color = v.get("color", v.get("name", "?"))
                print(f"    Variant {v_id} ({color}): {len(images)} images")

    if not variant_images:
        print("  WARNING: No images found in template data.")
        print("  Will try to get images from Printful mockup generator or use template thumbnails.")
        print("  Continuing with Baselinker/Amazon steps using CDN URLs from template...")

        # Try to extract catalog_product_id for mockup generation
        catalog_product_id = None
        if isinstance(template_data, dict):
            t = template_data
            for key_path in [("data",), ("data", "data"), ("result",), ("data", "result")]:
                obj = t
                for k in key_path:
                    if isinstance(obj, dict):
                        obj = obj.get(k, {})
                if isinstance(obj, dict):
                    catalog_product_id = obj.get("catalog_product_id", obj.get("product_id"))
                    if catalog_product_id:
                        break

        if catalog_product_id:
            print(f"  Catalog product ID: {catalog_product_id}")

    print(f"\n  Total variants with images: {len(variant_images)}")
    return variant_images


def step3_baselinker(bl, variant_images, template_data, dry_run=False):
    """Step 3: Add products to Baselinker inventory."""
    print("\n" + "=" * 70)
    print("  STEP 3: Add Products to Baselinker (inventory {})".format(BL_INVENTORY_ID))
    print("=" * 70)

    # Parse variants from template data
    variants = []
    if isinstance(template_data, dict):
        t = template_data
        if "data" in t:
            t = t["data"]
        if "result" in t:
            t = t["result"]
        if "data" in t and isinstance(t["data"], dict):
            t = t["data"]
        variants = t.get("variants", [])

    if not variants:
        # Fallback: use known dad hat variants
        print("  WARNING: No variants in template data, using known Dad Hat variants")
        variants = [{"id": suffix, "color": COLOR_EN.get(suffix, f"Color_{suffix}")}
                    for suffix in COLOR_EN.keys()]

    created = 0
    skipped = 0
    errors = 0
    bl_products = {}  # suffix -> bl_product_id

    # Check which products already exist
    existing = bl.get_inventory_products_list(BL_INVENTORY_ID, f"PFT-{TEMPLATE_ID}")
    existing_skus = set()
    if "products" in existing:
        for pid, pdata in existing["products"].items():
            sku = pdata.get("sku", "")
            if sku.startswith(f"PFT-{TEMPLATE_ID}"):
                existing_skus.add(sku)

    print(f"  Existing products with prefix PFT-{TEMPLATE_ID}: {len(existing_skus)}")

    for v in variants:
        v_id = str(v.get("id", v.get("variant_id", "")))
        color = v.get("color", v.get("name", COLOR_EN.get(v_id, f"Variant_{v_id}")))
        sku = f"PFT-{TEMPLATE_ID}-{v_id}"

        if sku in existing_skus:
            print(f"  SKIP (exists): {sku} ({color})")
            skipped += 1
            continue

        name = f"Make Germany Great Again with Flag - {color}"

        # Get image URL for this variant
        images_dict = {}
        imgs = variant_images.get(v_id, [])
        for idx, img_url in enumerate(imgs[:3]):
            images_dict[str(idx)] = f"url:{img_url}"

        # Baselinker API format:
        # - product_id: "0" to create new
        # - text_fields: {"name": "..."} for product name
        # - prices: {"price_group_id": gross_price}  (31059 is Printful inventory price group)
        # - stock: {"warehouse_id": qty}  (bl_79555 is Printful warehouse)
        params = {
            "sku": sku,
            "text_fields": {"name": name},
            "prices": {"31059": 29.99},
            "stock": {"bl_79555": 999},
        }
        if images_dict:
            params["images"] = images_dict

        if dry_run:
            print(f"  [DRY-RUN] Would add: {sku} ({color})")
            created += 1
            continue

        result = bl.add_inventory_product(BL_INVENTORY_ID, **params)
        if result.get("status") == "SUCCESS":
            pid = result.get("product_id", "?")
            print(f"  CREATED: {sku} ({color}) -> BL product_id={pid}")
            bl_products[v_id] = pid
            created += 1
        else:
            err = result.get("error_message", result.get("error_code", str(result)))
            print(f"  ERROR: {sku} ({color}) -> {err}")
            errors += 1

        time.sleep(0.3)

    print(f"\n  Summary: {created} created, {skipped} skipped, {errors} errors")
    return bl_products


def step4_amazon_listings(amz, variant_images, template_data, dry_run=False):
    """Step 4: Create Amazon listings on all 8 EU marketplaces."""
    print("\n" + "=" * 70)
    print("  STEP 4: Create Amazon Listings (8 EU Marketplaces)")
    print("=" * 70)

    parent_sku = f"PFT-{TEMPLATE_ID}"

    # Parse variants from template data to get suffixes
    variants = []
    if isinstance(template_data, dict):
        t = template_data
        if "data" in t:
            t = t["data"]
        if "result" in t:
            t = t["result"]
        if "data" in t and isinstance(t["data"], dict):
            t = t["data"]
        variants = t.get("variants", [])

    # Get variant suffixes
    if variants:
        variant_suffixes = [str(v.get("id", v.get("variant_id", ""))) for v in variants]
        # Filter to only known dad hat suffixes
        known = set(COLOR_EN.keys())
        variant_suffixes = [s for s in variant_suffixes if s in known]
        if not variant_suffixes:
            variant_suffixes = list(COLOR_EN.keys())
    else:
        variant_suffixes = list(COLOR_EN.keys())

    print(f"  Parent SKU: {parent_sku}")
    print(f"  Variant suffixes: {variant_suffixes}")
    print(f"  Color variants: {len(variant_suffixes)}")

    results = {"created": 0, "skipped": 0, "errors": 0, "details": []}

    for mkt_code, mkt_id in MARKETPLACE_IDS.items():
        print(f"\n  --- {mkt_code} ({mkt_id}) ---")

        # ── Create parent ──
        if dry_run:
            print(f"    [DRY-RUN] Would PUT parent {parent_sku}")
        else:
            parent_attrs = build_parent_attrs(mkt_code)
            # Add images to parent if available (use first variant's images)
            first_suffix = variant_suffixes[0] if variant_suffixes else None
            if first_suffix and first_suffix in variant_images:
                imgs = variant_images[first_suffix]
                if imgs:
                    parent_attrs["main_product_image_locator"] = [{"media_location": imgs[0], "marketplace_id": mkt_id}]
                for i, url in enumerate(imgs[1:7], 1):
                    parent_attrs[f"other_product_image_locator_{i}"] = [{"media_location": url, "marketplace_id": mkt_id}]

            status, resp = amz.put_listing(parent_sku, mkt_id, parent_attrs)
            resp_status = resp.get("status", "?")
            if status == 200 and resp_status in ("ACCEPTED", "VALID"):
                results["created"] += 1
                results["details"].append({"sku": parent_sku, "mkt": mkt_code, "type": "parent", "status": "OK"})
            else:
                results["errors"] += 1
                results["details"].append({"sku": parent_sku, "mkt": mkt_code, "type": "parent", "status": "ERROR", "resp": resp_status})
            time.sleep(1.5)

        # ── Create children ──
        for suffix in variant_suffixes:
            child_sku = f"{parent_sku}-{suffix}"
            color_name = COLOR_EN.get(suffix, suffix)
            imgs = variant_images.get(suffix, [])

            if dry_run:
                print(f"    [DRY-RUN] Would PUT child {child_sku} ({color_name})")
                results["created"] += 1
                continue

            child_attrs = build_child_attrs(mkt_code, parent_sku, suffix, imgs)
            status, resp = amz.put_listing(child_sku, mkt_id, child_attrs)
            resp_status = resp.get("status", "?")

            if status == 200 and resp_status in ("ACCEPTED", "VALID"):
                results["created"] += 1
                results["details"].append({"sku": child_sku, "mkt": mkt_code, "type": "child", "status": "OK"})
            else:
                results["errors"] += 1
                err_issues = [i for i in resp.get("issues", []) if i.get("severity") == "ERROR"]
                err_msg = err_issues[0].get("message", "")[:100] if err_issues else resp_status
                results["details"].append({"sku": child_sku, "mkt": mkt_code, "type": "child", "status": "ERROR", "error": err_msg})

            time.sleep(1.0)

    return results


def step5_upload_images(amz, variant_images, template_data):
    """Step 5: Upload images to Amazon (PATCH if not included in PUT)."""
    print("\n" + "=" * 70)
    print("  STEP 5: Upload Images to Amazon")
    print("=" * 70)

    if not variant_images:
        print("  No images available to upload. Skipping.")
        return {"patched": 0}

    parent_sku = f"PFT-{TEMPLATE_ID}"
    patched = 0
    errors = 0

    # Parse variant suffixes
    variant_suffixes = list(COLOR_EN.keys())

    for suffix in variant_suffixes:
        child_sku = f"{parent_sku}-{suffix}"
        imgs = variant_images.get(suffix, [])
        if not imgs:
            continue

        for mkt_code, mkt_id in MARKETPLACE_IDS.items():
            patches = []

            # Main image
            if imgs:
                patches.append({
                    "op": "replace",
                    "path": "/attributes/main_product_image_locator",
                    "value": [{"media_location": imgs[0], "marketplace_id": mkt_id}]
                })

            # Additional images
            for i, url in enumerate(imgs[1:7], 1):
                patches.append({
                    "op": "replace",
                    "path": f"/attributes/other_product_image_locator_{i}",
                    "value": [{"media_location": url, "marketplace_id": mkt_id}]
                })

            if patches:
                status, resp = amz.patch_listing(child_sku, mkt_id, patches)
                resp_status = resp.get("status", "?")
                if status == 200 and resp_status in ("ACCEPTED", "VALID"):
                    patched += 1
                else:
                    errors += 1
                time.sleep(0.5)

    print(f"\n  Images patched: {patched}, errors: {errors}")
    return {"patched": patched, "errors": errors}


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Create Germany Flag Hat pipeline")
    parser.add_argument("--step", choices=["printful", "baselinker", "amazon", "all"], default="all")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    print("=" * 70)
    print("  PIPELINE: Make Germany Great Again with Flag (Dad Hat)")
    print(f"  Template: {TEMPLATE_ID}")
    print(f"  Step: {args.step}")
    print(f"  Dry run: {args.dry_run}")
    print(f"  Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    pf = PrintfulAPI(PF_TOKEN, PF_STORE_ID)
    bl = BaselinkerAPI(BL_TOKEN)
    amz = AmazonAPI(AMZ_CREDS)

    # ── Step 1: Get Printful template ──
    template_data = None
    if args.step in ("printful", "all"):
        template_data = step1_printful_template(pf)
        if not template_data:
            print("\n  FATAL: Could not get template from Printful. Aborting.")
            sys.exit(1)

    # If not running printful step, try loading cached data
    if template_data is None:
        cache_path = Path(__file__).parent / "germany_flag_template.json"
        if cache_path.exists():
            template_data = json.loads(cache_path.read_text())
            print(f"\n  Loaded cached template data from {cache_path}")
        else:
            template_data = {}
            print("\n  WARNING: No template data available, using defaults")

    # ── Step 2: Collect mockup/image URLs ──
    variant_images = {}
    if args.step in ("printful", "all"):
        variant_images = step2_mockups_and_images(pf, template_data)

    # ── Step 3: Baselinker ──
    if args.step in ("baselinker", "all"):
        step3_baselinker(bl, variant_images, template_data, dry_run=args.dry_run)

    # ── Steps 4-5: Amazon ──
    if args.step in ("amazon", "all"):
        amz_results = step4_amazon_listings(amz, variant_images, template_data, dry_run=args.dry_run)

        if not args.dry_run and variant_images:
            step5_upload_images(amz, variant_images, template_data)

        # ── Report ──
        print("\n" + "=" * 70)
        print("  FINAL REPORT")
        print("=" * 70)

        parent_sku = f"PFT-{TEMPLATE_ID}"
        variant_count = len(COLOR_EN)

        print(f"  Template: {TEMPLATE_ID}")
        print(f"  Parent SKU: {parent_sku}")
        print(f"  Variant count: {variant_count}")
        print(f"  Marketplaces: {len(MARKETPLACE_IDS)}")
        print(f"  Total listings attempted: {amz_results['created'] + amz_results['errors']}")
        print(f"  Successful: {amz_results['created']}")
        print(f"  Errors: {amz_results['errors']}")

        if amz_results["errors"]:
            print(f"\n  Error details:")
            for d in amz_results["details"]:
                if d.get("status") == "ERROR":
                    print(f"    {d['sku']} @ {d['mkt']}: {d.get('error', '?')}")

        print(f"\n  Amazon listing links (DE):")
        print(f"    Parent: https://www.amazon.de/dp/s?k={parent_sku}")
        for suffix in list(COLOR_EN.keys())[:3]:
            print(f"    Child:  https://www.amazon.de/dp/s?k={parent_sku}-{suffix}")

        # Save results
        results_path = Path(__file__).parent / "germany_flag_results.json"
        with open(results_path, "w") as f:
            json.dump(amz_results, f, indent=2, default=str)
        print(f"\n  Results saved to: {results_path}")


if __name__ == "__main__":
    main()
