"""Printful Scale Pipeline — batch design upload, mockup generation, Amazon EU listing.

10 new hat designs → Printful upload → auto-mockups → Amazon 8 EU listings.
Collection: SUM26 (Summer 2026)

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/pipeline_printful_scale.py --dry-run
    python3.11 scripts/pipeline_printful_scale.py --step upload-designs
    python3.11 scripts/pipeline_printful_scale.py --step generate-mockups
    python3.11 scripts/pipeline_printful_scale.py --step create-listings
    python3.11 scripts/pipeline_printful_scale.py --step create-listings --market DE
    python3.11 scripts/pipeline_printful_scale.py --step all
    python3.11 scripts/pipeline_printful_scale.py --step all --product mountain
    python3.11 scripts/pipeline_printful_scale.py --design-dir /path/to/designs

Pipeline Steps:
    1. upload-designs   — Upload design PNG files to Printful file library
    2. generate-mockups — Generate product mockups via Printful Mockup Generator API
    3. create-listings  — Create parent + child listings on Amazon EU (8 markets)
    all                 — Run all steps sequentially

Design files should be placed in:
    ~/nesell-analytics/designs/sum26/<product-key>.png
    e.g., designs/sum26/mountain.png, designs/sum26/coffee.png

Each PNG should be:
    - Min 4000x4000 px for embroidery quality
    - Transparent background
    - Named matching the product key (see PRODUCTS dict below)
"""

import argparse
import json
import os
import sys
import time
import requests
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.amazon_listings import (
    MARKETPLACE_IDS, LANG_TAGS, CURRENCIES, SIZE_SYSTEMS,
    SELLER_ID, put_listing, check_listing_exists,
)

# ── Paths ─────────────────────────────────────────────────────────────────────

SCRIPT_DIR = Path(__file__).parent
PROJECT_DIR = SCRIPT_DIR.parent
DESIGNS_DIR = PROJECT_DIR / "designs" / "sum26"
STATE_FILE = SCRIPT_DIR / "pipeline_sum26_state.json"
RESULTS_FILE = SCRIPT_DIR / "pipeline_sum26_results.json"

# ── Credentials ───────────────────────────────────────────────────────────────

def _load_printful_key() -> tuple[str, str]:
    """Load Printful API token and store ID from ~/.keys/printful.env."""
    env_path = Path.home() / ".keys" / "printful.env"
    token, store_id = "", ""
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("PRINTFUL_API_TOKEN=") and "V2" not in line:
                token = line.split("=", 1)[1].strip().strip('"')
            elif line.startswith("PRINTFUL_STORE_ID="):
                store_id = line.split("=", 1)[1].strip().strip('"')
    return token, store_id


PRINTFUL_TOKEN, PRINTFUL_STORE_ID = _load_printful_key()
PRINTFUL_HEADERS = {
    "Authorization": f"Bearer {PRINTFUL_TOKEN}",
    "Content-Type": "application/json",
}
if PRINTFUL_STORE_ID:
    PRINTFUL_HEADERS["X-PF-Store-Id"] = PRINTFUL_STORE_ID

PRINTFUL_BASE = "https://api.printful.com"


def pf_get(path: str, params: dict | None = None) -> dict:
    """GET request to Printful API with retry."""
    for attempt in range(3):
        resp = requests.get(f"{PRINTFUL_BASE}{path}", headers=PRINTFUL_HEADERS, params=params)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            print(f"  [RATE LIMIT] waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}


def pf_post(path: str, data: dict) -> dict:
    """POST request to Printful API with retry."""
    for attempt in range(3):
        resp = requests.post(f"{PRINTFUL_BASE}{path}", headers=PRINTFUL_HEADERS, json=data)
        if resp.status_code == 429:
            wait = int(resp.headers.get("Retry-After", 60))
            print(f"  [RATE LIMIT] waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    resp.raise_for_status()
    return {}


# ── Product Definitions ──────────────────────────────────────────────────────
# 10 trending niches for Summer 2026 EU market
# SKU format: PFT-SUM26-{KEY}-{COLOR}
# Hat styles: Dad Hat (catalog 145 = Yupoong 6245CM), Trucker (catalog 381)

# Printful catalog product IDs
DAD_HAT_PRODUCT_ID = 145      # Yupoong 6245CM Low Profile Cotton Twill Dad Hat
TRUCKER_PRODUCT_ID = 381      # Yupoong 6606 Retro Trucker Cap
BUCKET_HAT_PRODUCT_ID = 379   # BX003 Big Accessories Bucket Hat

# Variant IDs per color (Dad Hat 145)
DAD_HAT_VARIANTS = {
    "BLACK":     7853,
    "WHITE":     7854,
    "NAVY":      7857,
    "KHAKI":     7855,
    "STONE":     7859,
    "CHARCOAL":  12736,
}

# Variant IDs per color (Trucker 381)
TRUCKER_VARIANTS = {
    "BLACK":         8090,
    "NAVY":          8094,
    "CHARCOAL":      8125,
    "HEATHER-WHITE": 8100,
    "NAVY-WHITE":    8095,
}

# Variant IDs per color (Bucket Hat 379)
BUCKET_VARIANTS = {
    "BLACK": 8058,
    "NAVY":  8060,
    "WHITE": 8062,
}

# ── 10 Design Niches ─────────────────────────────────────────────────────────

PRODUCTS = {
    "mountain": {
        "niche": "Mountain / Outdoor Minimalist",
        "design_concept": "Simple mountain peak silhouette with 'EXPLORE' text",
        "hat_type": "dad_hat",
        "catalog_id": DAD_HAT_PRODUCT_ID,
        "variants": DAD_HAT_VARIANTS,
        "colors": ["BLACK", "KHAKI", "STONE", "NAVY"],
        "price_tier": "standard",
    },
    "coffee": {
        "niche": "Coffee Culture / Barista",
        "design_concept": "Small coffee cup icon with 'But First, Coffee' embroidered text",
        "hat_type": "dad_hat",
        "catalog_id": DAD_HAT_PRODUCT_ID,
        "variants": DAD_HAT_VARIANTS,
        "colors": ["BLACK", "WHITE", "KHAKI", "STONE"],
        "price_tier": "standard",
    },
    "surf": {
        "niche": "Surf / Beach Lifestyle",
        "design_concept": "Wave line drawing with 'SURF' minimal text",
        "hat_type": "trucker",
        "catalog_id": TRUCKER_PRODUCT_ID,
        "variants": TRUCKER_VARIANTS,
        "colors": ["NAVY-WHITE", "BLACK", "HEATHER-WHITE", "CHARCOAL"],
        "price_tier": "premium",
    },
    "cycling": {
        "niche": "Cycling / Bike Commuter",
        "design_concept": "Minimalist bicycle icon with 'RIDE' text below",
        "hat_type": "dad_hat",
        "catalog_id": DAD_HAT_PRODUCT_ID,
        "variants": DAD_HAT_VARIANTS,
        "colors": ["BLACK", "NAVY", "CHARCOAL", "WHITE"],
        "price_tier": "standard",
    },
    "dog-dad": {
        "niche": "Dog Lover / Pet Parent",
        "design_concept": "Paw print icon with 'Dog Dad' or 'Dog Mom' embroidered text",
        "hat_type": "dad_hat",
        "catalog_id": DAD_HAT_PRODUCT_ID,
        "variants": DAD_HAT_VARIANTS,
        "colors": ["BLACK", "STONE", "NAVY", "KHAKI"],
        "price_tier": "standard",
    },
    "vinyl": {
        "niche": "Music / Vinyl Collector",
        "design_concept": "Small vinyl record icon with 'ANALOG' text",
        "hat_type": "trucker",
        "catalog_id": TRUCKER_PRODUCT_ID,
        "variants": TRUCKER_VARIANTS,
        "colors": ["BLACK", "NAVY", "CHARCOAL", "HEATHER-WHITE"],
        "price_tier": "premium",
    },
    "garden": {
        "niche": "Gardening / Plant Lover",
        "design_concept": "Small leaf/sprout icon with 'GROW' text",
        "hat_type": "bucket",
        "catalog_id": BUCKET_HAT_PRODUCT_ID,
        "variants": BUCKET_VARIANTS,
        "colors": ["BLACK", "NAVY", "WHITE"],
        "price_tier": "standard",
    },
    "astro": {
        "niche": "Astronomy / Space Enthusiast",
        "design_concept": "Crescent moon + stars minimal embroidery with 'COSMOS' text",
        "hat_type": "dad_hat",
        "catalog_id": DAD_HAT_PRODUCT_ID,
        "variants": DAD_HAT_VARIANTS,
        "colors": ["BLACK", "NAVY", "CHARCOAL", "WHITE"],
        "price_tier": "standard",
    },
    "camping": {
        "niche": "Camping / Outdoor Adventure",
        "design_concept": "Tent under stars icon with 'WILD' text",
        "hat_type": "trucker",
        "catalog_id": TRUCKER_PRODUCT_ID,
        "variants": TRUCKER_VARIANTS,
        "colors": ["BLACK", "NAVY-WHITE", "CHARCOAL", "HEATHER-WHITE"],
        "price_tier": "premium",
    },
    "yoga": {
        "niche": "Yoga / Mindfulness / Wellness",
        "design_concept": "Lotus flower silhouette with 'BREATHE' text",
        "hat_type": "dad_hat",
        "catalog_id": DAD_HAT_PRODUCT_ID,
        "variants": DAD_HAT_VARIANTS,
        "colors": ["WHITE", "STONE", "BLACK", "KHAKI"],
        "price_tier": "standard",
    },
}

ALL_PRODUCTS = list(PRODUCTS.keys())

# ── Price Tables ──────────────────────────────────────────────────────────────

PRICE_TIERS = {
    "standard": {  # Dad hats, bucket hats
        "DE": 24.99, "FR": 24.99, "IT": 24.99, "ES": 24.99,
        "NL": 24.99, "BE": 24.99, "PL": 109.99, "SE": 279.00,
    },
    "premium": {  # Trucker caps
        "DE": 27.99, "FR": 27.99, "IT": 27.99, "ES": 27.99,
        "NL": 27.99, "BE": 27.99, "PL": 119.99, "SE": 309.00,
    },
}

# ── Browse Nodes (Caps & Hats category across EU) ────────────────────────────

BROWSE_NODES = {
    "DE": "1981316031", "FR": "1981316031", "IT": "1981316031", "ES": "1981316031",
    "NL": "1981316031", "PL": "1981316031", "SE": "1981316031", "BE": "1981316031",
}

# ── Localized Color Names ────────────────────────────────────────────────────

COLOR_TRANSLATIONS = {
    "BLACK": {
        "DE": "Schwarz", "FR": "Noir", "IT": "Nero", "ES": "Negro",
        "NL": "Zwart", "PL": "Czarny", "SE": "Svart", "BE": "Zwart",
    },
    "WHITE": {
        "DE": "Weiß", "FR": "Blanc", "IT": "Bianco", "ES": "Blanco",
        "NL": "Wit", "PL": "Biały", "SE": "Vit", "BE": "Wit",
    },
    "NAVY": {
        "DE": "Marineblau", "FR": "Bleu Marine", "IT": "Blu Marina", "ES": "Azul Marino",
        "NL": "Marineblauw", "PL": "Granatowy", "SE": "Marinblå", "BE": "Marineblauw",
    },
    "KHAKI": {
        "DE": "Khaki", "FR": "Kaki", "IT": "Cachi", "ES": "Caqui",
        "NL": "Khaki", "PL": "Khaki", "SE": "Khaki", "BE": "Khaki",
    },
    "STONE": {
        "DE": "Steingrau", "FR": "Pierre", "IT": "Pietra", "ES": "Piedra",
        "NL": "Steengrijs", "PL": "Kamienny", "SE": "Stengrå", "BE": "Steengrijs",
    },
    "CHARCOAL": {
        "DE": "Anthrazit", "FR": "Anthracite", "IT": "Antracite", "ES": "Antracita",
        "NL": "Antraciet", "PL": "Antracytowy", "SE": "Antracit", "BE": "Antraciet",
    },
    "NAVY-WHITE": {
        "DE": "Marineblau-Weiß", "FR": "Bleu Marine-Blanc", "IT": "Blu Marina-Bianco", "ES": "Azul Marino-Blanco",
        "NL": "Marineblauw-Wit", "PL": "Granatowy-Biały", "SE": "Marinblå-Vit", "BE": "Marineblauw-Wit",
    },
    "HEATHER-WHITE": {
        "DE": "Grau Meliert-Weiß", "FR": "Gris Chiné-Blanc", "IT": "Grigio Mélange-Bianco", "ES": "Gris Jaspeado-Blanco",
        "NL": "Grijs Gemêleerd-Wit", "PL": "Szary Melanż-Biały", "SE": "Gråmelerad-Vit", "BE": "Grijs Gemêleerd-Wit",
    },
}

# ── Localized Content Templates ──────────────────────────────────────────────
# Each niche gets localized titles, bullets, keywords for 8 EU markets

def _hat_type_name(hat_type: str, lang: str) -> dict:
    """Return localized hat type name."""
    names = {
        "dad_hat": {
            "DE": "Baseball Cap", "FR": "Casquette", "IT": "Cappellino Baseball",
            "ES": "Gorra", "NL": "Baseball Pet", "PL": "Czapka z daszkiem",
            "SE": "Keps", "BE": "Baseball Pet",
        },
        "trucker": {
            "DE": "Trucker Cap", "FR": "Casquette Trucker", "IT": "Cappellino Trucker",
            "ES": "Gorra Trucker", "NL": "Trucker Pet", "PL": "Czapka Trucker",
            "SE": "Trucker Keps", "BE": "Trucker Pet",
        },
        "bucket": {
            "DE": "Bucket Hat", "FR": "Bob", "IT": "Cappello Bucket",
            "ES": "Sombrero Bucket", "NL": "Bucket Hat", "PL": "Kapelusz Bucket",
            "SE": "Bucket Hatt", "BE": "Bucket Hat",
        },
    }
    return names[hat_type][lang]


# Niche-specific localized content
NICHE_CONTENT = {
    "mountain": {
        "title_keyword": {
            "DE": "Berg Outdoor Wandern", "FR": "Montagne Outdoor Randonnée", "IT": "Montagna Outdoor Escursionismo",
            "ES": "Montaña Outdoor Senderismo", "NL": "Berg Outdoor Wandelen", "PL": "Góry Outdoor Turystyka",
            "SE": "Berg Outdoor Vandring", "BE": "Berg Outdoor Wandelen",
        },
        "keywords": {
            "DE": "berg cap bestickt outdoor wandern natur bergsteiger alpen hut baseball cap unisex geschenk",
            "FR": "casquette montagne brodée outdoor randonnée nature alpinisme alpes chapeau unisexe cadeau",
            "IT": "cappellino montagna ricamato outdoor escursionismo natura alpinismo alpi berretto unisex regalo",
            "ES": "gorra montaña bordada outdoor senderismo naturaleza alpinismo sombrero unisex regalo",
            "NL": "berg pet geborduurd outdoor wandelen natuur alpinisme hoed unisex cadeau",
            "PL": "czapka góry haftowana outdoor turystyka natura alpinizm czapka z daszkiem unisex prezent",
            "SE": "keps berg broderad outdoor vandring natur alpinism hatt unisex present",
            "BE": "berg pet geborduurd outdoor wandelen natuur alpinisme hoed unisex cadeau",
        },
    },
    "coffee": {
        "title_keyword": {
            "DE": "Kaffee Barista Geschenk", "FR": "Café Barista Cadeau", "IT": "Caffè Barista Regalo",
            "ES": "Café Barista Regalo", "NL": "Koffie Barista Cadeau", "PL": "Kawa Barista Prezent",
            "SE": "Kaffe Barista Present", "BE": "Koffie Barista Cadeau",
        },
        "keywords": {
            "DE": "kaffee cap bestickt barista kaffeeliebhaber coffee lover geschenk baseball cap lustig unisex",
            "FR": "casquette café brodée barista amateur café coffee lover cadeau unisexe drôle",
            "IT": "cappellino caffè ricamato barista amante caffè coffee lover regalo unisex divertente",
            "ES": "gorra café bordada barista amante café coffee lover regalo unisex divertida",
            "NL": "koffie pet geborduurd barista koffieliefhebber coffee lover cadeau unisex grappig",
            "PL": "czapka kawa haftowana barista miłośnik kawy coffee lover prezent unisex śmieszna",
            "SE": "keps kaffe broderad barista kaffeälskare coffee lover present unisex rolig",
            "BE": "koffie pet geborduurd barista koffieliefhebber coffee lover cadeau unisex grappig",
        },
    },
    "surf": {
        "title_keyword": {
            "DE": "Surf Welle Strand Sommer", "FR": "Surf Vague Plage Été", "IT": "Surf Onda Spiaggia Estate",
            "ES": "Surf Ola Playa Verano", "NL": "Surf Golf Strand Zomer", "PL": "Surf Fala Plaża Lato",
            "SE": "Surf Våg Strand Sommar", "BE": "Surf Golf Strand Zomer",
        },
        "keywords": {
            "DE": "surf cap trucker bestickt welle strand sommer surfer ocean meer skateboard baseball unisex",
            "FR": "casquette surf trucker brodée vague plage été surfeur océan mer skateboard unisexe",
            "IT": "cappellino surf trucker ricamato onda spiaggia estate surfista oceano mare skateboard unisex",
            "ES": "gorra surf trucker bordada ola playa verano surfista océano mar skateboard unisex",
            "NL": "surf trucker pet geborduurd golf strand zomer surfer oceaan zee skateboard unisex",
            "PL": "czapka surf trucker haftowana fala plaża lato surfer ocean morze skateboard unisex",
            "SE": "keps surf trucker broderad våg strand sommar surfare ocean hav skateboard unisex",
            "BE": "surf trucker pet geborduurd golf strand zomer surfer oceaan zee skateboard unisex",
        },
    },
    "cycling": {
        "title_keyword": {
            "DE": "Fahrrad Radfahrer Radsport", "FR": "Vélo Cyclisme Cycliste", "IT": "Bicicletta Ciclismo Ciclista",
            "ES": "Bicicleta Ciclismo Ciclista", "NL": "Fiets Wielrennen Fietser", "PL": "Rower Kolarstwo Rowerzysta",
            "SE": "Cykel Cykling Cyklist", "BE": "Fiets Wielrennen Fietser",
        },
        "keywords": {
            "DE": "fahrrad cap bestickt radfahrer radsport rad geschenk baseball cap cycling bike unisex pendler",
            "FR": "casquette vélo brodée cycliste cyclisme cadeau velo bicycle unisexe",
            "IT": "cappellino bicicletta ricamato ciclista ciclismo regalo bici bicycle unisex",
            "ES": "gorra bicicleta bordada ciclista ciclismo regalo bici bicycle unisex",
            "NL": "fiets pet geborduurd wielrenner wielrennen cadeau bicycle cycling unisex",
            "PL": "czapka rower haftowana rowerzysta kolarstwo prezent bicycle cycling unisex",
            "SE": "keps cykel broderad cyklist cykling present bicycle cycling unisex",
            "BE": "fiets pet geborduurd wielrenner wielrennen cadeau bicycle cycling unisex",
        },
    },
    "dog-dad": {
        "title_keyword": {
            "DE": "Hund Papa Hundeliebhaber", "FR": "Chien Papa Amoureux Chiens", "IT": "Cane Papà Amante Cani",
            "ES": "Perro Papá Amante Perros", "NL": "Hond Papa Hondenliefhebber", "PL": "Pies Tata Miłośnik Psów",
            "SE": "Hund Pappa Hundälskare", "BE": "Hond Papa Hondenliefhebber",
        },
        "keywords": {
            "DE": "hund cap bestickt hundeliebhaber dog dad dog mom pfote hundepfote geschenk baseball cap unisex",
            "FR": "casquette chien brodée amoureux chiens dog dad dog mom patte cadeau unisexe",
            "IT": "cappellino cane ricamato amante cani dog dad dog mom zampa regalo unisex",
            "ES": "gorra perro bordada amante perros dog dad dog mom pata regalo unisex",
            "NL": "hond pet geborduurd hondenliefhebber dog dad dog mom poot cadeau unisex",
            "PL": "czapka pies haftowana miłośnik psów dog dad dog mom łapa prezent unisex",
            "SE": "keps hund broderad hundälskare dog dad dog mom tass present unisex",
            "BE": "hond pet geborduurd hondenliefhebber dog dad dog mom poot cadeau unisex",
        },
    },
    "vinyl": {
        "title_keyword": {
            "DE": "Vinyl Musik Schallplatte Retro", "FR": "Vinyle Musique Disque Rétro", "IT": "Vinile Musica Disco Retrò",
            "ES": "Vinilo Música Disco Retro", "NL": "Vinyl Muziek Plaat Retro", "PL": "Winyl Muzyka Płyta Retro",
            "SE": "Vinyl Musik Skiva Retro", "BE": "Vinyl Muziek Plaat Retro",
        },
        "keywords": {
            "DE": "vinyl cap trucker bestickt musik schallplatte retro analog dj plattenspieler geschenk unisex",
            "FR": "casquette vinyle trucker brodée musique disque rétro analog dj platine cadeau unisexe",
            "IT": "cappellino vinile trucker ricamato musica disco retrò analog dj giradischi regalo unisex",
            "ES": "gorra vinilo trucker bordada música disco retro analog dj tocadiscos regalo unisex",
            "NL": "vinyl trucker pet geborduurd muziek plaat retro analog dj draaitafel cadeau unisex",
            "PL": "czapka winyl trucker haftowana muzyka płyta retro analog dj gramofon prezent unisex",
            "SE": "keps vinyl trucker broderad musik skiva retro analog dj skivspelare present unisex",
            "BE": "vinyl trucker pet geborduurd muziek plaat retro analog dj draaitafel cadeau unisex",
        },
    },
    "garden": {
        "title_keyword": {
            "DE": "Garten Gärtner Pflanzen", "FR": "Jardin Jardinier Plantes", "IT": "Giardino Giardiniere Piante",
            "ES": "Jardín Jardinero Plantas", "NL": "Tuin Tuinman Planten", "PL": "Ogród Ogrodnik Rośliny",
            "SE": "Trädgård Trädgårdsmästare Växter", "BE": "Tuin Tuinman Planten",
        },
        "keywords": {
            "DE": "garten bucket hat bestickt gärtner pflanzen blumen natur outdoor sonnenhut geschenk unisex",
            "FR": "bob jardin brodé jardinier plantes fleurs nature outdoor chapeau soleil cadeau unisexe",
            "IT": "cappello bucket giardino ricamato giardiniere piante fiori natura outdoor cappello sole regalo unisex",
            "ES": "sombrero bucket jardín bordado jardinero plantas flores naturaleza outdoor sombrero sol regalo unisex",
            "NL": "tuin bucket hat geborduurd tuinman planten bloemen natuur outdoor zonnehoed cadeau unisex",
            "PL": "kapelusz bucket ogród haftowany ogrodnik rośliny kwiaty natura outdoor kapelusz słoneczny prezent unisex",
            "SE": "bucket hatt trädgård broderad trädgårdsmästare växter blommor natur outdoor solhatt present unisex",
            "BE": "tuin bucket hat geborduurd tuinman planten bloemen natuur outdoor zonnehoed cadeau unisex",
        },
    },
    "astro": {
        "title_keyword": {
            "DE": "Astronomie Sterne Weltraum Mond", "FR": "Astronomie Étoiles Espace Lune", "IT": "Astronomia Stelle Spazio Luna",
            "ES": "Astronomía Estrellas Espacio Luna", "NL": "Astronomie Sterren Ruimte Maan", "PL": "Astronomia Gwiazdy Kosmos Księżyc",
            "SE": "Astronomi Stjärnor Rymden Måne", "BE": "Astronomie Sterren Ruimte Maan",
        },
        "keywords": {
            "DE": "astronomie cap bestickt sterne weltraum mond kosmos space nasa nacht himmel geschenk unisex",
            "FR": "casquette astronomie brodée étoiles espace lune cosmos nasa nuit ciel cadeau unisexe",
            "IT": "cappellino astronomia ricamato stelle spazio luna cosmo nasa notte cielo regalo unisex",
            "ES": "gorra astronomía bordada estrellas espacio luna cosmos nasa noche cielo regalo unisex",
            "NL": "astronomie pet geborduurd sterren ruimte maan kosmos nasa nacht hemel cadeau unisex",
            "PL": "czapka astronomia haftowana gwiazdy kosmos księżyc space nasa noc niebo prezent unisex",
            "SE": "keps astronomi broderad stjärnor rymden måne kosmos nasa natt himmel present unisex",
            "BE": "astronomie pet geborduurd sterren ruimte maan kosmos nasa nacht hemel cadeau unisex",
        },
    },
    "camping": {
        "title_keyword": {
            "DE": "Camping Zelt Abenteuer Outdoor", "FR": "Camping Tente Aventure Outdoor", "IT": "Campeggio Tenda Avventura Outdoor",
            "ES": "Camping Tienda Aventura Outdoor", "NL": "Camping Tent Avontuur Outdoor", "PL": "Camping Namiot Przygoda Outdoor",
            "SE": "Camping Tält Äventyr Outdoor", "BE": "Camping Tent Avontuur Outdoor",
        },
        "keywords": {
            "DE": "camping cap trucker bestickt zelt abenteuer outdoor wandern natur wald lagerfeuer geschenk unisex",
            "FR": "casquette camping trucker brodée tente aventure outdoor randonnée nature forêt feu de camp cadeau unisexe",
            "IT": "cappellino campeggio trucker ricamato tenda avventura outdoor escursionismo natura foresta falò regalo unisex",
            "ES": "gorra camping trucker bordada tienda aventura outdoor senderismo naturaleza bosque fogata regalo unisex",
            "NL": "camping trucker pet geborduurd tent avontuur outdoor wandelen natuur bos kampvuur cadeau unisex",
            "PL": "czapka camping trucker haftowana namiot przygoda outdoor turystyka natura las ognisko prezent unisex",
            "SE": "keps camping trucker broderad tält äventyr outdoor vandring natur skog lägereld present unisex",
            "BE": "camping trucker pet geborduurd tent avontuur outdoor wandelen natuur bos kampvuur cadeau unisex",
        },
    },
    "yoga": {
        "title_keyword": {
            "DE": "Yoga Meditation Wellness Lotus", "FR": "Yoga Méditation Bien-être Lotus", "IT": "Yoga Meditazione Benessere Loto",
            "ES": "Yoga Meditación Bienestar Loto", "NL": "Yoga Meditatie Wellness Lotus", "PL": "Yoga Medytacja Wellness Lotos",
            "SE": "Yoga Meditation Wellness Lotus", "BE": "Yoga Meditatie Wellness Lotus",
        },
        "keywords": {
            "DE": "yoga cap bestickt meditation wellness lotus achtsamkeit mindfulness geschenk baseball cap unisex sport",
            "FR": "casquette yoga brodée méditation bien-être lotus pleine conscience mindfulness cadeau unisexe sport",
            "IT": "cappellino yoga ricamato meditazione benessere loto consapevolezza mindfulness regalo unisex sport",
            "ES": "gorra yoga bordada meditación bienestar loto conciencia plena mindfulness regalo unisex deporte",
            "NL": "yoga pet geborduurd meditatie wellness lotus mindfulness cadeau unisex sport",
            "PL": "czapka yoga haftowana medytacja wellness lotos uważność mindfulness prezent unisex sport",
            "SE": "keps yoga broderad meditation wellness lotus mindfulness present unisex sport",
            "BE": "yoga pet geborduurd meditatie wellness lotus mindfulness cadeau unisex sport",
        },
    },
}


# ── Localized Bullet Templates ───────────────────────────────────────────────

BULLET_TEMPLATES = {
    "dad_hat": {
        "DE": [
            "PREMIUM MASCHINENSTICKEREI — Hochwertiges Embroidery das nicht verblasst, nicht abblättert und nicht reißt wie Druckverfahren",
            "100% WEICHE BAUMWOLLE — Chino Twill, niedriges Profil, vorgebogener Schirm. Angenehm leicht und atmungsaktiv",
            "VERSTELLBARER METALLVERSCHLUSS — Passt jedem Kopfumfang. Einheitsgröße für Damen und Herren unisex",
            "PERFEKTES GESCHENK — Ideal für Geburtstage, Weihnachten, Vatertag oder als Überraschung für Liebhaber",
            "VIELSEITIG EINSETZBAR — Für Freizeit, Reisen, Festivals, Outdoor und den täglichen Gebrauch",
        ],
        "FR": [
            "BRODERIE MACHINE PREMIUM — Broderie de qualité qui ne décolore pas, ne s'écaille pas et ne se déchire pas",
            "100% COTON DOUX — Twill chino, faible profil, visière pré-courbée. Léger et respirant",
            "FERMETURE RÉGLABLE EN MÉTAL — S'adapte à toutes les tailles de tête. Taille unique unisexe",
            "CADEAU PARFAIT — Idéal pour anniversaires, Noël, fête des pères ou comme surprise",
            "POLYVALENTE — Pour les loisirs, voyages, festivals, outdoor et le quotidien",
        ],
        "IT": [
            "RICAMO A MACCHINA PREMIUM — Ricamo di qualità che non sbiadisce, non si stacca e non si strappa",
            "100% COTONE MORBIDO — Twill chino, profilo basso, visiera pre-curvata. Leggero e traspirante",
            "CHIUSURA REGOLABILE IN METALLO — Si adatta a tutte le misure di testa. Taglia unica unisex",
            "REGALO PERFETTO — Ideale per compleanni, Natale, festa del papà o come sorpresa",
            "VERSATILE — Per tempo libero, viaggi, festival, outdoor e uso quotidiano",
        ],
        "ES": [
            "BORDADO A MÁQUINA PREMIUM — Bordado de calidad que no destiñe, no se descascarilla y no se rompe",
            "100% ALGODÓN SUAVE — Twill chino, perfil bajo, visera pre-curvada. Ligera y transpirable",
            "CIERRE AJUSTABLE EN METAL — Se adapta a todos los tamaños de cabeza. Talla única unisex",
            "REGALO PERFECTO — Ideal para cumpleaños, Navidad, día del padre o como sorpresa",
            "VERSÁTIL — Para ocio, viajes, festivales, outdoor y uso diario",
        ],
        "NL": [
            "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert en niet scheurt",
            "100% ZACHTE KATOEN — Chino twill, laag profiel, voorgebogen klep. Licht en ademend",
            "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size unisex",
            "PERFECT CADEAU — Ideaal voor verjaardagen, Kerstmis, Vaderdag of als verrassing",
            "VEELZIJDIG — Voor vrije tijd, reizen, festivals, outdoor en dagelijks gebruik",
        ],
        "PL": [
            "PREMIUM HAFT MASZYNOWY — Wysokiej jakości haft, który nie blaknie, nie odpryskuje i nie rwie się",
            "100% MIĘKKA BAWEŁNA — Twill chino, niski profil, zakrzywiony daszek. Lekka i oddychająca",
            "REGULOWANE ZAPIĘCIE METALOWE — Dopasowuje się do każdego obwodu głowy. Jeden rozmiar unisex",
            "IDEALNY PREZENT — Na urodziny, święta, Dzień Ojca lub jako niespodzianka",
            "WSZECHSTRONNA — Na co dzień, podróże, festiwale, outdoor i każdą okazję",
        ],
        "SE": [
            "PREMIUM MASKINBRODERI — Kvalitetsbroderi som inte bleknar, flagnar eller rivs som tryck",
            "100% MJUK BOMULL — Chino twill, låg profil, förböjd skärm. Lätt och andningsbar",
            "JUSTERBAR METALLSPÄNNE — Passar alla huvudstorlekar. One size unisex",
            "PERFEKT PRESENT — Idealisk för födelsedagar, jul, fars dag eller som överraskning",
            "MÅNGSIDIG — För fritid, resor, festivaler, outdoor och dagligt bruk",
        ],
        "BE": [
            "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert en niet scheurt",
            "100% ZACHTE KATOEN — Chino twill, laag profiel, voorgebogen klep. Licht en ademend",
            "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size unisex",
            "PERFECT CADEAU — Ideaal voor verjaardagen, Kerstmis, Vaderdag of als verrassing",
            "VEELZIJDIG — Voor vrije tijd, reizen, festivals, outdoor en dagelijks gebruik",
        ],
    },
    "trucker": {
        "DE": [
            "PREMIUM MASCHINENSTICKEREI — Hochwertiges Embroidery das nicht verblasst, nicht abblättert und nicht reißt",
            "KLASSISCHER TRUCKER-STIL — Strukturierte Front, Mesh-Rücken für optimale Belüftung an warmen Tagen",
            "VERSTELLBARER SNAPBACK — Snap-Verschluss passt jeden Kopfumfang. Einheitsgröße unisex",
            "PERFEKTES GESCHENK — Ideal für Geburtstage, Weihnachten, Vatertag oder als Überraschung",
            "VIELSEITIG EINSETZBAR — Für Freizeit, Sport, Strand, Festivals und den täglichen Gebrauch",
        ],
        "FR": [
            "BRODERIE MACHINE PREMIUM — Broderie de qualité qui ne décolore pas, ne s'écaille pas",
            "STYLE TRUCKER CLASSIQUE — Devant structuré, dos en mesh pour ventilation optimale",
            "SNAPBACK RÉGLABLE — Fermeture snap s'adapte à toutes les tailles. Taille unique unisexe",
            "CADEAU PARFAIT — Idéal pour anniversaires, Noël, fête des pères ou comme surprise",
            "POLYVALENTE — Pour les loisirs, sport, plage, festivals et le quotidien",
        ],
        "IT": [
            "RICAMO A MACCHINA PREMIUM — Ricamo di qualità che non sbiadisce, non si stacca",
            "STILE TRUCKER CLASSICO — Parte anteriore strutturata, retro in mesh per ventilazione ottimale",
            "SNAPBACK REGOLABILE — Chiusura snap si adatta a tutte le misure. Taglia unica unisex",
            "REGALO PERFETTO — Ideale per compleanni, Natale, festa del papà o come sorpresa",
            "VERSATILE — Per tempo libero, sport, spiaggia, festival e uso quotidiano",
        ],
        "ES": [
            "BORDADO A MÁQUINA PREMIUM — Bordado de calidad que no destiñe, no se descascarilla",
            "ESTILO TRUCKER CLÁSICO — Parte delantera estructurada, malla trasera para ventilación óptima",
            "SNAPBACK AJUSTABLE — Cierre snap se adapta a todos los tamaños. Talla única unisex",
            "REGALO PERFECTO — Ideal para cumpleaños, Navidad, día del padre o como sorpresa",
            "VERSÁTIL — Para ocio, deporte, playa, festivales y uso diario",
        ],
        "NL": [
            "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert",
            "KLASSIEKE TRUCKER-STIJL — Gestructureerde voorkant, mesh-achterkant voor optimale ventilatie",
            "VERSTELBARE SNAPBACK — Snap-sluiting past elke hoofdomtrek. One size unisex",
            "PERFECT CADEAU — Ideaal voor verjaardagen, Kerstmis, Vaderdag of als verrassing",
            "VEELZIJDIG — Voor vrije tijd, sport, strand, festivals en dagelijks gebruik",
        ],
        "PL": [
            "PREMIUM HAFT MASZYNOWY — Wysokiej jakości haft, który nie blaknie, nie odpryskuje",
            "KLASYCZNY STYL TRUCKER — Strukturowany przód, mesh z tyłu dla optymalnej wentylacji",
            "REGULOWANY SNAPBACK — Zapięcie snap dopasowuje się do każdego rozmiaru. Jeden rozmiar unisex",
            "IDEALNY PREZENT — Na urodziny, święta, Dzień Ojca lub jako niespodzianka",
            "WSZECHSTRONNA — Na co dzień, sport, plażę, festiwale i każdą okazję",
        ],
        "SE": [
            "PREMIUM MASKINBRODERI — Kvalitetsbroderi som inte bleknar, flagnar eller rivs",
            "KLASSISK TRUCKER-STIL — Strukturerad framsida, mesh-baksida för optimal ventilation",
            "JUSTERBAR SNAPBACK — Snap-spänne passar alla huvudstorlekar. One size unisex",
            "PERFEKT PRESENT — Idealisk för födelsedagar, jul, fars dag eller som överraskning",
            "MÅNGSIDIG — För fritid, sport, strand, festivaler och dagligt bruk",
        ],
        "BE": [
            "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert",
            "KLASSIEKE TRUCKER-STIJL — Gestructureerde voorkant, mesh-achterkant voor optimale ventilatie",
            "VERSTELBARE SNAPBACK — Snap-sluiting past elke hoofdomtrek. One size unisex",
            "PERFECT CADEAU — Ideaal voor verjaardagen, Kerstmis, Vaderdag of als verrassing",
            "VEELZIJDIG — Voor vrije tijd, sport, strand, festivals en dagelijks gebruik",
        ],
    },
    "bucket": {
        "DE": [
            "PREMIUM MASCHINENSTICKEREI — Hochwertiges Embroidery das nicht verblasst und nicht reißt",
            "BREITE KREMPE — Klassischer Bucket-Hat-Stil mit allseitigem UV-Sonnenschutz",
            "100% BAUMWOLLE — Leicht und atmungsaktiv, ideal für Sommer und Outdoor",
            "PERFEKTES GESCHENK — Ideal für Geburtstage, Strand-Urlaub oder als Überraschung",
            "VIELSEITIG EINSETZBAR — Für Strand, Garten, Festivals, Reisen und Freizeit",
        ],
        "FR": [
            "BRODERIE MACHINE PREMIUM — Broderie de qualité qui ne décolore pas et ne se déchire pas",
            "BORD LARGE — Style bob classique avec protection UV tout autour",
            "100% COTON — Léger et respirant, idéal pour l'été et l'outdoor",
            "CADEAU PARFAIT — Idéal pour anniversaires, vacances plage ou comme surprise",
            "POLYVALENTE — Pour plage, jardin, festivals, voyages et loisirs",
        ],
        "IT": [
            "RICAMO A MACCHINA PREMIUM — Ricamo di qualità che non sbiadisce e non si strappa",
            "TESA LARGA — Stile bucket classico con protezione UV completa",
            "100% COTONE — Leggero e traspirante, ideale per estate e outdoor",
            "REGALO PERFETTO — Ideale per compleanni, vacanze al mare o come sorpresa",
            "VERSATILE — Per spiaggia, giardino, festival, viaggi e tempo libero",
        ],
        "ES": [
            "BORDADO A MÁQUINA PREMIUM — Bordado de calidad que no destiñe y no se rompe",
            "ALA ANCHA — Estilo bucket clásico con protección UV completa",
            "100% ALGODÓN — Ligero y transpirable, ideal para verano y outdoor",
            "REGALO PERFECTO — Ideal para cumpleaños, vacaciones playa o como sorpresa",
            "VERSÁTIL — Para playa, jardín, festivales, viajes y ocio",
        ],
        "NL": [
            "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt en niet scheurt",
            "BREDE RAND — Klassieke bucket-hat-stijl met rondom UV-bescherming",
            "100% KATOEN — Licht en ademend, ideaal voor zomer en outdoor",
            "PERFECT CADEAU — Ideaal voor verjaardagen, strandvakantie of als verrassing",
            "VEELZIJDIG — Voor strand, tuin, festivals, reizen en vrije tijd",
        ],
        "PL": [
            "PREMIUM HAFT MASZYNOWY — Wysokiej jakości haft, który nie blaknie i nie rwie się",
            "SZEROKIE RONDO — Klasyczny styl bucket hat z pełną ochroną UV",
            "100% BAWEŁNA — Lekka i oddychająca, idealna na lato i outdoor",
            "IDEALNY PREZENT — Na urodziny, wakacje nad morzem lub jako niespodzianka",
            "WSZECHSTRONNY — Na plażę, do ogrodu, na festiwale, podróże i wolny czas",
        ],
        "SE": [
            "PREMIUM MASKINBRODERI — Kvalitetsbroderi som inte bleknar och inte rivs",
            "BRED BRÄTTE — Klassisk bucket-hatt-stil med komplett UV-skydd",
            "100% BOMULL — Lätt och andningsbar, perfekt för sommar och outdoor",
            "PERFEKT PRESENT — Idealisk för födelsedagar, strandsemester eller som överraskning",
            "MÅNGSIDIG — För strand, trädgård, festivaler, resor och fritid",
        ],
        "BE": [
            "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt en niet scheurt",
            "BREDE RAND — Klassieke bucket-hat-stijl met rondom UV-bescherming",
            "100% KATOEN — Licht en ademend, ideaal voor zomer en outdoor",
            "PERFECT CADEAU — Ideaal voor verjaardagen, strandvakantie of als verrassing",
            "VEELZIJDIG — Voor strand, tuin, festivals, reizen en vrije tijd",
        ],
    },
}

# Localized meta fields
META_FIELDS = {
    "pattern": {
        "DE": "Einfarbig", "FR": "Uni", "IT": "Tinta Unita", "ES": "Liso",
        "NL": "Effen", "PL": "Jednobarwny", "SE": "Enfärgad", "BE": "Effen",
    },
    "age": {
        "DE": "Erwachsener", "FR": "Adulte", "IT": "Adulto", "ES": "Adulto",
        "NL": "Volwassene", "PL": "Dorosły", "SE": "Vuxen", "BE": "Volwassene",
    },
    "care": {
        "DE": "Handwäsche", "FR": "Lavage à la main", "IT": "Lavaggio a mano", "ES": "Lavado a mano",
        "NL": "Handwas", "PL": "Pranie ręczne", "SE": "Handtvätt", "BE": "Handwas",
    },
    "fabric": {
        "DE": "100% Baumwolle", "FR": "100% Coton", "IT": "100% Cotone", "ES": "100% Algodón",
        "NL": "100% Katoen", "PL": "100% Bawełna", "SE": "100% Bomull", "BE": "100% Katoen",
    },
    "material": {
        "DE": "Baumwolle", "FR": "Coton", "IT": "Cotone", "ES": "Algodón",
        "NL": "Katoen", "PL": "Bawełna", "SE": "Bomull", "BE": "Katoen",
    },
    "style": {
        "dad_hat": {
            "DE": "Casual", "FR": "Décontracté", "IT": "Casual", "ES": "Casual",
            "NL": "Casual", "PL": "Casual", "SE": "Casual", "BE": "Casual",
        },
        "trucker": {
            "DE": "Trucker", "FR": "Trucker", "IT": "Trucker", "ES": "Trucker",
            "NL": "Trucker", "PL": "Trucker", "SE": "Trucker", "BE": "Trucker",
        },
        "bucket": {
            "DE": "Fischerhut", "FR": "Bob", "IT": "Bucket", "ES": "Bucket",
            "NL": "Bucket", "PL": "Bucket", "SE": "Bucket", "BE": "Bucket",
        },
    },
    "unit_type": {
        "DE": "Stück", "FR": "pièce", "IT": "pezzo", "ES": "pieza",
        "NL": "stuk", "PL": "sztuka", "SE": "styck", "BE": "stuk",
    },
}

HAT_FORM_TYPES = {
    "dad_hat": "baseball_cap",
    "trucker": "baseball_cap",
    "bucket": "bucket_hat",
}

ALL_MARKETS = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]


# ══════════════════════════════════════════════════════════════════════════════
# STEP 1: Upload Designs to Printful
# ══════════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    """Load pipeline state from JSON file."""
    if STATE_FILE.exists():
        with open(STATE_FILE) as f:
            return json.load(f)
    return {"designs": {}, "mockups": {}, "listings": {}}


def save_state(state: dict) -> None:
    """Save pipeline state to JSON file."""
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
        f.write("\n")


def upload_designs(products: list[str], design_dir: Path, dry_run: bool = False) -> dict:
    """Upload design PNG files to Printful file library.

    Each design file should be at: <design_dir>/<product_key>.png
    Returns dict of product_key -> {file_id, url, status}
    """
    state = load_state()
    results = {"uploaded": 0, "skipped": 0, "errors": 0, "missing": 0}

    print(f"\n{'=' * 70}")
    print(f"STEP 1: Upload Designs to Printful")
    print(f"Design directory: {design_dir}")
    print(f"Products: {products}")
    print(f"{'=' * 70}")

    for product_key in products:
        design_file = design_dir / f"{product_key}.png"

        # Check if already uploaded
        if product_key in state.get("designs", {}):
            existing = state["designs"][product_key]
            print(f"  [SKIP] {product_key} — already uploaded (file_id={existing['file_id']})")
            results["skipped"] += 1
            continue

        if not design_file.exists():
            print(f"  [MISSING] {product_key} — design file not found at {design_file}")
            print(f"           Create your design and save it as: {design_file}")
            results["missing"] += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Would upload {design_file}")
            results["uploaded"] += 1
            continue

        # Upload to Printful file library
        try:
            # For local files, we need to use multipart upload
            print(f"  Uploading {product_key}.png to Printful...")
            upload_headers = {
                "Authorization": f"Bearer {PRINTFUL_TOKEN}",
            }
            if PRINTFUL_STORE_ID:
                upload_headers["X-PF-Store-Id"] = PRINTFUL_STORE_ID

            with open(design_file, "rb") as f:
                resp = requests.post(
                    f"{PRINTFUL_BASE}/files",
                    headers=upload_headers,
                    files={"file": (f"{product_key}.png", f, "image/png")},
                )
            resp.raise_for_status()
            data = resp.json()
            file_info = data.get("result", {})

            state.setdefault("designs", {})[product_key] = {
                "file_id": file_info["id"],
                "url": file_info.get("preview_url") or file_info.get("url", ""),
                "thumbnail_url": file_info.get("thumbnail_url", ""),
                "status": file_info.get("status", "waiting"),
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
            }
            save_state(state)

            print(f"  [OK] {product_key} — file_id={file_info['id']}, status={file_info.get('status')}")
            results["uploaded"] += 1
            time.sleep(1)  # Rate limit

        except Exception as e:
            print(f"  [ERROR] {product_key} — {e}")
            results["errors"] += 1

    return results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 2: Generate Mockups
# ══════════════════════════════════════════════════════════════════════════════

def generate_mockups(products: list[str], dry_run: bool = False) -> dict:
    """Generate product mockups via Printful Mockup Generator API.

    Uses the design files uploaded in Step 1.
    Polls for completion (mockup generation is async).
    """
    state = load_state()
    results = {"generated": 0, "skipped": 0, "errors": 0}

    print(f"\n{'=' * 70}")
    print(f"STEP 2: Generate Mockups via Printful API")
    print(f"Products: {products}")
    print(f"{'=' * 70}")

    for product_key in products:
        product = PRODUCTS[product_key]
        catalog_id = product["catalog_id"]

        # Check if mockups already exist
        if product_key in state.get("mockups", {}) and state["mockups"][product_key].get("urls"):
            print(f"  [SKIP] {product_key} — mockups already generated ({len(state['mockups'][product_key]['urls'])} images)")
            results["skipped"] += 1
            continue

        # Check if design was uploaded
        design_info = state.get("designs", {}).get(product_key)
        if not design_info:
            print(f"  [ERROR] {product_key} — no design uploaded yet. Run --step upload-designs first.")
            results["errors"] += 1
            continue

        design_url = design_info["url"]
        if not design_url:
            print(f"  [ERROR] {product_key} — design URL not available yet (status={design_info.get('status')})")
            results["errors"] += 1
            continue

        if dry_run:
            print(f"  [DRY-RUN] Would generate mockups for {product_key} (catalog={catalog_id})")
            results["generated"] += 1
            continue

        try:
            # Get first variant ID for mockup generation
            first_color = product["colors"][0]
            variant_id = product["variants"][first_color]

            # Get available mockup styles
            print(f"  Fetching mockup styles for {product_key} (catalog={catalog_id})...")
            styles_resp = pf_get(f"/mockup-generator/printfiles/{catalog_id}")
            available_styles = styles_resp.get("result", {}).get("available_placements", {})
            front_styles = available_styles.get("front", {}).get("mockup_styles", [])

            # Pick first 3 styles (front, angled, detail)
            style_ids = [s["id"] for s in front_styles[:3]] if front_styles else []
            if not style_ids:
                print(f"  [WARN] No mockup styles found for catalog {catalog_id}, using default")
                style_ids = [1]  # Default front mockup

            # Create mockup task
            print(f"  Creating mockup task for {product_key} with {len(style_ids)} styles...")
            task_body = {
                "variant_ids": [variant_id],
                "format": "jpg",
                "files": [{
                    "placement": "front",
                    "image_url": design_url,
                    "position": {
                        "area_width": 1800,
                        "area_height": 2400,
                        "width": 1800,
                        "height": 1800,
                        "top": 300,
                        "left": 0,
                    },
                }],
            }
            if style_ids:
                task_body["option_groups"] = style_ids[:3]

            task_resp = pf_post(f"/mockup-generator/create-task/{catalog_id}", task_body)
            task_key = task_resp.get("result", {}).get("task_key")

            if not task_key:
                print(f"  [ERROR] {product_key} — no task_key returned: {task_resp}")
                results["errors"] += 1
                continue

            # Poll for completion
            print(f"  Waiting for mockups (task_key={task_key})...")
            mockup_urls = []
            for attempt in range(40):
                time.sleep(10)
                status_resp = pf_get(f"/mockup-generator/task", params={"task_key": task_key})
                status = status_resp.get("result", {}).get("status")

                if status == "completed":
                    mockups = status_resp.get("result", {}).get("mockups", [])
                    mockup_urls = [m["mockup_url"] for m in mockups if "mockup_url" in m]
                    extra = status_resp.get("result", {}).get("extra_mockups", [])
                    mockup_urls += [m["url"] for m in extra if "url" in m]
                    print(f"  [OK] {product_key} — {len(mockup_urls)} mockup(s) generated")
                    break
                elif status == "failed":
                    error = status_resp.get("result", {}).get("error", "unknown")
                    print(f"  [ERROR] {product_key} — mockup generation failed: {error}")
                    break
                else:
                    if attempt % 3 == 0:
                        print(f"    ... still generating (attempt {attempt + 1}/40)")

            if mockup_urls:
                state.setdefault("mockups", {})[product_key] = {
                    "urls": mockup_urls,
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "catalog_id": catalog_id,
                    "variant_id": variant_id,
                }
                save_state(state)
                results["generated"] += 1
            else:
                results["errors"] += 1

            time.sleep(2)  # Rate limit between products

        except Exception as e:
            print(f"  [ERROR] {product_key} — {e}")
            results["errors"] += 1

    return results


# ══════════════════════════════════════════════════════════════════════════════
# STEP 3: Create Amazon EU Listings
# ══════════════════════════════════════════════════════════════════════════════

def build_listing_attrs(product_key: str, mkt_code: str, color_key: str | None,
                        parent_sku: str, is_parent: bool) -> dict:
    """Build Amazon listing attributes for a SUM26 hat product."""
    product = PRODUCTS[product_key]
    hat_type = product["hat_type"]
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    currency = CURRENCIES[mkt_code]
    price = PRICE_TIERS[product["price_tier"]][mkt_code]

    niche = NICHE_CONTENT[product_key]
    title_kw = niche["title_keyword"][mkt_code]
    keywords = niche["keywords"][mkt_code]
    bullets = BULLET_TEMPLATES[hat_type][mkt_code]
    hat_name = _hat_type_name(hat_type, mkt_code)

    # Build title: "{Niche Keywords} {Hat Type} Bestickt | Unisex Sommer 2026"
    season_word = {
        "DE": "Sommer", "FR": "Été", "IT": "Estate", "ES": "Verano",
        "NL": "Zomer", "PL": "Lato", "SE": "Sommar", "BE": "Zomer",
    }[mkt_code]
    embroidered_word = {
        "DE": "Bestickt", "FR": "Brodé", "IT": "Ricamato", "ES": "Bordado",
        "NL": "Geborduurd", "PL": "Haftowany", "SE": "Broderad", "BE": "Geborduurd",
    }[mkt_code]

    base_title = f"{title_kw} {hat_name} {embroidered_word} | Unisex {season_word} 2026"

    if is_parent:
        color_value = {
            "DE": "Mehrfarbig", "FR": "Multicolore", "IT": "Multicolore",
            "ES": "Multicolor", "NL": "Meerkleurig", "PL": "Wielokolorowy",
            "SE": "Flerfärgad", "BE": "Meerkleurig",
        }[mkt_code]
        item_name = base_title
    else:
        color_value = COLOR_TRANSLATIONS[color_key][mkt_code]
        child_suffix = f" | {color_value}"
        if len(base_title) + len(child_suffix) <= 200:
            item_name = base_title + child_suffix
        else:
            item_name = base_title[:197] + "..."

    # Ensure title under 200 chars
    if len(item_name) > 200:
        item_name = item_name[:197] + "..."

    # Build localized description
    desc_template = {
        "DE": f"{hat_name} mit hochwertiger Maschinenstickerei. {title_kw} Design. Perfektes Geschenk für Liebhaber. Verstellbar, Einheitsgröße unisex.",
        "FR": f"{hat_name} avec broderie machine premium. Design {title_kw}. Cadeau parfait. Réglable, taille unique unisexe.",
        "IT": f"{hat_name} con ricamo a macchina premium. Design {title_kw}. Regalo perfetto. Regolabile, taglia unica unisex.",
        "ES": f"{hat_name} con bordado a máquina premium. Diseño {title_kw}. Regalo perfecto. Ajustable, talla única unisex.",
        "NL": f"{hat_name} met premium machineborduurwerk. {title_kw} ontwerp. Perfect cadeau. Verstelbaar, one size unisex.",
        "PL": f"{hat_name} z premium haftem maszynowym. Design {title_kw}. Idealny prezent. Regulowany, jeden rozmiar unisex.",
        "SE": f"{hat_name} med premium maskinbroderi. {title_kw} design. Perfekt present. Justerbar, one size unisex.",
        "BE": f"{hat_name} met premium machineborduurwerk. {title_kw} ontwerp. Perfect cadeau. Verstelbaar, one size unisex.",
    }

    style_value = META_FIELDS["style"][hat_type][mkt_code]
    hat_form = HAT_FORM_TYPES[hat_type]

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": color_value, "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": META_FIELDS["pattern"][mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "Printful Latvia AS", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": f"Summer 2026 {product_key.upper()} Hat", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": META_FIELDS["age"][mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": BROWSE_NODES[mkt_code], "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in bullets],
        "product_description": [{"value": desc_template[mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "Printful", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": keywords, "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS.get(mkt_code, "as3"), "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": price, "currency": currency, "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "fabric_type": [{"value": META_FIELDS["fabric"][mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "material": [{"value": META_FIELDS["material"][mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "style": [{"value": style_value, "language_tag": lang, "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": hat_form, "marketplace_id": mkt_id}],
        "care_instructions": [{"value": META_FIELDS["care"][mkt_code], "language_tag": lang, "marketplace_id": mkt_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mkt_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt_id}],
    }

    if is_parent:
        attrs["parentage_level"] = [{"marketplace_id": mkt_id, "value": "parent"}]
        attrs["child_parent_sku_relationship"] = [{"marketplace_id": mkt_id, "child_relationship_type": "variation"}]
    else:
        sale_price = round(price * 0.85, 2)
        attrs["fulfillment_availability"] = [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}]
        attrs["purchasable_offer"] = [{
            "currency": currency,
            "audience": "ALL",
            "our_price": [{"schedule": [{"value_with_tax": sale_price}]}],
            "marketplace_id": mkt_id,
        }]
        attrs["parentage_level"] = [{"marketplace_id": mkt_id, "value": "child"}]
        attrs["child_parent_sku_relationship"] = [{
            "marketplace_id": mkt_id,
            "child_relationship_type": "variation",
            "parent_sku": parent_sku,
        }]

    return attrs


def create_listings(products: list[str], markets: list[str], dry_run: bool = False) -> dict:
    """Create parent + child Amazon listings for each product across markets."""
    results = {"created": 0, "skipped": 0, "errors": 0}

    print(f"\n{'=' * 70}")
    print(f"STEP 3: Create Amazon EU Listings")
    print(f"Products: {products}")
    print(f"Markets:  {markets}")
    print(f"{'=' * 70}")

    for product_key in products:
        product = PRODUCTS[product_key]
        parent_sku = f"PFT-SUM26-{product_key.upper()}"
        colors = product["colors"]

        print(f"\n  ── Product: {product_key.upper()} | SKU: {parent_sku} ──")
        print(f"     Colors: {colors} | Hat: {product['hat_type']}")

        for mkt_code in markets:
            mkt_id = MARKETPLACE_IDS[mkt_code]
            print(f"\n    Market: {mkt_code}")

            # ── Parent ──
            if not dry_run and check_listing_exists(parent_sku, mkt_id):
                print(f"      [SKIP] Parent {parent_sku} already exists on {mkt_code}")
                results["skipped"] += 1
            else:
                attrs = build_listing_attrs(product_key, mkt_code, None, parent_sku, is_parent=True)
                title = attrs["item_name"][0]["value"]
                print(f"      [PARENT] {parent_sku} — \"{title[:80]}\"")
                assert len(title) <= 200, f"Title too long ({len(title)} chars): {title}"
                status, resp = put_listing(parent_sku, mkt_id, attrs, dry_run=dry_run)
                if status in (200, 202) or dry_run:
                    results["created"] += 1
                else:
                    results["errors"] += 1
                    print(f"      ERROR: {resp}")
                time.sleep(1.5)

            # ── Children ──
            for color_key in colors:
                child_sku = f"{parent_sku}-{color_key}"
                if not dry_run and check_listing_exists(child_sku, mkt_id):
                    print(f"      [SKIP] Child {child_sku} already exists on {mkt_code}")
                    results["skipped"] += 1
                    continue

                attrs = build_listing_attrs(product_key, mkt_code, color_key, parent_sku, is_parent=False)
                title = attrs["item_name"][0]["value"]
                color_val = attrs["color"][0]["value"]
                print(f"      [CHILD] {child_sku} [{color_val}]")
                assert len(title) <= 200, f"Title too long ({len(title)} chars): {title}"
                status, resp = put_listing(child_sku, mkt_id, attrs, dry_run=dry_run)
                if status in (200, 202) or dry_run:
                    results["created"] += 1
                else:
                    results["errors"] += 1
                    print(f"      ERROR: {resp}")
                time.sleep(1)

    return results


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="Printful Scale Pipeline — batch design upload, mockup generation, Amazon EU listing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Dry run (no API calls):
  python3.11 scripts/pipeline_printful_scale.py --dry-run --step all

  # Upload designs only:
  python3.11 scripts/pipeline_printful_scale.py --step upload-designs

  # Generate mockups for a single product:
  python3.11 scripts/pipeline_printful_scale.py --step generate-mockups --product mountain

  # Create listings on DE only:
  python3.11 scripts/pipeline_printful_scale.py --step create-listings --market DE

  # Full pipeline:
  python3.11 scripts/pipeline_printful_scale.py --step all

  # Custom design directory:
  python3.11 scripts/pipeline_printful_scale.py --step all --design-dir /path/to/designs
""",
    )
    parser.add_argument(
        "--step", required=True,
        choices=["upload-designs", "generate-mockups", "create-listings", "all"],
        help="Pipeline step to run",
    )
    parser.add_argument(
        "--product", default="all",
        choices=ALL_PRODUCTS + ["all"],
        help="Product to process (default: all)",
    )
    parser.add_argument(
        "--market", default="all",
        choices=ALL_MARKETS + ["all"],
        help="Market to target for listings (default: all 8 EU)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would happen without calling APIs",
    )
    parser.add_argument(
        "--design-dir",
        help="Custom directory for design PNG files",
    )
    args = parser.parse_args()

    products = ALL_PRODUCTS if args.product == "all" else [args.product]
    markets = ALL_MARKETS if args.market == "all" else [args.market]
    design_dir = Path(args.design_dir) if args.design_dir else DESIGNS_DIR

    print(f"\n{'#' * 70}")
    print(f"# Printful Scale Pipeline — Summer 2026 Collection")
    print(f"# Step:     {args.step}")
    print(f"# Products: {len(products)} ({', '.join(products)})")
    print(f"# Markets:  {len(markets)} ({', '.join(markets)})")
    print(f"# Dry-run:  {args.dry_run}")
    print(f"# Designs:  {design_dir}")
    print(f"{'#' * 70}")

    all_results = {}

    # Step 1: Upload designs
    if args.step in ("upload-designs", "all"):
        r = upload_designs(products, design_dir, dry_run=args.dry_run)
        all_results["upload_designs"] = r
        print(f"\n  Upload: {r}")

    # Step 2: Generate mockups
    if args.step in ("generate-mockups", "all"):
        r = generate_mockups(products, dry_run=args.dry_run)
        all_results["generate_mockups"] = r
        print(f"\n  Mockups: {r}")

    # Step 3: Create listings
    if args.step in ("create-listings", "all"):
        r = create_listings(products, markets, dry_run=args.dry_run)
        all_results["create_listings"] = r
        print(f"\n  Listings: {r}")

    # Save results
    print(f"\n{'=' * 70}")
    print("PIPELINE SUMMARY")
    print(f"{'=' * 70}")
    for step_name, r in all_results.items():
        print(f"  {step_name}: {r}")

    with open(RESULTS_FILE, "w") as f:
        json.dump({
            "run_at": datetime.now(timezone.utc).isoformat(),
            "dry_run": args.dry_run,
            "step": args.step,
            "products": products,
            "markets": markets,
            "results": all_results,
        }, f, indent=2, ensure_ascii=False)
    print(f"\nResults saved to {RESULTS_FILE}")

    # Print SKU manifest
    print(f"\n{'=' * 70}")
    print("SKU MANIFEST (for reference)")
    print(f"{'=' * 70}")
    for pk in products:
        p = PRODUCTS[pk]
        parent = f"PFT-SUM26-{pk.upper()}"
        print(f"\n  {parent} ({p['niche']})")
        print(f"    Hat: {p['hat_type']} (catalog={p['catalog_id']})")
        for c in p["colors"]:
            print(f"    └─ {parent}-{c}")


if __name__ == "__main__":
    main()
