"""Propagate PFT-MEGA-CAP (Europe) and PFT-90229846 (Trucker) to all EU marketplaces.

Job 1: PFT-MEGA-CAP - Create parent + 22 children (11 NF + 11 FL) on 7 new markets
       (already on FR; needs DE, IT, ES, NL, PL, SE, BE)
Job 2: PFT-90229846 - Propagate parent + 17 children from DE to 7 new markets
       (already on DE; needs FR, IT, ES, NL, PL, SE, BE)

Usage:
    cd ~/nesell-analytics
    python3.11 -m etl.propagate_mega_trucker --dry-run   # preview
    python3.11 -m etl.propagate_mega_trucker              # execute
    python3.11 -m etl.propagate_mega_trucker --job 1      # MEGA only
    python3.11 -m etl.propagate_mega_trucker --job 2      # Trucker only
"""

import argparse
import json
import time
import sys
from datetime import datetime
from . import config
from .amazon_api import headers, _refresh_token, api_get
from .amazon_listings import (
    SELLER_ID, MARKETPLACE_IDS, LANG_TAGS, CURRENCIES, SIZE_SYSTEMS,
    put_listing, check_listing_exists, get_listing_full,
    NF_CHILDREN, FL_CHILDREN, TRUCKER_CHILDREN,
    TRUCKER_COLORS, TRUCKER_COLORS_DE, TRUCKER_COLORS_SE,
    TRUCKER_TRANSLATIONS,
    DE_COLORS, SE_COLORS,
    build_trucker_listing_for_mkt,
)


# ── MEGA-CAP Configuration ──────────────────────────────────────────

MEGA_PARENT_SKU = "PFT-MEGA-CAP"

# Pricing for MEGA-CAP
MEGA_NF_PRICES = {
    "DE": {"sell": 24.99, "list": 29.99, "currency": "EUR"},
    "FR": {"sell": 24.99, "list": 29.99, "currency": "EUR"},
    "IT": {"sell": 24.99, "list": 29.99, "currency": "EUR"},
    "ES": {"sell": 24.99, "list": 29.99, "currency": "EUR"},
    "NL": {"sell": 24.99, "list": 29.99, "currency": "EUR"},
    "PL": {"sell": 109.99, "list": 129.99, "currency": "PLN"},
    "SE": {"sell": 279, "list": 329, "currency": "SEK"},
    "BE": {"sell": 24.99, "list": 29.99, "currency": "EUR"},
}

MEGA_FL_PRICES = {
    "DE": {"sell": 29.99, "list": 37.99, "currency": "EUR"},
    "FR": {"sell": 29.99, "list": 37.99, "currency": "EUR"},
    "IT": {"sell": 29.99, "list": 37.99, "currency": "EUR"},
    "ES": {"sell": 29.99, "list": 37.99, "currency": "EUR"},
    "NL": {"sell": 29.99, "list": 37.99, "currency": "EUR"},
    "PL": {"sell": 129.99, "list": 159.99, "currency": "PLN"},
    "SE": {"sell": 329, "list": 399, "currency": "SEK"},
    "BE": {"sell": 29.99, "list": 37.99, "currency": "EUR"},
}

# Trucker pricing (from actual DE listing: sell=22.99, list=27.99)
TRUCKER_PRICES = {
    "DE": {"sell": 22.99, "list": 27.99, "currency": "EUR"},
    "FR": {"sell": 22.99, "list": 27.99, "currency": "EUR"},
    "IT": {"sell": 22.99, "list": 27.99, "currency": "EUR"},
    "ES": {"sell": 22.99, "list": 27.99, "currency": "EUR"},
    "NL": {"sell": 22.99, "list": 27.99, "currency": "EUR"},
    "PL": {"sell": 109.99, "list": 129.99, "currency": "PLN"},
    "SE": {"sell": 279, "list": 329, "currency": "SEK"},
    "BE": {"sell": 22.99, "list": 27.99, "currency": "EUR"},
}

# Style names per marketplace (for STYLE_NAME/COLOR_NAME variation)
STYLE_NAMES = {
    "DE": {"nf": "ohne Flagge", "fl": "mit Flagge"},
    "FR": {"nf": "sans drapeau", "fl": "avec drapeau"},
    "IT": {"nf": "senza bandiera", "fl": "con bandiera"},
    "ES": {"nf": "sin bandera", "fl": "con bandera"},
    "NL": {"nf": "zonder vlag", "fl": "met vlag"},
    "PL": {"nf": "bez flagi", "fl": "z flaga"},
    "SE": {"nf": "utan flagga", "fl": "med flagga"},
    "BE": {"nf": "zonder vlag", "fl": "met vlag"},
}

# Color translations for Dad Hat per marketplace
DAD_HAT_COLORS = {
    "DE": DE_COLORS,
    "FR": {
        "7853": "Blanc", "7854": "Noir", "7855": "Beige",
        "7856": "Bleu Clair", "7857": "Bleu Marine", "7858": "Rose",
        "7859": "Gris Pierre", "8745": "Sapin", "9794": "Camouflage Vert",
        "12735": "Canneberge", "12736": "Gris Fonce",
    },
    "IT": {
        "7853": "Bianco", "7854": "Nero", "7855": "Beige",
        "7856": "Azzurro", "7857": "Blu Marina", "7858": "Rosa",
        "7859": "Grigio Pietra", "8745": "Abete", "9794": "Camouflage Verde",
        "12735": "Mirtillo Rosso", "12736": "Grigio Scuro",
    },
    "ES": {
        "7853": "Blanco", "7854": "Negro", "7855": "Beige",
        "7856": "Azul Claro", "7857": "Azul Marino", "7858": "Rosa",
        "7859": "Gris Piedra", "8745": "Abeto", "9794": "Camuflaje Verde",
        "12735": "Arandano", "12736": "Gris Oscuro",
    },
    "NL": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige",
        "7856": "Lichtblauw", "7857": "Marineblauw", "7858": "Roze",
        "7859": "Steengrijs", "8745": "Spargroen", "9794": "Groen Camouflage",
        "12735": "Cranberry", "12736": "Donkergrijs",
    },
    "PL": {
        "7853": "Bialy", "7854": "Czarny", "7855": "Bezowy",
        "7856": "Jasnoniebieski", "7857": "Granatowy", "7858": "Rozowy",
        "7859": "Kamiennoszary", "8745": "Swierkowy", "9794": "Zielony Kamufaz",
        "12735": "Zurawinowy", "12736": "Ciemnoszary",
    },
    "SE": SE_COLORS,
    "BE": {
        "7853": "Wit", "7854": "Zwart", "7855": "Beige",
        "7856": "Lichtblauw", "7857": "Marineblauw", "7858": "Roze",
        "7859": "Steengrijs", "8745": "Spargroen", "9794": "Groen Camouflage",
        "12735": "Cranberry", "12736": "Donkergrijs",
    },
}

# Localized titles and content for MEGA-CAP (Europe)
MEGA_TRANSLATIONS = {
    "DE": {
        "item_name_parent": "Bestickte Kappe Make Europe Great Again - Verstellbare Dad Hat aus Baumwolle Unisex Baseball Cap",
        "bullets": [
            "PREMIUM MASCHINENSTICKEREI - Professionell gestickter Schriftzug auf der Vorderseite. Stickerei verblasst nicht, blaettert nicht ab und reisst nicht wie Drucke",
            "BEQUEM FUER DEN ALLTAG - Aus 100% weicher Baumwolle Chino Twill. Niedriges Profil und vorgebogener Schirm fuer den klassischen Dad-Hat-Look",
            "VERSTELLBARE PASSFORM - Metallschnallenverschluss hinten passt sich jedem Kopfumfang an. Einheitsgroesse fuer Damen und Herren",
            "PERFEKTES GESCHENK - Ideales Geschenk fuer Patrioten, Geburtstage, Feiertage oder einfach als Statement-Accessoire",
            "VIELSEITIG EINSETZBAR - Perfekt fuer Freizeit, Sport, Reisen, Festivals und den taeglichen Gebrauch bei jedem Wetter",
        ],
        "description": "Diese hochwertige bestickte Baseball Cap mit dem Schriftzug Make Europe Great Again ist der perfekte Begleiter fuer jeden Tag. Gefertigt aus 100% Baumwolle Chino Twill bietet sie hervorragenden Tragekomfort und Atmungsaktivitaet.",
        "pattern": "Buchstabenmuster",
        "age": "Erwachsener",
        "care": "Handwaesche",
        "fabric": "100% Cotton",
        "material": "Baumwolle",
        "style": "Klassisch",
        "department": "Unisex",
        "keywords": "bestickte kappe europa eu patriot geschenk baseball cap dad hat baumwolle verstellbar make europe great again unisex",
        "unit_type": "Count",
    },
    "FR": {
        "item_name_parent": "Casquette Brodee Make Europe Great Again - Dad Hat Unisex en Coton Ajustable Baseball Cap",
        "bullets": [
            "BRODERIE MACHINE PREMIUM - Inscription brodee professionnellement sur le devant. La broderie ne decolore pas, ne s'ecaille pas et ne se dechire pas comme les impressions",
            "CONFORTABLE AU QUOTIDIEN - 100% coton chino twill. Profil bas et visiere pre-courbee pour le look classique Dad Hat",
            "TAILLE AJUSTABLE - Boucle metallique a l'arriere s'adapte a toutes les tailles. Taille unique homme et femme",
            "CADEAU PARFAIT - Ideal pour les patriotes, anniversaires, fetes ou comme accessoire tendance",
            "POLYVALENT - Parfait pour les loisirs, le sport, les voyages, les festivals et le quotidien",
        ],
        "description": "Cette casquette brodee de haute qualite avec l'inscription Make Europe Great Again est le compagnon ideal. 100% coton chino twill, broderie professionnelle durable, boucle metallique ajustable.",
        "pattern": "Lettres",
        "age": "Adulte",
        "care": "Lavage a la main",
        "fabric": "100% Cotton",
        "material": "Coton",
        "style": "Classique",
        "department": "Unisexe",
        "keywords": "casquette brodee europe patriote cadeau baseball cap dad hat coton ajustable make europe great again drapeau unisex",
        "unit_type": "Count",
    },
    "IT": {
        "item_name_parent": "Cappellino Ricamato Make Europe Great Again - Dad Hat Unisex in Cotone Regolabile Baseball Cap",
        "bullets": [
            "RICAMO A MACCHINA PREMIUM - Scritta ricamata professionalmente sulla parte anteriore. Il ricamo non sbiadisce, non si stacca e non si strappa come le stampe",
            "COMODO PER OGNI GIORNO - 100% cotone chino twill. Profilo basso e visiera pre-curvata per il classico look Dad Hat",
            "TAGLIA REGOLABILE - Fibbia metallica posteriore si adatta a tutte le taglie. Taglia unica uomo e donna",
            "REGALO PERFETTO - Ideale per patrioti, compleanni, festivita o semplicemente come accessorio di tendenza",
            "VERSATILE - Perfetto per tempo libero, sport, viaggi, festival e uso quotidiano",
        ],
        "description": "Questo cappellino ricamato di alta qualita con la scritta Make Europe Great Again e il compagno ideale. 100% cotone chino twill, ricamo professionale durevole, fibbia metallica regolabile.",
        "pattern": "Lettere",
        "age": "Adulto",
        "care": "Lavaggio a mano",
        "fabric": "100% Cotton",
        "material": "Cotone",
        "style": "Classico",
        "department": "Unisex",
        "keywords": "cappellino ricamato europa patriota regalo baseball cap dad hat cotone regolabile make europe great again unisex",
        "unit_type": "Count",
    },
    "ES": {
        "item_name_parent": "Gorra Bordada Make Europe Great Again - Dad Hat Unisex de Algodon Ajustable Baseball Cap",
        "bullets": [
            "BORDADO A MAQUINA PREMIUM - Inscripcion bordada profesionalmente en la parte delantera. El bordado no destine, no se descascarilla y no se rompe como las impresiones",
            "COMODA PARA CADA DIA - 100% algodon chino twill. Perfil bajo y visera pre-curvada para el look clasico Dad Hat",
            "TALLA AJUSTABLE - Hebilla metalica trasera se adapta a todas las tallas. Talla unica hombre y mujer",
            "REGALO PERFECTO - Ideal para patriotas, cumpleanos, fiestas o simplemente como accesorio de moda",
            "VERSATIL - Perfecta para ocio, deporte, viajes, festivales y uso diario",
        ],
        "description": "Esta gorra bordada de alta calidad con la inscripcion Make Europe Great Again es el companero ideal. 100% algodon chino twill, bordado profesional duradero, hebilla metalica ajustable.",
        "pattern": "Letras",
        "age": "Adulto",
        "care": "Lavado a mano",
        "fabric": "100% Cotton",
        "material": "Algodon",
        "style": "Clasico",
        "department": "Unisex",
        "keywords": "gorra bordada europa patriota regalo baseball cap dad hat algodon ajustable make europe great again unisex",
        "unit_type": "Count",
    },
    "NL": {
        "item_name_parent": "Geborduurde Pet Make Europe Great Again - Dad Hat Unisex van Katoen Verstelbare Baseball Cap",
        "bullets": [
            "PREMIUM MACHINEBORDUURWERK - Professioneel geborduurde tekst aan de voorkant. Borduurwerk vervaagt niet, bladdert niet af en scheurt niet zoals prints",
            "COMFORTABEL VOOR ELKE DAG - 100% katoen chino twill. Laag profiel en voorgebogen klep voor de klassieke Dad Hat-look",
            "VERSTELBARE PASVORM - Metalen gesp achter past zich aan elke hoofdomvang aan. One size voor dames en heren",
            "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of gewoon als stijlvol accessoire",
            "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
        ],
        "description": "Deze hoogwaardige geborduurde pet met de tekst Make Europe Great Again is de perfecte metgezel. 100% katoen chino twill, professioneel duurzaam borduurwerk, verstelbare metalen gesp.",
        "pattern": "Letters",
        "age": "Volwassene",
        "care": "Handwas",
        "fabric": "100% Cotton",
        "material": "Katoen",
        "style": "Klassiek",
        "department": "Unisex",
        "keywords": "geborduurde pet europa patriot cadeau baseball cap dad hat katoen verstelbaar make europe great again unisex",
        "unit_type": "Count",
    },
    "PL": {
        "item_name_parent": "Haftowana Czapka Make Europe Great Again - Dad Hat Unisex z Bawelny Regulowana Baseball Cap",
        "bullets": [
            "HAFT MASZYNOWY PREMIUM - Profesjonalnie wyhaftowany napis na przodzie. Haft nie blaknie, nie luszczy sie i nie rwie jak nadruki",
            "WYGODNA NA CO DZIEN - 100% bawelna chino twill. Niski profil i wstepnie wygiete daszek dla klasycznego wygladu Dad Hat",
            "REGULOWANY ROZMIAR - Metalowa klamra z tylu dopasowuje sie do kazdego obwodu glowy. Rozmiar uniwersalny dla kobiet i mezczyzn",
            "IDEALNY PREZENT - Idealny dla patriotow, na urodziny, swieta lub po prostu jako stylowy dodatek",
            "WSZECHSTRONNA - Idealna na co dzien, sport, podroze, festiwale i codzienne uzytkowanie",
        ],
        "description": "Ta wysokiej jakosci haftowana czapka z napisem Make Europe Great Again to idealny towarzysz na co dzien. 100% bawelna chino twill, profesjonalny trwaly haft, regulowana metalowa klamra.",
        "pattern": "Litery",
        "age": "Dorosly",
        "care": "Pranie reczne",
        "fabric": "100% Cotton",
        "material": "Bawelna",
        "style": "Klasyczny",
        "department": "Unisex",
        "keywords": "haftowana czapka europa patriota prezent baseball cap dad hat bawelna regulowana make europe great again unisex",
        "unit_type": "Count",
    },
    "SE": {
        "item_name_parent": "Broderad Keps Make Europe Great Again - Dad Hat Unisex Bomull Justerbar Baseball Keps",
        "bullets": [
            "PREMIUM MASKINBRODERI - Professionellt broderad text pa framsidan. Broderier bleknar inte, flagnar inte och rivs inte som tryck",
            "BEKVAM FOR VARDAGEN - 100% bomull chino twill. Lag profil och forbojd skarm for klassisk Dad Hat-look",
            "JUSTERBAR PASSFORM - Metallspanne bak anpassar sig till alla storlekar. En storlek for dam och herr",
            "PERFEKT PRESENT - Idealisk for patrioter, fodelsedagar, hogtider eller som stiligt tillbehor",
            "MANGSIDIG - Perfekt for fritid, sport, resor, festivaler och dagligt bruk",
        ],
        "description": "Denna hogkvalitativa broderade keps med texten Make Europe Great Again ar den perfekta foljeslagaren. 100% bomull chino twill, professionellt hallbart broderi, justerbart metallspanne.",
        "pattern": "Bokstaever",
        "age": "Vuxen",
        "care": "Handtvaett",
        "fabric": "100% Cotton",
        "material": "Bomull",
        "style": "Klassisk",
        "department": "Unisex",
        "keywords": "broderad keps europa eu patriot present baseball cap dad hat bomull justerbar make europe great again unisex",
        "unit_type": "Count",
    },
    "BE": {
        "item_name_parent": "Geborduurde Pet Make Europe Great Again - Dad Hat Unisex van Katoen Verstelbare Baseball Cap",
        "bullets": [
            "PREMIUM MACHINEBORDUURWERK - Professioneel geborduurde tekst aan de voorkant. Borduurwerk vervaagt niet, bladdert niet af en scheurt niet zoals prints",
            "COMFORTABEL VOOR ELKE DAG - 100% katoen chino twill. Laag profiel en voorgebogen klep voor de klassieke Dad Hat-look",
            "VERSTELBARE PASVORM - Metalen gesp achter past zich aan elke hoofdomvang aan. One size voor dames en heren",
            "PERFECT CADEAU - Ideaal voor patriotten, verjaardagen, feestdagen of gewoon als stijlvol accessoire",
            "VEELZIJDIG - Perfect voor vrije tijd, sport, reizen, festivals en dagelijks gebruik",
        ],
        "description": "Deze hoogwaardige geborduurde pet met de tekst Make Europe Great Again is de perfecte metgezel. 100% katoen chino twill, professioneel duurzaam borduurwerk, verstelbare metalen gesp.",
        "pattern": "Letters",
        "age": "Volwassene",
        "care": "Handwas",
        "fabric": "100% Cotton",
        "material": "Katoen",
        "style": "Klassiek",
        "department": "Unisex",
        "keywords": "geborduurde pet europa patriot cadeau baseball cap dad hat katoen verstelbaar make europe great again unisex",
        "unit_type": "Count",
    },
}


# ── MEGA-CAP Listing Builders ────────────────────────────────────────

def build_mega_parent_attrs(mkt_code):
    """Build MEGA-CAP parent listing for any marketplace."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    trans = MEGA_TRANSLATIONS[mkt_code]
    # Parent uses FL list price (higher)
    fl_price = MEGA_FL_PRICES[mkt_code]

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": "Multicolor", "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "STYLE_NAME/COLOR_NAME"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": "Make Europe Great Again Dad Hat", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": trans["department"], "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS.get(mkt_code, "as3"), "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": trans["item_name_parent"], "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": fl_price["list"], "currency": fl_price["currency"], "marketplace_id": mkt_id}],
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


def build_mega_child_attrs(mkt_code, variant_id, is_flag):
    """Build MEGA-CAP child listing for any marketplace."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    trans = MEGA_TRANSLATIONS[mkt_code]
    style_key = "fl" if is_flag else "nf"
    prices = MEGA_FL_PRICES[mkt_code] if is_flag else MEGA_NF_PRICES[mkt_code]

    # Get localized color name
    color_name = DAD_HAT_COLORS.get(mkt_code, DE_COLORS).get(variant_id, DE_COLORS.get(variant_id, ""))
    style_name = STYLE_NAMES[mkt_code][style_key]

    # Build child SKU
    child_sku = f"{MEGA_PARENT_SKU}-{'FL' if is_flag else 'NF'}-{variant_id}"

    # Flag text for title
    flag_str_map = {
        "DE": " mit EU-Flagge" if is_flag else "",
        "FR": " avec Drapeau UE" if is_flag else "",
        "IT": " con Bandiera UE" if is_flag else "",
        "ES": " con Bandera UE" if is_flag else "",
        "NL": " met EU-Vlag" if is_flag else "",
        "PL": " z Flaga UE" if is_flag else "",
        "SE": " med EU-Flagga" if is_flag else "",
        "BE": " met EU-Vlag" if is_flag else "",
    }
    flag_str = flag_str_map.get(mkt_code, "")

    # Title pattern per locale
    title_patterns = {
        "DE": f"Bestickte Kappe Make Europe Great Again{flag_str} - Dad Hat Unisex Baumwolle - {color_name}",
        "FR": f"Casquette Brodee Make Europe Great Again{flag_str} - Dad Hat Unisex Coton - {color_name}",
        "IT": f"Cappellino Ricamato Make Europe Great Again{flag_str} - Dad Hat Unisex Cotone - {color_name}",
        "ES": f"Gorra Bordada Make Europe Great Again{flag_str} - Dad Hat Unisex Algodon - {color_name}",
        "NL": f"Geborduurde Pet Make Europe Great Again{flag_str} - Dad Hat Unisex Katoen - {color_name}",
        "PL": f"Haftowana Czapka Make Europe Great Again{flag_str} - Dad Hat Unisex Bawelna - {color_name}",
        "SE": f"Broderad Keps Make Europe Great Again{flag_str} - Dad Hat Unisex Bomull - {color_name}",
        "BE": f"Geborduurde Pet Make Europe Great Again{flag_str} - Dad Hat Unisex Katoen - {color_name}",
    }
    item_name = title_patterns.get(mkt_code, f"Make Europe Great Again Dad Hat - {color_name}")

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": color_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "STYLE_NAME/COLOR_NAME"}],
        "style_name": [{"value": style_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "fulfillment_availability": [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": "Make Europe Great Again Dad Hat", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": trans["department"], "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS.get(mkt_code, "as3"), "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": prices["list"], "currency": prices["currency"], "marketplace_id": mkt_id}],
        "purchasable_offer": [{"currency": prices["currency"], "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": prices["sell"]}]}], "marketplace_id": mkt_id}],
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
        "child_parent_sku_relationship": [{"marketplace_id": mkt_id, "child_relationship_type": "variation", "parent_sku": MEGA_PARENT_SKU}],
    }
    return child_sku, attrs


# ── Trucker Listing Builder (corrected pricing) ─────────────────────

def build_trucker_attrs(mkt_code, suffix, is_parent=False):
    """Build trucker listing with correct pricing from DE (22.99/27.99 EUR)."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    prices = TRUCKER_PRICES[mkt_code]

    # Use existing translations from amazon_listings.py for non-DE markets
    if mkt_code == "DE":
        # DE uses direct attributes from existing listing
        trans = {
            "item_name": "Bestickte Trucker Cap Make America Great Again - Verstellbare Snapback Kappe mit Mesh Unisex Baseball Cap",
            "bullets": [
                "PREMIUM MASCHINENSTICKEREI - Professionell gestickter Schriftzug, langlebig und waschfest. Hochwertige Stickerei, die auch nach vielen Waeschen wie neu aussieht.",
                "BEQUEM FUER DEN ALLTAG - Robuste Baumwoll-Front mit luftdurchlaessigem Mesh-Ruecken fuer optimale Belueftung. Vorgebogener Schirm, Trucker-Stil.",
                "VERSTELLBARE PASSFORM - Snapback-Verschluss hinten, Einheitsgroesse fuer Kopfumfang 55-62 cm. Unisex Trucker Cap fuer Damen und Herren.",
                "PERFEKTES GESCHENK - Ideal fuer Patrioten, Geburtstage, Feiertage oder als modisches Statement-Piece. Ein originelles Geschenk fuer jeden Anlass.",
                "VIELSEITIG EINSETZBAR - Ob Freizeit, Sport, Reisen, Festivals oder Alltag - dieses Trucker Cap passt zu jedem Outfit und jeder Gelegenheit.",
            ],
            "description": "Diese hochwertige bestickte Trucker Cap mit dem Schriftzug Make America Great Again ist der perfekte Begleiter. Strukturierte Baumwoll-Front, atmungsaktives Mesh-Netz, professionelles Broderi, verstellbarer Snapback-Verschluss.",
            "pattern": "Buchstabenmuster",
            "age": "Erwachsener",
            "care": "Handwaesche",
            "fabric": "Baumwolle, Polyester Mesh",
            "material": "Baumwolle, Polyester",
            "style": "Klassisch",
            "keywords": "bestickte trucker cap amerika usa patriot geschenk snapback mesh baumwolle verstellbar make america great again unisex",
            "unit_type": "Count",
        }
    else:
        trans = TRUCKER_TRANSLATIONS[mkt_code]

    if is_parent:
        color_value = "Mehrfarbig" if mkt_code == "DE" else ("Multicolor" if mkt_code not in ("SE",) else "Flerfaergad")
        item_name = trans["item_name"] if mkt_code != "DE" else trans["item_name"]
    else:
        color_value = TRUCKER_COLORS.get(mkt_code, TRUCKER_COLORS_DE).get(suffix, TRUCKER_COLORS_DE.get(suffix, ""))
        item_name = f"{trans['item_name']} - {color_value}"

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": color_value, "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": "Make America Great Again Trucker Cap", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": "1981316031", "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "nesell", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS.get(mkt_code, "as3"), "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": "Cotton", "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": prices["list"], "currency": prices["currency"], "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "fabric_type": [{"value": trans["fabric"], "language_tag": lang, "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "material": [{"value": trans["material"], "language_tag": lang, "marketplace_id": mkt_id}],
        "style": [{"value": trans["style"], "language_tag": lang, "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": "baseball_cap", "marketplace_id": mkt_id}],
        "care_instructions": [{"value": trans["care"], "language_tag": lang, "marketplace_id": mkt_id}],
        "unit_count": [{"type": {"value": "Count", "language_tag": "en_US"}, "value": 1.0, "marketplace_id": mkt_id}],
        "target_gender": [{"value": "unisex", "marketplace_id": mkt_id}],
    }

    if is_parent:
        attrs["parentage_level"] = [{"marketplace_id": mkt_id, "value": "parent"}]
        attrs["child_parent_sku_relationship"] = [{"marketplace_id": mkt_id, "child_relationship_type": "variation"}]
    else:
        attrs["fulfillment_availability"] = [{"fulfillment_channel_code": "DEFAULT", "quantity": 999}]
        attrs["purchasable_offer"] = [{"currency": prices["currency"], "audience": "ALL", "our_price": [{"schedule": [{"value_with_tax": prices["sell"]}]}], "marketplace_id": mkt_id}]
        attrs["parentage_level"] = [{"marketplace_id": mkt_id, "value": "child"}]
        attrs["child_parent_sku_relationship"] = [{"marketplace_id": mkt_id, "child_relationship_type": "variation", "parent_sku": "PFT-90229846"}]

    return attrs


# ── Task Functions ───────────────────────────────────────────────────

def job1_mega_cap(dry_run=False):
    """Job 1: Create PFT-MEGA-CAP parent + 22 children on all 8 EU marketplaces."""
    print("=" * 70)
    print("JOB 1: PFT-MEGA-CAP (Make Europe Great Again) - All EU Marketplaces")
    print("=" * 70)

    target_mkts = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]
    variant_ids = NF_CHILDREN  # 11 variants

    created = 0
    skipped = 0
    errors = 0

    # First, fetch images from FR parent to replicate
    print("\nFetching images from FR parent...")
    fr_parent = get_listing_full(MEGA_PARENT_SKU, MARKETPLACE_IDS["FR"])
    fr_attrs = fr_parent.get("attributes", {})
    parent_images = {}
    if "main_product_image_locator" in fr_attrs:
        parent_images["main"] = fr_attrs["main_product_image_locator"][0].get("media_location", "")
    for i in range(1, 8):
        key = f"other_product_image_locator_{i}"
        if key in fr_attrs:
            parent_images[f"other_{i}"] = fr_attrs[key][0].get("media_location", "")
    print(f"  Found {len(parent_images)} parent images")

    for mkt_code in target_mkts:
        mkt_id = MARKETPLACE_IDS[mkt_code]
        print(f"\n{'='*50}")
        print(f"  Marketplace: {mkt_code} ({mkt_id})")
        print(f"{'='*50}")

        # Parent
        if check_listing_exists(MEGA_PARENT_SKU, mkt_id):
            print(f"  Parent {MEGA_PARENT_SKU} already exists on {mkt_code}, skipping")
            skipped += 1
        else:
            attrs = build_mega_parent_attrs(mkt_code)
            # Add images from FR parent
            if parent_images.get("main"):
                attrs["main_product_image_locator"] = [{"media_location": parent_images["main"], "marketplace_id": mkt_id}]
            for i in range(1, 8):
                key = f"other_{i}"
                if parent_images.get(key):
                    attrs[f"other_product_image_locator_{i}"] = [{"media_location": parent_images[key], "marketplace_id": mkt_id}]

            status, resp = put_listing(MEGA_PARENT_SKU, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(1.5)

        # NF Children (11)
        print(f"\n  --- NF Children (11) ---")
        for vid in variant_ids:
            child_sku, attrs = build_mega_child_attrs(mkt_code, vid, is_flag=False)

            if check_listing_exists(child_sku, mkt_id):
                print(f"  {child_sku} already exists on {mkt_code}, skipping")
                skipped += 1
                continue

            status, resp = put_listing(child_sku, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(0.5)

        # FL Children (11)
        print(f"\n  --- FL Children (11) ---")
        for vid in variant_ids:
            child_sku, attrs = build_mega_child_attrs(mkt_code, vid, is_flag=True)

            if check_listing_exists(child_sku, mkt_id):
                print(f"  {child_sku} already exists on {mkt_code}, skipping")
                skipped += 1
                continue

            status, resp = put_listing(child_sku, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(0.5)

    print(f"\n{'='*70}")
    print(f"JOB 1 Complete: {created} created, {skipped} skipped, {errors} errors")
    print(f"{'='*70}")
    return created, skipped, errors


def job2_trucker(dry_run=False):
    """Job 2: Propagate PFT-90229846 (Trucker) from DE to all other EU marketplaces."""
    print("=" * 70)
    print("JOB 2: PFT-90229846 (Trucker Cap) - Propagate from DE")
    print("=" * 70)

    trucker_parent = "PFT-90229846"
    target_mkts = ["FR", "IT", "ES", "NL", "PL", "SE", "BE"]

    created = 0
    skipped = 0
    errors = 0

    # Fetch images from DE
    print("\nFetching image URLs from DE listings...")
    de_images = {}
    for suffix in TRUCKER_CHILDREN:
        child_sku = f"{trucker_parent}-{suffix}"
        data = get_listing_full(child_sku, MARKETPLACE_IDS["DE"])
        attrs = data.get("attributes", {})
        images = {}
        if "main_product_image_locator" in attrs:
            images["main"] = attrs["main_product_image_locator"][0].get("media_location", "")
        for i in range(1, 8):
            key = f"other_product_image_locator_{i}"
            if key in attrs:
                images[f"other_{i}"] = attrs[key][0].get("media_location", "")
        de_images[suffix] = images
        time.sleep(0.5)
    print(f"  Collected images for {len(de_images)} children")

    # Also get parent images
    parent_data = get_listing_full(trucker_parent, MARKETPLACE_IDS["DE"])
    parent_attrs = parent_data.get("attributes", {})
    parent_images = {}
    if "main_product_image_locator" in parent_attrs:
        parent_images["main"] = parent_attrs["main_product_image_locator"][0].get("media_location", "")
    for i in range(1, 8):
        key = f"other_product_image_locator_{i}"
        if key in parent_attrs:
            parent_images[f"other_{i}"] = parent_attrs[key][0].get("media_location", "")

    for mkt_code in target_mkts:
        mkt_id = MARKETPLACE_IDS[mkt_code]
        print(f"\n{'='*50}")
        print(f"  Marketplace: {mkt_code} ({mkt_id})")
        print(f"{'='*50}")

        # Parent
        if check_listing_exists(trucker_parent, mkt_id):
            print(f"  Parent {trucker_parent} already exists on {mkt_code}, skipping")
            skipped += 1
        else:
            attrs = build_trucker_attrs(mkt_code, None, is_parent=True)
            # Add parent images
            if parent_images.get("main"):
                attrs["main_product_image_locator"] = [{"media_location": parent_images["main"], "marketplace_id": mkt_id}]
            for i in range(1, 8):
                key = f"other_{i}"
                if parent_images.get(key):
                    attrs[f"other_product_image_locator_{i}"] = [{"media_location": parent_images[key], "marketplace_id": mkt_id}]

            status, resp = put_listing(trucker_parent, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(1.5)

        # Children
        for suffix in TRUCKER_CHILDREN:
            child_sku = f"{trucker_parent}-{suffix}"

            if check_listing_exists(child_sku, mkt_id):
                print(f"  {child_sku} already exists on {mkt_code}, skipping")
                skipped += 1
                continue

            attrs = build_trucker_attrs(mkt_code, suffix, is_parent=False)

            # Add images from DE
            imgs = de_images.get(suffix, {})
            if imgs.get("main"):
                attrs["main_product_image_locator"] = [{"media_location": imgs["main"], "marketplace_id": mkt_id}]
            for i in range(1, 8):
                key = f"other_{i}"
                if imgs.get(key):
                    attrs[f"other_product_image_locator_{i}"] = [{"media_location": imgs[key], "marketplace_id": mkt_id}]

            status, resp = put_listing(child_sku, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202):
                created += 1
            else:
                errors += 1
            time.sleep(0.5)

    print(f"\n{'='*70}")
    print(f"JOB 2 Complete: {created} created, {skipped} skipped, {errors} errors")
    print(f"{'='*70}")
    return created, skipped, errors


# ── Main ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Propagate MEGA-CAP and Trucker to all EU marketplaces")
    parser.add_argument("--job", type=int, choices=[1, 2], help="Run specific job (1=MEGA, 2=Trucker)")
    parser.add_argument("--dry-run", action="store_true", help="Preview without executing")
    args = parser.parse_args()

    print(f"\n{'#'*70}")
    print(f"# MEGA-CAP + Trucker EU Propagation - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"# {'DRY RUN' if args.dry_run else 'LIVE EXECUTION'}")
    print(f"{'#'*70}\n")

    total_created = 0
    total_skipped = 0
    total_errors = 0

    if args.job is None or args.job == 1:
        c, s, e = job1_mega_cap(dry_run=args.dry_run)
        total_created += c
        total_skipped += s
        total_errors += e
        print()

    if args.job is None or args.job == 2:
        c, s, e = job2_trucker(dry_run=args.dry_run)
        total_created += c
        total_skipped += s
        total_errors += e
        print()

    print(f"\n{'#'*70}")
    print(f"# GRAND TOTAL: {total_created} created, {total_skipped} skipped, {total_errors} errors")
    print(f"{'#'*70}")


if __name__ == "__main__":
    main()
