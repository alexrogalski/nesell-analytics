"""Create DRAFT Amazon DE listings for Cat Puzzle Feeder + LED Night Light 2-pack.

These are pre-launch INACTIVE drafts — no EAN, no images, no price.
Run: cd ~/nesell-analytics && python3.11 scripts/create_new_products_de.py [--dry-run]

Task: task_KreYgppeLttC
"""
import argparse
import json
import sys
import os

# Add parent dir so we can import etl
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.amazon_api import headers, _refresh_token
from etl.amazon_listings import api_put, put_listing, SELLER_ID, MARKETPLACE_IDS

DE_MKT = MARKETPLACE_IDS["DE"]
LANG = "de_DE"

# ── Product 1: Interactive Cat Puzzle Feeder ────────────────────────

CAT_FEEDER_SKU = "NP-CAT-PUZZLE-001"
CAT_FEEDER_PRODUCT_TYPE = "PET_TOY"

CAT_FEEDER_TITLE = (
    "Interaktives Katzen Intelligenzspielzeug Puzzle Feeder - "
    "Futterspielzeug für Langsames Fressen mit Mehreren Schwierigkeitsstufen, "
    "Beschäftigung für Wohnungskatzen"
)
# 148 chars

CAT_FEEDER_BULLETS = [
    (
        "GEISTIGE STIMULATION — Fordern Sie Ihre Katze mit einem mehrstufigen "
        "Puzzle-Feeder heraus, der natürliche Jagdinstinkte aktiviert und "
        "Langeweile bei Wohnungskatzen effektiv vorbeugt"
    ),
    (
        "LANGSAMES FRESSEN — Verlangsamt die Futteraufnahme um bis zu 10x "
        "gegenüber normalem Napf. Reduziert Schlingen, fördert die Verdauung "
        "und beugt Übergewicht bei Katzen vor"
    ),
    (
        "MEHRERE SCHWIERIGKEITSSTUFEN — Vom Anfänger bis zum Profi: Passen "
        "Sie die Herausforderung an die Intelligenz und Erfahrung Ihrer "
        "Katze an. Ideal für alle Altersgruppen"
    ),
    (
        "LEBENSMITTELECHTES MATERIAL — Hergestellt aus BPA-freiem, "
        "robustem Kunststoff. Spülmaschinenfest für einfache Reinigung. "
        "Rutschfeste Unterseite verhindert Verrutschen beim Spielen"
    ),
    (
        "PERFEKTES GESCHENK — Ideales Katzenspielzeug für Geburtstage, "
        "Weihnachten oder als Erstausstattung. Geeignet für Trockenfutter "
        "und Leckerlis. Maße ca. 25 x 25 x 6 cm"
    ),
]

CAT_FEEDER_DESCRIPTION = (
    "Beschäftigen Sie Ihre Katze artgerecht mit unserem interaktiven "
    "Puzzle-Feeder!\n\n"
    "Wohnungskatzen brauchen geistige Herausforderungen, um gesund und "
    "glücklich zu bleiben. Unser Intelligenzspielzeug verwandelt die "
    "Fütterungszeit in ein spannendes Abenteuer, das natürliche "
    "Jagdinstinkte Ihrer Katze weckt.\n\n"
    "DAS PROBLEM: Viele Katzen fressen zu schnell aus normalen Näpfen, "
    "was zu Verdauungsproblemen und Übergewicht führt. Gleichzeitig leiden "
    "besonders Wohnungskatzen unter Langeweile.\n\n"
    "DIE LÖSUNG: Unser mehrstufiges Puzzle-System kombiniert geistige "
    "Stimulation mit kontrollierter Futteraufnahme. Die verschiedenen "
    "Schwierigkeitsstufen wachsen mit den Fähigkeiten Ihrer Katze mit.\n\n"
    "EIGENSCHAFTEN:\n"
    "- Mehrere austauschbare Puzzle-Elemente für steigende Schwierigkeit\n"
    "- Rutschfeste Gummifüße für stabilen Stand auf allen Böden\n"
    "- Lebensmittelechter, BPA-freier Kunststoff\n"
    "- Spülmaschinenfest — hygienisch und pflegeleicht\n"
    "- Geeignet für Trocken- und Nassfutter sowie Leckerlis\n\n"
    "IDEAL FÜR: Wohnungskatzen, Katzen die zu schnell fressen, "
    "übergewichtige Katzen, und als tägliche Beschäftigung für "
    "neugierige Samtpfoten jeden Alters.\n\n"
    "Mit 15,7 Millionen Katzen ist Deutschland eines der "
    "katzenfreundlichsten Länder Europas. Geben Sie Ihrer Katze die "
    "Beschäftigung, die sie verdient!\n\n"
    "GPSR-Konformität: Verantwortliche Person in der EU gemäß "
    "Verordnung (EU) 2023/988."
)

CAT_FEEDER_SEARCH_TERMS = (
    "futterlabyrinth katze anti schling napf haustier denkspiel "
    "slow feeder katzenbeschäftigung spielzeug indoor stubentiger "
    "futterstation intelligenz puzzle treat dispenser wohnungskatze "
    "snackball activity board nahrungssuche gehirntraining"
)

# ── Product 2: LED Motion Sensor Night Light (2-pack) ──────────────

LED_LIGHT_SKU = "NP-LED-NIGHT-001"
LED_LIGHT_PRODUCT_TYPE = "NIGHT_LIGHT"

LED_LIGHT_TITLE = (
    "LED Nachtlicht mit Bewegungsmelder USB Aufladbar 2er Pack - "
    "Warmweiß 2700K Orientierungslicht Steckdose Magnetisch, "
    "Nachtlampe für Flur Schlafzimmer Treppe"
)
# 151 chars

LED_LIGHT_BULLETS = [
    (
        "INTELLIGENTER BEWEGUNGSMELDER — Schaltet sich automatisch ein "
        "bei Bewegung im 120° Erfassungswinkel bis 3 Meter Reichweite. "
        "Leuchtet 30 Sekunden und schaltet sich dann selbstständig ab"
    ),
    (
        "AUGENSCHONENDES WARMWEISS 2700K — Sanftes, blendfreies Licht "
        "stört nicht den Schlafrhythmus. Perfekt für nächtliche Gänge "
        "zum Bad oder zur Küche ohne grelles Deckenlicht einschalten"
    ),
    (
        "USB-C AUFLADBAR — Kein Batteriewechsel nötig. Einmal aufladen, "
        "wochenlange Nutzung. Umweltfreundlich und kosteneffizient im "
        "Vergleich zu batteriebetriebenen Nachtlichtern"
    ),
    (
        "MAGNETISCHE MONTAGE — Starker Magnet und Klebepad ermöglichen "
        "flexible Platzierung an Wand, Schrank oder Metalloberflächen. "
        "Kein Bohren oder Werkzeug erforderlich. Sofort einsatzbereit"
    ),
    (
        "2ER SET IM LIEFERUMFANG — Zwei Nachtlichter für Flur und "
        "Schlafzimmer. Kompaktes Design (8 x 8 x 2,5 cm) passt überall "
        "hin. Ideal für Kinder, Senioren und als Orientierungslicht"
    ),
]

LED_LIGHT_DESCRIPTION = (
    "Sicherheit und Komfort in der Nacht — mit unserem USB-C "
    "aufladbaren LED Nachtlicht mit Bewegungsmelder im praktischen "
    "2er Set.\n\n"
    "Kennen Sie das? Sie stehen nachts auf und müssen sich im Dunkeln "
    "orientieren. Grelles Deckenlicht würde Sie und Ihre Familie wecken. "
    "Unser intelligentes Nachtlicht erkennt Ihre Bewegung und schaltet "
    "sich automatisch mit warmweißem 2700K Licht ein — gerade hell genug "
    "für sichere Orientierung, ohne den Schlaf zu stören.\n\n"
    "WARUM USB-C STATT BATTERIEN?\n"
    "- Einmal aufladen für Wochen der Nutzung\n"
    "- Keine laufenden Batteriekosten\n"
    "- Umweltfreundlich: kein Batteriemüll\n"
    "- Moderner USB-C Anschluss — kompatibel mit vorhandenen Ladekabeln\n\n"
    "FLEXIBEL EINSETZBAR:\n"
    "- Flur und Treppenhaus: Sichere Orientierung nachts\n"
    "- Schlafzimmer: Sanftes Licht ohne aufzuwachen\n"
    "- Kinderzimmer: Beruhigendes Nachtlicht für Kinder\n"
    "- Badezimmer: Kein Lichtschalter-Suchen in der Nacht\n"
    "- Kleiderschrank: Automatische Beleuchtung beim Öffnen\n\n"
    "TECHNISCHE DATEN:\n"
    "- Lichtfarbe: Warmweiß 2700K\n"
    "- Erfassungswinkel: 120°\n"
    "- Reichweite: bis 3 Meter\n"
    "- Leuchtdauer: 30 Sekunden nach letzter Bewegung\n"
    "- Akku: USB-C aufladbar, ca. 3-4 Wochen Laufzeit\n"
    "- Maße: ca. 8 x 8 x 2,5 cm pro Licht\n"
    "- Montage: Magnet + 3M Klebepad (beides im Lieferumfang)\n\n"
    "Das perfekte Geschenk für Eltern, Großeltern und alle, die sich "
    "mehr Sicherheit und Komfort in der Nacht wünschen.\n\n"
    "GPSR-Konformität: Verantwortliche Person in der EU gemäß "
    "Verordnung (EU) 2023/988. CE-gekennzeichnet."
)

LED_LIGHT_SEARCH_TERMS = (
    "steckdosenlicht nachtlicht kinder baby kinderzimmer dimmbar "
    "energiesparend schrankbeleuchtung treppenbeleuchtung kabellos "
    "wiederaufladbar seniorenlicht magnetisch toilettenlicht nachtlampe"
)


# ── Build Attributes ────────────────────────────────────────────────

GPSR_MANUFACTURER = [{"component": [
    {"type": "name", "value": "nesell"},
    {"type": "street_address", "value": "Nawrot 12"},
    {"type": "city", "value": "Lodz"},
    {"type": "country", "value": "PL"},
    {"type": "postal_code", "value": "90-010"},
    {"type": "email", "value": "contact@nesell.store"},
]}]


def build_cat_feeder_attrs():
    """Build SP-API attributes for Cat Puzzle Feeder (PET_TOY).

    All required attributes validated against SP-API schema.
    Successfully submitted: 2026-03-16, submission 20d34de7a29643439d09ebf476b2fbdc
    """
    return {
        "item_name": [{"value": CAT_FEEDER_TITLE, "language_tag": LANG, "marketplace_id": DE_MKT}],
        "bullet_point": [
            {"value": b, "language_tag": LANG, "marketplace_id": DE_MKT}
            for b in CAT_FEEDER_BULLETS
        ],
        "product_description": [{"value": CAT_FEEDER_DESCRIPTION, "language_tag": LANG, "marketplace_id": DE_MKT}],
        "generic_keyword": [{"value": CAT_FEEDER_SEARCH_TERMS, "language_tag": LANG, "marketplace_id": DE_MKT}],
        "brand": [{"value": "nesell", "language_tag": LANG}],
        "manufacturer": [{"value": "nesell", "language_tag": LANG}],
        "condition_type": [{"value": "new_new"}],
        "model_name": [{"value": "Interactive Cat Puzzle Feeder", "language_tag": LANG, "marketplace_id": DE_MKT}],
        "model_number": [{"value": "NP-CPF-2026", "marketplace_id": DE_MKT}],
        "item_length_width_height": [{"length": {"unit": "centimeters", "value": 25}, "width": {"unit": "centimeters", "value": 25}, "height": {"unit": "centimeters", "value": 6}, "marketplace_id": DE_MKT}],
        "directions": [{"value": "Füllen Sie das Puzzle mit Trockenfutter oder Leckerlis. Beginnen Sie mit der einfachsten Stufe.", "language_tag": LANG, "marketplace_id": DE_MKT}],
        "number_of_boxes": [{"value": 1, "marketplace_id": DE_MKT}],
        "power_plug_type": [{"value": "no_plug", "marketplace_id": DE_MKT}],
        "color": [{"value": "Blau", "language_tag": LANG, "marketplace_id": DE_MKT}],
        "recommended_browse_nodes": [{"value": "3550339031", "marketplace_id": DE_MKT}],
        "contains_liquid_contents": [{"value": "false", "marketplace_id": DE_MKT}],
        "unit_count": [{"value": 1, "type": {"value": "stück", "language_tag": LANG}, "marketplace_id": DE_MKT}],
        "included_components": [{"value": "Interaktives Katzen-Puzzle-Spielzeug, Anleitung", "language_tag": LANG, "marketplace_id": DE_MKT}],
        "country_of_origin": [{"value": "CN", "marketplace_id": DE_MKT}],
        "list_price": [{"value_with_tax": 29.99, "currency": "EUR", "marketplace_id": DE_MKT}],
        "contains_food_or_beverage": [{"value": "false", "marketplace_id": DE_MKT}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": DE_MKT}],
        "item_package_dimensions": [{"length": {"unit": "centimeters", "value": 28}, "width": {"unit": "centimeters", "value": 28}, "height": {"unit": "centimeters", "value": 8}, "marketplace_id": DE_MKT}],
        "item_package_weight": [{"value": 0.5, "unit": "kilograms", "marketplace_id": DE_MKT}],
        "warranty_description": [{"value": "2 Jahre Garantie", "language_tag": LANG, "marketplace_id": DE_MKT}],
        "specific_uses_for_product": [{"value": "Katzenspielzeug, Futterspielzeug, Slow Feeder", "language_tag": LANG, "marketplace_id": DE_MKT}],
        "batteries_required": [{"value": "false", "marketplace_id": DE_MKT}],
        "supplier_declared_has_product_identifier_exemption": [{"value": "true", "marketplace_id": DE_MKT}],
        "gpsr_safety_attestation": [{"value": "true"}],
        "gpsr_manufacturer_reference": GPSR_MANUFACTURER,
    }


def build_led_light_attrs():
    """Build SP-API attributes for LED Night Light 2-pack."""
    return {
        "item_name": [{"value": LED_LIGHT_TITLE, "language_tag": LANG, "marketplace_id": DE_MKT}],
        "bullet_point": [
            {"value": b, "language_tag": LANG, "marketplace_id": DE_MKT}
            for b in LED_LIGHT_BULLETS
        ],
        "product_description": [{"value": LED_LIGHT_DESCRIPTION, "language_tag": LANG, "marketplace_id": DE_MKT}],
        "generic_keyword": [{"value": LED_LIGHT_SEARCH_TERMS, "language_tag": LANG, "marketplace_id": DE_MKT}],
        "brand": [{"value": "nesell", "language_tag": LANG}],
        "manufacturer": [{"value": "nesell", "language_tag": LANG}],
        "condition_type": [{"value": "new_new"}],
        "gpsr_safety_attestation": [{"value": "true"}],
        "gpsr_manufacturer_reference": [{
            "component": [
                {"type": "name", "value": "nesell"},
                {"type": "street_address", "value": "Nawrot 12"},
                {"type": "city", "value": "Lodz"},
                {"type": "country", "value": "PL"},
                {"type": "postal_code", "value": "90-010"},
                {"type": "email", "value": "contact@nesell.store"},
            ]
        }],
        "gpsr_eu_responsible_person": [{
            "component": [
                {"type": "name", "value": "nesell"},
                {"type": "street_address", "value": "Nawrot 12"},
                {"type": "city", "value": "Lodz"},
                {"type": "country", "value": "PL"},
                {"type": "postal_code", "value": "90-010"},
                {"type": "email", "value": "contact@nesell.store"},
            ]
        }],
    }


# ── Main ────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Create draft Amazon DE listings for new products")
    parser.add_argument("--dry-run", action="store_true", help="Print payloads without calling API")
    parser.add_argument("--product", choices=["cat", "led", "both"], default="both",
                        help="Which product to create (default: both)")
    args = parser.parse_args()

    products = []
    if args.product in ("cat", "both"):
        products.append(("Cat Puzzle Feeder", CAT_FEEDER_SKU, CAT_FEEDER_PRODUCT_TYPE, build_cat_feeder_attrs()))
    if args.product in ("led", "both"):
        products.append(("LED Night Light 2-pack", LED_LIGHT_SKU, LED_LIGHT_PRODUCT_TYPE, build_led_light_attrs()))

    for name, sku, product_type, attrs in products:
        print(f"\n{'='*60}")
        print(f"Creating DRAFT listing: {name}")
        print(f"  SKU: {sku}")
        print(f"  Product Type: {product_type}")
        print(f"  Marketplace: DE ({DE_MKT})")
        print(f"  Title ({len(attrs['item_name'][0]['value'])} chars): {attrs['item_name'][0]['value'][:80]}...")
        print(f"  Bullets: {len(attrs['bullet_point'])}")
        print(f"  Search terms ({len(attrs['generic_keyword'][0]['value'])} bytes): {attrs['generic_keyword'][0]['value'][:60]}...")
        print(f"{'='*60}")

        if args.dry_run:
            print(f"\n[DRY-RUN] Full payload for {sku}:")
            payload = {
                "productType": product_type,
                "requirements": "LISTING",
                "attributes": attrs,
            }
            print(json.dumps(payload, indent=2, ensure_ascii=False)[:3000])
            print("... (truncated)")
        else:
            print(f"\nSubmitting to SP-API...")
            status, resp = put_listing(sku, DE_MKT, attrs, product_type=product_type)
            print(f"\nResult: HTTP {status}")
            print(json.dumps(resp, indent=2, ensure_ascii=False)[:1000])

    print(f"\n{'='*60}")
    print("IMPORTANT: These are DRAFT listings.")
    print("Missing before activation:")
    print("  - EAN/GTIN barcode")
    print("  - Product images (main + 6 lifestyle/infographic)")
    print("  - Price")
    print("  - Inventory/fulfillment channel")
    print(f"{'='*60}")


if __name__ == "__main__":
    main()
