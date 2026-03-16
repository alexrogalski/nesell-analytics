#!/usr/bin/env python3.11
"""
Create 5 new Printful hat products in Baselinker with full multilingual
Amazon EU listings for 8 markets: DE, FR, IT, ES, NL, SE, PL, BE.

Products (from bsr-opportunities-2026-03-16.md):
1. Otto Cap 18-772 Washed Dad Hat       (Printful ID 961) — €22.99
2. Capstone Organic Bucket Hat          (Printful ID 547) — €34.99
3. Big Accessories BX003 Bucket Hat     (Printful ID 379) — €26.99
4. Otto Cap 104-1018 Distressed Dad Hat (Printful ID 396) — €24.99
5. Beechfield B682 Corduroy Cap         (Printful ID 532) — €26.99

Baselinker IDs (discovered 2026-03-16):
  inventory_id   = 52954  (Printful)
  price_group_id = 31059  (Główna EUR)
  warehouse_id   = bl_79555  (Printful)
  category_id    = 3732161   (Czapki z daszkiem)
"""

import requests
import json
import time
import sys
from datetime import datetime
from pathlib import Path

# ── Config ────────────────────────────────────────────────────────────────────

KEYS_DIR = Path.home() / ".keys"
BL_TOKEN = ""
for line in (KEYS_DIR / "baselinker.env").read_text().splitlines():
    if line.startswith("BASELINKER_API_TOKEN="):
        BL_TOKEN = line.split("=", 1)[1].strip()

BL_URL = "https://api.baselinker.com/connector.php"
INVENTORY_ID = 52954
PRICE_GROUP_ID = 31059
WAREHOUSE_ID = "bl_79555"
CATEGORY_ID = 3732161

# Printful CDN placeholder images (public Printful product renders)
# NOTE: Replace with actual design mockups after creating designs in Printful
PRINTFUL_IMAGES = {
    961: [  # Washed Dad Hat
        "https://files.cdn.printful.com/products/961/catalog_primary.jpg",
        "https://files.cdn.printful.com/products/961/catalog_secondary.jpg",
    ],
    547: [  # Organic Bucket Hat
        "https://files.cdn.printful.com/products/547/catalog_primary.jpg",
        "https://files.cdn.printful.com/products/547/catalog_secondary.jpg",
    ],
    379: [  # BX003 Bucket Hat
        "https://files.cdn.printful.com/products/379/catalog_primary.jpg",
        "https://files.cdn.printful.com/products/379/catalog_secondary.jpg",
    ],
    396: [  # Distressed Dad Hat
        "https://files.cdn.printful.com/products/396/catalog_primary.jpg",
        "https://files.cdn.printful.com/products/396/catalog_secondary.jpg",
    ],
    532: [  # Corduroy Cap
        "https://files.cdn.printful.com/products/532/catalog_primary.jpg",
        "https://files.cdn.printful.com/products/532/catalog_secondary.jpg",
    ],
}

# ── Baselinker client ─────────────────────────────────────────────────────────

class BL:
    def __init__(self, token):
        self.session = requests.Session()
        self.session.headers["X-BLToken"] = token
        self._last = 0.0

    def call(self, method, params=None):
        elapsed = time.time() - self._last
        if elapsed < 0.65:
            time.sleep(0.65 - elapsed)
        r = self.session.post(BL_URL, data={
            "method": method,
            "parameters": json.dumps(params or {}),
        })
        self._last = time.time()
        data = r.json()
        if data.get("status") == "ERROR":
            raise RuntimeError(f"BL error [{data.get('error_code')}]: {data.get('error_message')}")
        return data

    def add_product(self, product_data):
        product_data["inventory_id"] = str(INVENTORY_ID)
        product_data.setdefault("product_id", "0")
        return self.call("addInventoryProduct", product_data)

# ── Product definitions ───────────────────────────────────────────────────────

PRODUCTS = [
    {
        "printful_id": 961,
        "internal_code": "WDH",  # Washed Dad Hat
        "weight": 0.12,
        "tax_rate": 0,
        "average_cost": 12.50,
        "colors": [
            {"code": "KHA", "name_en": "Khaki", "name_de": "Khaki", "name_fr": "Kaki", "name_it": "Cachi", "name_es": "Caqui", "name_nl": "Khaki", "name_se": "Khaki", "name_pl": "Khaki"},
            {"code": "STN", "name_en": "Stone", "name_de": "Stein", "name_fr": "Pierre", "name_it": "Pietra", "name_es": "Piedra", "name_nl": "Steen", "name_se": "Sten", "name_pl": "Kamień"},
            {"code": "SGE", "name_en": "Sage", "name_de": "Salbei", "name_fr": "Sauge", "name_it": "Salvia", "name_es": "Salvia", "name_nl": "Salie", "name_se": "Salvia", "name_pl": "Szałwia"},
            {"code": "NVY", "name_en": "Navy", "name_de": "Navy", "name_fr": "Marine", "name_it": "Blu navy", "name_es": "Marino", "name_nl": "Marine", "name_se": "Marinblå", "name_pl": "Granatowy"},
        ],
        "price_eur": 22.99,
        "text_fields": {
            "name":    "Garment Washed Dad Hat | Vintage Baseball Cap | Adjustable Unisex | Spring Summer",
            "name|de": "Garment Washed Dad Hat | Vintage Basecap | Verstellbar Unisex | Frühling Sommer",
            "name|fr": "Dad Hat Délavée Vintage | Casquette Baseball Réglable Unisex | Printemps Été",
            "name|it": "Dad Hat Slavato Vintage | Berretto Baseball Regolabile Unisex | Primavera Estate",
            "name|es": "Gorra Dad Hat Lavada Vintage | Béisbol Ajustable Unisex | Primavera Verano",
            "name|nl": "Gewassen Dad Hat | Vintage Baseball Cap | Verstelbaar Unisex | Lente Zomer",
            "name|sv": "Tvättad Dad Hat | Vintage Basebollkeps | Justerbar Unisex | Vår Sommar",
            "name|pl": "Prany Dad Hat | Vintage Czapka z Daszkiem | Regulowana Unisex | Wiosna Lato",
            "description": """<ul>
<li><b>GARMENT WASHED</b> – Soft, lived-in look with a vintage-washed finish that gets better with wear</li>
<li><b>PREMIUM COTTON</b> – 100% cotton twill construction, lightweight and breathable for all-day comfort</li>
<li><b>ADJUSTABLE FIT</b> – One size fits most with adjustable strap — perfect for men and women</li>
<li><b>EMBROIDERED DESIGN</b> – High-quality embroidery on the front panel, durable and fade-resistant</li>
<li><b>SPRING 2026 TREND</b> – The washed/vintage dad hat is the #1 trending style for spring 2026</li>
</ul>""",
            "description|de": """<ul>
<li><b>GARMENT WASHED</b> – Weiche, gelebte Optik mit Vintage-Waschung — wird mit jedem Tragen schöner</li>
<li><b>PREMIUM BAUMWOLLE</b> – 100% Baumwoll-Twill, leicht und atmungsaktiv für ganztägigen Komfort</li>
<li><b>VERSTELLBAR</b> – One Size mit verstellbarem Riemen — perfekt für Damen und Herren</li>
<li><b>STICKEREI</b> – Hochwertige Stickerei auf der Frontseite, langlebig und farbecht</li>
<li><b>FRÜHJAHR 2026 TREND</b> – Der Washed Dad Hat ist der meistgefragte Stil des Frühlings 2026</li>
</ul>""",
            "description|fr": """<ul>
<li><b>GARMENT WASHED</b> – Look vintage délavé et doux, s'améliore avec le port</li>
<li><b>COTON PREMIUM</b> – 100% coton sergé, légère et respirante pour un confort toute la journée</li>
<li><b>RÉGLABLE</b> – Taille unique avec sangle réglable — parfait pour femme et homme</li>
<li><b>BRODERIE</b> – Broderie haute qualité sur la face avant, durable et résistante à la décoloration</li>
<li><b>TENDANCE PRINTEMPS 2026</b> – La dad hat délavée est la tendance #1 du printemps 2026</li>
</ul>""",
            "description|it": """<ul>
<li><b>GARMENT WASHED</b> – Look vissuto e morbido con finitura vintage lavata</li>
<li><b>COTONE PREMIUM</b> – 100% twill di cotone, leggero e traspirante per il massimo comfort</li>
<li><b>REGOLABILE</b> – Taglia unica con cinturino regolabile — perfetto per donna e uomo</li>
<li><b>RICAMATO</b> – Ricamo di alta qualità sul pannello frontale, durevole e resistente</li>
<li><b>TENDENZA PRIMAVERA 2026</b> – Il dad hat vintage è il look #1 della primavera 2026</li>
</ul>""",
            "description|es": """<ul>
<li><b>GARMENT WASHED</b> – Aspecto vintage lavado y suave, mejora con el uso</li>
<li><b>ALGODÓN PREMIUM</b> – 100% sarga de algodón, ligera y transpirable para máxima comodidad</li>
<li><b>AJUSTABLE</b> – Talla única con correa ajustable — perfecto para mujer y hombre</li>
<li><b>BORDADO</b> – Bordado de alta calidad en el panel frontal, duradero y resistente</li>
<li><b>TENDENCIA PRIMAVERA 2026</b> – La gorra dad hat lavada es la tendencia #1 de primavera 2026</li>
</ul>""",
            "description|nl": """<ul>
<li><b>GARMENT WASHED</b> – Zachte, vintage-gewassen uitstraling die steeds mooier wordt</li>
<li><b>PREMIUM KATOEN</b> – 100% katoenen keperstof, licht en ademend voor dagelijks draagcomfort</li>
<li><b>VERSTELBAAR</b> – One size met verstelbare band — perfect voor dames en heren</li>
<li><b>GEBORDUURD</b> – Hoogwaardig borduurwerk op het voorpaneel, duurzaam en kleurecht</li>
<li><b>LENTE 2026 TREND</b> – De gewassen dad hat is dé trendstijl van lente 2026</li>
</ul>""",
            "description|pl": """<ul>
<li><b>GARMENT WASHED</b> – Miękki, vintageowy wygląd z efektem prania — nabiera charakteru z czasem</li>
<li><b>PREMIUM BAWEŁNA</b> – 100% bawełniana tkanina twill, lekka i oddychająca</li>
<li><b>REGULOWANA</b> – Rozmiar uniwersalny z regulowanym paskiem — dla kobiet i mężczyzn</li>
<li><b>HAFT</b> – Wysokiej jakości haft na przednim panelu, trwały i odporny na blaknięcie</li>
<li><b>TREND WIOSNA 2026</b> – Prany dad hat to trend nr 1 na wiosnę 2026</li>
</ul>""",
            "features": {"Material": "Cotton Twill", "Fit": "Adjustable", "Style": "Dad Hat", "Season": "Spring/Summer"},
            "features|de": {"Material": "Baumwoll-Twill", "Passform": "Verstellbar", "Stil": "Dad Hat", "Saison": "Frühling/Sommer"},
            "features|fr": {"Matière": "Sergé de coton", "Coupe": "Réglable", "Style": "Dad Hat", "Saison": "Printemps/Été"},
            "features|it": {"Materiale": "Twill di cotone", "Vestibilità": "Regolabile", "Stile": "Dad Hat", "Stagione": "Primavera/Estate"},
            "features|es": {"Material": "Sarga de algodón", "Ajuste": "Ajustable", "Estilo": "Dad Hat", "Temporada": "Primavera/Verano"},
            "features|pl": {"Materiał": "Bawełna twill", "Dopasowanie": "Regulowane", "Styl": "Dad Hat", "Sezon": "Wiosna/Lato"},
        },
    },
    {
        "printful_id": 547,
        "internal_code": "OBH",  # Organic Bucket Hat
        "weight": 0.12,
        "tax_rate": 0,
        "average_cost": 21.50,
        "colors": [
            {"code": "BLK", "name_en": "Black", "name_de": "Schwarz", "name_fr": "Noir", "name_it": "Nero", "name_es": "Negro", "name_nl": "Zwart", "name_se": "Svart", "name_pl": "Czarny"},
            {"code": "NVY", "name_en": "Navy", "name_de": "Navy", "name_fr": "Marine", "name_it": "Blu navy", "name_es": "Marino", "name_nl": "Marine", "name_se": "Marinblå", "name_pl": "Granatowy"},
            {"code": "SND", "name_en": "Sand", "name_de": "Sand", "name_fr": "Sable", "name_it": "Sabbia", "name_es": "Arena", "name_nl": "Zand", "name_se": "Sand", "name_pl": "Piaskowy"},
        ],
        "price_eur": 34.99,
        "text_fields": {
            "name":    "Organic Cotton Bucket Hat | GOTS Certified Eco-Friendly | Adjustable Unisex | Summer",
            "name|de": "Bio-Baumwolle Bucket Hat | Organischer Fischerhut | GOTS Zertifiziert | Nachhaltig Unisex",
            "name|fr": "Bob Chapeau Coton Biologique | Éco-Responsable GOTS | Pêcheur Unisex | Printemps Été",
            "name|it": "Cappello Pescatore Cotone Biologico | GOTS Certificato | Eco-Friendly Unisex | Estate",
            "name|es": "Sombrero Bob Algodón Orgánico | Certificado GOTS | Eco-Friendly Unisex | Verano",
            "name|nl": "Organic Cotton Bucket Hat | GOTS Gecertificeerd | Duurzaam Unisex | Lente Zomer",
            "name|sv": "Ekologisk Bomull Bucket Hat | GOTS-Certifierad | Hållbar Unisex | Vår Sommar",
            "name|pl": "Czapka Bucket Hat Bawełna Organiczna | Certyfikat GOTS | Ekologiczna Unisex | Lato",
            "description": """<ul>
<li><b>ORGANIC COTTON</b> – Made from certified organic cotton — better for you and the planet</li>
<li><b>GOTS CERTIFIED</b> – Global Organic Textile Standard certification ensures ethical and sustainable production</li>
<li><b>BUCKET HAT TREND</b> – Y2K revival: the bucket hat is the biggest summer fashion trend of 2026</li>
<li><b>PREMIUM EMBROIDERY</b> – High-quality embroidered design, durable and fade-resistant</li>
<li><b>UNISEX DESIGN</b> – Adjustable fit, perfect for women and men, festivals, beach, and everyday wear</li>
</ul>""",
            "description|de": """<ul>
<li><b>BIO-BAUMWOLLE</b> – Aus zertifizierter Bio-Baumwolle – besser für Sie und den Planeten</li>
<li><b>GOTS ZERTIFIZIERT</b> – GOTS-Zertifizierung gewährleistet ethische und nachhaltige Produktion</li>
<li><b>BUCKET HAT TREND</b> – Y2K Revival: Der Fischerhut ist der größte Sommertrend 2026</li>
<li><b>PREMIUM STICKEREI</b> – Hochwertige Stickerei, langlebig und farbecht</li>
<li><b>UNISEX</b> – Verstellbar, perfekt für Damen und Herren, Festival, Strand und Alltag</li>
</ul>""",
            "description|fr": """<ul>
<li><b>COTON BIOLOGIQUE</b> – Fabriqué en coton biologique certifié — meilleur pour vous et la planète</li>
<li><b>CERTIFIÉ GOTS</b> – Certification GOTS garantit une production éthique et durable</li>
<li><b>TENDANCE BOB 2026</b> – Retour Y2K : le bob est la plus grande tendance mode de l'été 2026</li>
<li><b>BRODERIE PREMIUM</b> – Design brodé haute qualité, durable et résistant aux lavages</li>
<li><b>UNISEXE</b> – Taille réglable, parfait pour femme et homme, festival, plage et usage quotidien</li>
</ul>""",
            "description|it": """<ul>
<li><b>COTONE BIOLOGICO</b> – Realizzato in cotone biologico certificato — rispettoso di te e del pianeta</li>
<li><b>CERTIFICATO GOTS</b> – Certificazione GOTS garantisce produzione etica e sostenibile</li>
<li><b>TREND ESTATE 2026</b> – Revival Y2K: il cappello pescatore è il trend più grande dell'estate 2026</li>
<li><b>RICAMO PREMIUM</b> – Design ricamato di alta qualità, durevole e resistente ai lavaggi</li>
<li><b>UNISEX</b> – Taglia regolabile, perfetto per donna e uomo, festival, spiaggia e uso quotidiano</li>
</ul>""",
            "description|es": """<ul>
<li><b>ALGODÓN ORGÁNICO</b> – Hecho de algodón orgánico certificado — mejor para ti y el planeta</li>
<li><b>CERTIFICADO GOTS</b> – Certificación GOTS garantiza producción ética y sostenible</li>
<li><b>TENDENCIA VERANO 2026</b> – Revival Y2K: el sombrero bob es la mayor tendencia de moda del verano 2026</li>
<li><b>BORDADO PREMIUM</b> – Diseño bordado de alta calidad, duradero y resistente al lavado</li>
<li><b>UNISEX</b> – Ajuste regulable, perfecto para mujer y hombre, festival, playa y uso diario</li>
</ul>""",
            "description|nl": """<ul>
<li><b>BIOLOGISCH KATOEN</b> – Gemaakt van gecertificeerd biologisch katoen — goed voor jou en de planeet</li>
<li><b>GOTS GECERTIFICEERD</b> – GOTS-certificering garandeert ethische en duurzame productie</li>
<li><b>BUCKET HAT TREND</b> – Y2K revival: de bucket hat is dé zomertrend van 2026</li>
<li><b>PREMIUM BORDUURWERK</b> – Hoogwaardig geborduurde design, duurzaam en kleurecht</li>
<li><b>UNISEX</b> – Verstelbaar, perfect voor dames en heren, festival, strand en dagelijks gebruik</li>
</ul>""",
            "description|pl": """<ul>
<li><b>BAWEŁNA ORGANICZNA</b> – Z certyfikowanej bawełny organicznej — lepsza dla Ciebie i planety</li>
<li><b>CERTYFIKAT GOTS</b> – Certyfikacja GOTS gwarantuje etyczną i zrównoważoną produkcję</li>
<li><b>TREND LATO 2026</b> – Y2K revival: bucket hat to największy trend modowy lata 2026</li>
<li><b>HAFT PREMIUM</b> – Wysokiej jakości haft, trwały i odporny na pranie</li>
<li><b>UNISEX</b> – Regulowany, idealny dla kobiet i mężczyzn, festiwale, plaża i codzień</li>
</ul>""",
            "features": {"Material": "Organic Cotton", "Certification": "GOTS", "Style": "Bucket Hat", "Season": "Summer"},
            "features|de": {"Material": "Bio-Baumwolle", "Zertifizierung": "GOTS", "Stil": "Fischerhut/Bucket Hat", "Saison": "Sommer"},
            "features|fr": {"Matière": "Coton biologique", "Certification": "GOTS", "Style": "Bob/Bucket Hat", "Saison": "Été"},
            "features|es": {"Material": "Algodón orgánico", "Certificación": "GOTS", "Estilo": "Sombrero Bob", "Temporada": "Verano"},
            "features|pl": {"Materiał": "Bawełna organiczna", "Certyfikat": "GOTS", "Styl": "Bucket Hat", "Sezon": "Lato"},
        },
    },
    {
        "printful_id": 379,
        "internal_code": "BBH",  # BX003 Bucket Hat
        "weight": 0.11,
        "tax_rate": 0,
        "average_cost": 16.29,
        "colors": [
            {"code": "BLK", "name_en": "Black", "name_de": "Schwarz", "name_fr": "Noir", "name_it": "Nero", "name_es": "Negro", "name_nl": "Zwart", "name_se": "Svart", "name_pl": "Czarny"},
            {"code": "NVY", "name_en": "Navy", "name_de": "Navy", "name_fr": "Marine", "name_it": "Blu navy", "name_es": "Marino", "name_nl": "Marine", "name_se": "Marinblå", "name_pl": "Granatowy"},
            {"code": "WHT", "name_en": "White", "name_de": "Weiß", "name_fr": "Blanc", "name_it": "Bianco", "name_es": "Blanco", "name_nl": "Wit", "name_se": "Vit", "name_pl": "Biały"},
        ],
        "price_eur": 26.99,
        "text_fields": {
            "name":    "Bucket Hat Summer Festival | Unisex Adjustable Fisherman Hat | Y2K Cotton Cap | Women Men",
            "name|de": "Bucket Hat Fischerhut Sommer | Festival Unisex Verstellbar | Y2K Trend Baumwolle | Damen Herren",
            "name|fr": "Bob Chapeau Été Festival | Casquette Pêcheur Unisex Réglable | Coton Tendance Y2K | Femme Homme",
            "name|it": "Cappello Pescatore Estate Festival | Unisex Regolabile | Cotone Tendenza Y2K | Donna Uomo",
            "name|es": "Sombrero Bob Verano Festival | Gorra Pescador Unisex Ajustable | Algodón Tendencia Y2K",
            "name|nl": "Bucket Hat Zomer Festival | Unisex Verstelbaar | Y2K Katoenen Vissershoed | Dames Heren",
            "name|sv": "Bucket Hat Sommar Festival | Unisex Justerbar Fiskarhatt | Y2K Bomull | Dam Herr",
            "name|pl": "Bucket Hat Lato Festiwal | Unisex Regulowana Czapka Wędkarska | Y2K Bawełna",
            "description": """<ul>
<li><b>Y2K REVIVAL TREND</b> – Bucket hats are back: the #1 summer fashion trend with +350% growth since 2020</li>
<li><b>PREMIUM COTTON</b> – Durable cotton construction, comfortable and lightweight for all-day wear</li>
<li><b>FESTIVAL READY</b> – The perfect hat for summer festivals, beach days, and outdoor adventures</li>
<li><b>UNISEX FIT</b> – Adjustable design that fits all head sizes — great gift for women and men</li>
<li><b>EMBROIDERED DESIGN</b> – Unique embroidered artwork on the front, screen-proof quality</li>
</ul>""",
            "description|de": """<ul>
<li><b>Y2K REVIVAL TREND</b> – Bucket Hats sind zurück: Sommertrend Nr. 1 mit +350% Wachstum seit 2020</li>
<li><b>PREMIUM BAUMWOLLE</b> – Strapazierfähige Baumwolle, angenehm und leicht für den ganzen Tag</li>
<li><b>FESTIVAL READY</b> – Der perfekte Hut für Sommerfestivals, Strandtage und Outdoor-Abenteuer</li>
<li><b>UNISEX</b> – Verstellbares Design für alle Kopfgrößen — tolles Geschenk für Damen und Herren</li>
<li><b>STICKEREI</b> – Einzigartiges Stickmotiv auf der Vorderseite, langlebig und ausdrucksstark</li>
</ul>""",
            "description|fr": """<ul>
<li><b>TENDANCE Y2K</b> – Le bob est de retour : tendance mode #1 de l'été avec +350% de croissance depuis 2020</li>
<li><b>COTON PREMIUM</b> – Construction en coton résistant, léger et confortable toute la journée</li>
<li><b>PARFAIT POUR LES FESTIVALS</b> – Le chapeau idéal pour les festivals d'été, la plage et les aventures</li>
<li><b>UNISEXE</b> – Design réglable pour toutes les tailles — excellent cadeau pour femme et homme</li>
<li><b>BRODERIE</b> – Broderie unique sur la face avant, qualité premium et durable</li>
</ul>""",
            "description|it": """<ul>
<li><b>TENDENZA Y2K</b> – Il cappello pescatore è tornato: tendenza estate #1 con +350% di crescita dal 2020</li>
<li><b>COTONE PREMIUM</b> – Costruzione in cotone resistente, leggera e comoda per tutto il giorno</li>
<li><b>PERFETTO PER FESTIVAL</b> – Il cappello ideale per festival estivi, spiaggia e avventure all'aperto</li>
<li><b>UNISEX</b> – Design regolabile per tutte le taglie — ottimo regalo per donna e uomo</li>
<li><b>RICAMATO</b> – Disegno ricamato unico sul pannello frontale, qualità premium e durevole</li>
</ul>""",
            "description|es": """<ul>
<li><b>TENDENCIA Y2K</b> – El sombrero bob vuelve: tendencia moda #1 del verano con +350% de crecimiento desde 2020</li>
<li><b>ALGODÓN PREMIUM</b> – Construcción de algodón resistente, ligera y cómoda todo el día</li>
<li><b>PERFECTO PARA FESTIVALES</b> – El sombrero ideal para festivales de verano, playa y aventuras</li>
<li><b>UNISEX</b> – Diseño ajustable para todos los tamaños — gran regalo para mujer y hombre</li>
<li><b>BORDADO</b> – Diseño bordado único en el panel frontal, calidad premium y duradero</li>
</ul>""",
            "description|nl": """<ul>
<li><b>Y2K REVIVAL TREND</b> – Bucket hats zijn terug: zomertrend #1 met +350% groei sinds 2020</li>
<li><b>PREMIUM KATOEN</b> – Duurzame katoenen constructie, licht en comfortabel voor de hele dag</li>
<li><b>FESTIVAL READY</b> – De perfecte hoed voor zomerfestivals, stranddagen en buitenactiviteiten</li>
<li><b>UNISEX</b> – Verstelbaar design voor alle maten — geweldig cadeau voor dames en heren</li>
<li><b>GEBORDUURD</b> – Uniek borduurwerk op het voorpaneel, premium kwaliteit en duurzaam</li>
</ul>""",
            "description|pl": """<ul>
<li><b>TREND Y2K</b> – Bucket haty wróciły: trend modowy lata nr 1 z +350% wzrostem od 2020</li>
<li><b>PREMIUM BAWEŁNA</b> – Wytrzymała bawełna, lekka i komfortowa przez cały dzień</li>
<li><b>NA FESTIWALE</b> – Idealny kapelusz na letnie festiwale, plaże i wyprawy plenerowe</li>
<li><b>UNISEX</b> – Regulowany design dla każdego rozmiaru głowy — świetny prezent</li>
<li><b>HAFT</b> – Unikalne hafcione zdobienie na przednim panelu, trwałe i wyraziste</li>
</ul>""",
            "features": {"Material": "Cotton", "Style": "Bucket Hat", "Season": "Summer", "Trend": "Y2K"},
            "features|de": {"Material": "Baumwolle", "Stil": "Bucket Hat / Fischerhut", "Saison": "Sommer", "Trend": "Y2K"},
            "features|fr": {"Matière": "Coton", "Style": "Bob / Bucket Hat", "Saison": "Été", "Tendance": "Y2K"},
            "features|it": {"Materiale": "Cotone", "Stile": "Cappello Pescatore", "Stagione": "Estate", "Trend": "Y2K"},
            "features|es": {"Material": "Algodón", "Estilo": "Sombrero Bob", "Temporada": "Verano", "Tendencia": "Y2K"},
            "features|pl": {"Materiał": "Bawełna", "Styl": "Bucket Hat", "Sezon": "Lato", "Trend": "Y2K"},
        },
    },
    {
        "printful_id": 396,
        "internal_code": "DDH",  # Distressed Dad Hat
        "weight": 0.13,
        "tax_rate": 0,
        "average_cost": 14.99,
        "colors": [
            {"code": "BLK", "name_en": "Black", "name_de": "Schwarz", "name_fr": "Noir", "name_it": "Nero", "name_es": "Negro", "name_nl": "Zwart", "name_se": "Svart", "name_pl": "Czarny"},
            {"code": "CGY", "name_en": "Charcoal Grey", "name_de": "Anthrazit", "name_fr": "Gris anthracite", "name_it": "Grigio antracite", "name_es": "Gris antracita", "name_nl": "Antraciet", "name_se": "Antracitgrå", "name_pl": "Antracytowy"},
            {"code": "KHA", "name_en": "Khaki", "name_de": "Khaki", "name_fr": "Kaki", "name_it": "Cachi", "name_es": "Caqui", "name_nl": "Khaki", "name_se": "Khaki", "name_pl": "Khaki"},
            {"code": "NVY", "name_en": "Navy", "name_de": "Navy", "name_fr": "Marine", "name_it": "Blu navy", "name_es": "Marino", "name_nl": "Marine", "name_se": "Marinblå", "name_pl": "Granatowy"},
        ],
        "price_eur": 24.99,
        "text_fields": {
            "name":    "Distressed Dad Hat | Vintage Retro Baseball Cap | Used Look | Adjustable Unisex | Spring 2026",
            "name|de": "Vintage Distressed Dad Hat | Used Look Basecap | Retro Unisex Verstellbar | Frühling 2026",
            "name|fr": "Casquette Dad Hat Vintage Délavée | Style Usé Rétro | Réglable Unisex | Printemps 2026",
            "name|it": "Dad Hat Vintage Consumato | Berretto Retrò Regolabile | Look Vissuto | Unisex | Primavera 2026",
            "name|es": "Gorra Dad Hat Vintage Desgastada | Retro Ajustable Unisex | Estilo Usado | Primavera 2026",
            "name|nl": "Distressed Dad Hat | Vintage Retro Look | Verstelbaar Unisex Baseball Cap | Lente 2026",
            "name|sv": "Distressed Dad Hat | Vintage Retro Basebollkeps | Sliten Look | Justerbar Unisex",
            "name|pl": "Vintage Distressed Dad Hat | Retro Czapka z Daszkiem | Used Look | Regulowana Unisex",
            "description": """<ul>
<li><b>DISTRESSED VINTAGE LOOK</b> – Pre-worn, lived-in aesthetic — the #1 retro style trend for spring/summer 2026</li>
<li><b>PREMIUM COTTON</b> – Durable cotton construction with authentic worn-in feel and texture</li>
<li><b>UNSTRUCTURED FIT</b> – Soft, low-profile silhouette that molds to your head for ultimate comfort</li>
<li><b>ADJUSTABLE STRAP</b> – One size fits most with metal buckle adjustment — unisex design</li>
<li><b>EMBROIDERED DESIGN</b> – Retro-inspired embroidery on the front panel, premium quality stitching</li>
</ul>""",
            "description|de": """<ul>
<li><b>VINTAGE DISTRESSED LOOK</b> – Vorgetragenere Ästhetik — der Retro-Stil-Trend Nr. 1 für Frühling/Sommer 2026</li>
<li><b>PREMIUM BAUMWOLLE</b> – Strapazierfähige Baumwolle mit authentischem Vintage-Charakter</li>
<li><b>UNSTRUKTURIERT</b> – Weiches, flaches Profil das sich der Kopfform anpasst</li>
<li><b>VERSTELLBAR</b> – One Size mit Metallschnalle — Unisex Design für Damen und Herren</li>
<li><b>STICKEREI</b> – Retro-inspirierte Stickerei auf der Vorderseite, hochwertige Verarbeitung</li>
</ul>""",
            "description|fr": """<ul>
<li><b>LOOK VINTAGE DÉLAVÉ</b> – Esthétique rétro pré-usée — la tendance style #1 du printemps/été 2026</li>
<li><b>COTON PREMIUM</b> – Construction en coton résistant avec un toucher vintage authentique</li>
<li><b>PROFIL BAS</b> – Silhouette souple et basse qui épouse la forme de votre tête</li>
<li><b>RÉGLABLE</b> – Taille unique avec boucle métallique — design unisexe femme et homme</li>
<li><b>BRODERIE</b> – Broderie d'inspiration rétro sur la face avant, couture de qualité premium</li>
</ul>""",
            "description|it": """<ul>
<li><b>LOOK VINTAGE CONSUMATO</b> – Estetica pre-consumata — la tendenza stile retrò #1 di primavera/estate 2026</li>
<li><b>COTONE PREMIUM</b> – Costruzione in cotone resistente con autentica sensazione vintage</li>
<li><b>NON STRUTTURATO</b> – Silhouette morbida e bassa che si adatta alla forma della testa</li>
<li><b>REGOLABILE</b> – Taglia unica con fibbia metallica — design unisex donna e uomo</li>
<li><b>RICAMATO</b> – Ricamo ispirato al retrò sul pannello frontale, cucitura di alta qualità</li>
</ul>""",
            "description|es": """<ul>
<li><b>LOOK VINTAGE DESGASTADO</b> – Estética pre-envejecida — la tendencia retro #1 de primavera/verano 2026</li>
<li><b>ALGODÓN PREMIUM</b> – Construcción en algodón resistente con auténtica sensación vintage</li>
<li><b>PERFIL BAJO</b> – Silueta suave y baja que se adapta a la forma de tu cabeza</li>
<li><b>AJUSTABLE</b> – Talla única con hebilla metálica — diseño unisex mujer y hombre</li>
<li><b>BORDADO</b> – Bordado de inspiración retro en el panel frontal, costura de calidad premium</li>
</ul>""",
            "description|nl": """<ul>
<li><b>VINTAGE DISTRESSED LOOK</b> – Pre-versleten esthetiek — de retro stijltrend #1 voor lente/zomer 2026</li>
<li><b>PREMIUM KATOEN</b> – Duurzame katoenen constructie met authentieke vintage uitstraling</li>
<li><b>ONGESTRUCTUREERD</b> – Zacht, laag profiel dat zich aanpast aan de hoofdvorm</li>
<li><b>VERSTELBAAR</b> – One size met metalen gesp — unisex design voor dames en heren</li>
<li><b>GEBORDUURD</b> – Retro-geïnspireerd borduurwerk op het voorpaneel, premium kwaliteit</li>
</ul>""",
            "description|pl": """<ul>
<li><b>VINTAGE DISTRESSED LOOK</b> – Efekt przetarcia, styl retro — trend nr 1 na wiosnę/lato 2026</li>
<li><b>PREMIUM BAWEŁNA</b> – Wytrzymała bawełna z autentycznym vintageowym charakterem</li>
<li><b>NIESTRUKTURYZOWANA</b> – Miękki, niski profil dopasowujący się do kształtu głowy</li>
<li><b>REGULOWANA</b> – Rozmiar uniwersalny z metalową klamrą — design unisex</li>
<li><b>HAFT</b> – Retro-inspirowany haft na przednim panelu, wysoka jakość szycia</li>
</ul>""",
            "features": {"Material": "Cotton", "Style": "Distressed Dad Hat", "Season": "Spring/Summer", "Aesthetic": "Vintage/Retro"},
            "features|de": {"Material": "Baumwolle", "Stil": "Distressed Dad Hat", "Saison": "Frühling/Sommer", "Ästhetik": "Vintage/Retro"},
            "features|fr": {"Matière": "Coton", "Style": "Dad Hat Délavée", "Saison": "Printemps/Été", "Esthétique": "Vintage/Rétro"},
            "features|es": {"Material": "Algodón", "Estilo": "Dad Hat Desgastada", "Temporada": "Primavera/Verano", "Estética": "Vintage/Retro"},
            "features|pl": {"Materiał": "Bawełna", "Styl": "Distressed Dad Hat", "Sezon": "Wiosna/Lato", "Estetyka": "Vintage/Retro"},
        },
    },
    {
        "printful_id": 532,
        "internal_code": "CCH",  # Corduroy Cap
        "weight": 0.14,
        "tax_rate": 0,
        "average_cost": 16.95,
        "colors": [
            {"code": "BLK", "name_en": "Black", "name_de": "Schwarz", "name_fr": "Noir", "name_it": "Nero", "name_es": "Negro", "name_nl": "Zwart", "name_se": "Svart", "name_pl": "Czarny"},
            {"code": "CML", "name_en": "Camel", "name_de": "Kamel", "name_fr": "Camel", "name_it": "Cammello", "name_es": "Camel", "name_nl": "Kameel", "name_se": "Kamel", "name_pl": "Camel"},
            {"code": "OLV", "name_en": "Dark Olive", "name_de": "Dunkeloliv", "name_fr": "Olive foncé", "name_it": "Oliva scuro", "name_es": "Verde oliva oscuro", "name_nl": "Donker olijfgroen", "name_se": "Mörkoliv", "name_pl": "Ciemna oliwka"},
            {"code": "ONV", "name_en": "Oxford Navy", "name_de": "Oxford Navy", "name_fr": "Marine Oxford", "name_it": "Blu Oxford", "name_es": "Azul marino Oxford", "name_nl": "Oxford Marine", "name_se": "Oxford Marinblå", "name_pl": "Oxford Granatowy"},
        ],
        "price_eur": 26.99,
        "text_fields": {
            "name":    "Corduroy Cap Baseball Hat | Adjustable Unisex | Premium Embroidered | Spring Fashion 2026",
            "name|de": "Cord Cap Corduroy Kappe | Kord Basecap Herren Damen | Verstellbar Unisex | Stickerei Frühling 2026",
            "name|fr": "Casquette Velours Côtelé | Corduroy Cap Brodée Unisex Réglable | Mode Printemps 2026",
            "name|it": "Cappellino Velluto a Coste | Corduroy Cap Ricamato Unisex Regolabile | Moda Primavera 2026",
            "name|es": "Gorra Pana Corduroy | Gorra Béisbol Bordada Unisex Ajustable | Moda Primavera 2026",
            "name|nl": "Corduroy Cap | Ribfluwelen Baseball Pet | Verstelbaar Unisex | Geborduurde Lente 2026",
            "name|sv": "Manchesterkeps Corduroy Cap | Justerbar Unisex | Broderad | Vårmode 2026",
            "name|pl": "Sztruksowa Czapka Corduroy | Regulowana Unisex | Haftowana | Moda Wiosna 2026",
            "description": """<ul>
<li><b>2026 TEXTURE TREND</b> – Corduroy caps are the breakout fashion trend for spring/fall 2026 — textured headwear is everywhere</li>
<li><b>PREMIUM CORDUROY</b> – Soft ribbed corduroy fabric in on-trend earth tone colorways (Camel, Olive, Navy, Black)</li>
<li><b>FASHION-FORWARD</b> – Minimal premium embroidery on a statement texture — the Brixton/Goorin Bros. look at a fair price</li>
<li><b>ADJUSTABLE FIT</b> – Brass/metal buckle at rear, fits all head sizes — unisex design for men and women</li>
<li><b>ZERO COMPETITION</b> – Virtually no POD sellers offer embroidered corduroy caps on Amazon EU — first-mover advantage</li>
</ul>""",
            "description|de": """<ul>
<li><b>TREND 2026</b> – Cord Caps sind der Fashion-Durchbruch für Frühling/Herbst 2026 — texturierte Kopfbedeckungen sind überall</li>
<li><b>PREMIUM KORD</b> – Weiches geripptes Cord-Gewebe in trendigen Erdtönen (Kamel, Olive, Navy, Schwarz)</li>
<li><b>FASHION-FORWARD</b> – Minimalistische Stickerei auf Textur-Statement — Brixton/Goorin Bros. Look zum fairen Preis</li>
<li><b>VERSTELLBAR</b> – Metallschnalle hinten, für alle Kopfgrößen — Unisex für Damen und Herren</li>
<li><b>EINZIGARTIG</b> – Kaum ein POD-Anbieter auf Amazon EU bietet bestickte Cord-Caps — First-Mover-Vorteil</li>
</ul>""",
            "description|fr": """<ul>
<li><b>TENDANCE 2026</b> – Les casquettes en velours côtelé sont LA tendance mode pour printemps/automne 2026</li>
<li><b>VELOURS CÔTELÉ PREMIUM</b> – Tissu côtelé doux dans des coloris tendance terres (Camel, Olive, Marine, Noir)</li>
<li><b>FASHION-FORWARD</b> – Broderie minimaliste premium sur texture statement — look Brixton/Goorin Bros. à prix juste</li>
<li><b>RÉGLABLE</b> – Boucle en laiton à l'arrière, s'adapte à toutes les tailles — design unisexe</li>
<li><b>UNIQUE</b> – Presque aucun vendeur POD sur Amazon EU ne propose des casquettes en velours brodées</li>
</ul>""",
            "description|it": """<ul>
<li><b>TENDENZA 2026</b> – I cappellini in velluto a coste sono LA tendenza moda per primavera/autunno 2026</li>
<li><b>VELLUTO A COSTE PREMIUM</b> – Tessuto coste morbido in colori tendenza terra (Cammello, Oliva, Navy, Nero)</li>
<li><b>FASHION-FORWARD</b> – Ricamo minimale premium su texture statement — look Brixton/Goorin Bros. a prezzo equo</li>
<li><b>REGOLABILE</b> – Fibbia in ottone sul retro, si adatta a tutte le misure — design unisex</li>
<li><b>UNICO</b> – Quasi nessun venditore POD su Amazon EU offre cappellini corduroy ricamati — vantaggio first-mover</li>
</ul>""",
            "description|es": """<ul>
<li><b>TENDENCIA 2026</b> – Las gorras de pana son LA tendencia moda para primavera/otoño 2026</li>
<li><b>PANA PREMIUM</b> – Tejido de pana suave en colores tendencia tierra (Camel, Oliva, Marino, Negro)</li>
<li><b>FASHION-FORWARD</b> – Bordado minimalista premium en textura statement — look Brixton/Goorin Bros. a precio justo</li>
<li><b>AJUSTABLE</b> – Hebilla de latón en la parte trasera, se adapta a todos los tamaños — diseño unisex</li>
<li><b>ÚNICO</b> – Prácticamente ningún vendedor POD en Amazon EU ofrece gorras de pana bordadas</li>
</ul>""",
            "description|nl": """<ul>
<li><b>TREND 2026</b> – Corduroy caps zijn DE fashion trend voor lente/herfst 2026 — getextureerd hoofddeksels overal</li>
<li><b>PREMIUM RIBFLUWEEL</b> – Zacht geribbeld corduroy in trendy aardtinten (Kameel, Olijf, Marine, Zwart)</li>
<li><b>FASHION-FORWARD</b> – Minimalistisch premium borduurwerk op statement textuur — Brixton/Goorin Bros. look</li>
<li><b>VERSTELBAAR</b> – Metalen gesp achteraan, past op alle hoofdmaten — unisex design</li>
<li><b>UNIEK</b> – Vrijwel geen POD-verkoper op Amazon EU biedt geborduurde corduroy caps aan</li>
</ul>""",
            "description|pl": """<ul>
<li><b>TREND 2026</b> – Sztruksowe czapki to przełom modowy na wiosnę/jesień 2026</li>
<li><b>PREMIUM SZTRUKS</b> – Miękki prążkowany materiał w modnych ziemistych kolorach (Camel, Oliwka, Granat, Czarny)</li>
<li><b>FASHION-FORWARD</b> – Minimalistyczny haft premium na wyrazistej teksturze — look Brixton/Goorin Bros.</li>
<li><b>REGULOWANA</b> – Metalowa klamra z tyłu, pasuje do każdego rozmiaru głowy — design unisex</li>
<li><b>UNIKALNE</b> – Prawie żaden sprzedawca POD na Amazon EU nie oferuje haftowanych czapek sztruksowych</li>
</ul>""",
            "features": {"Material": "Corduroy / Cord", "Style": "Baseball Cap", "Season": "Spring/Fall", "Trend": "2026 Texture"},
            "features|de": {"Material": "Cord / Kord", "Stil": "Basecap / Cord Cap", "Saison": "Frühling/Herbst", "Trend": "2026 Textur"},
            "features|fr": {"Matière": "Velours côtelé", "Style": "Casquette baseball", "Saison": "Printemps/Automne", "Tendance": "2026 Texture"},
            "features|es": {"Material": "Pana / Corduroy", "Estilo": "Gorra de béisbol", "Temporada": "Primavera/Otoño", "Tendencia": "2026 Textura"},
            "features|pl": {"Materiał": "Sztruks / Kord", "Styl": "Czapka z daszkiem", "Sezon": "Wiosna/Jesień", "Trend": "2026 Tekstura"},
        },
    },
]

# ── Amazon keyword data (for title validation) ────────────────────────────────

AMAZON_TITLE_KEYWORDS = {
    961: {
        "de": "Garment Washed Dad Hat | Vintage Basecap | Verstellbar Unisex | Frühling Sommer 2026",
        "fr": "Dad Hat Délavée Vintage | Casquette Baseball Réglable Unisex | Printemps Été 2026",
        "it": "Dad Hat Slavato Vintage | Berretto Baseball Regolabile Unisex | Primavera Estate 2026",
        "es": "Gorra Dad Hat Lavada Vintage | Béisbol Ajustable Unisex | Primavera Verano 2026",
        "nl": "Gewassen Dad Hat | Vintage Baseball Cap | Verstelbaar Unisex | Lente Zomer 2026",
        "sv": "Tvättad Dad Hat | Vintage Basebollkeps | Justerbar Unisex | Vår Sommar 2026",
        "pl": "Prany Dad Hat | Vintage Czapka z Daszkiem | Regulowana Unisex | Wiosna Lato 2026",
        "en": "Garment Washed Dad Hat | Vintage Baseball Cap | Adjustable Unisex | Spring Summer 2026",
    },
    547: {
        "de": "Bio-Baumwolle Bucket Hat | Organischer Fischerhut | GOTS Zertifiziert | Nachhaltig Unisex",
        "fr": "Bob Chapeau Coton Biologique | Éco-Responsable GOTS | Pêcheur Unisex | Printemps Été",
        "it": "Cappello Pescatore Cotone Biologico | GOTS Certificato | Eco-Friendly Unisex | Estate",
        "es": "Sombrero Bob Algodón Orgánico | Certificado GOTS | Eco-Friendly Unisex | Verano",
        "nl": "Organic Cotton Bucket Hat | GOTS Gecertificeerd | Duurzaam Unisex | Lente Zomer",
        "sv": "Ekologisk Bomull Bucket Hat | GOTS-Certifierad | Hållbar Unisex | Vår Sommar",
        "pl": "Czapka Bucket Hat Bawełna Organiczna | Certyfikat GOTS | Ekologiczna Unisex | Lato",
        "en": "Organic Cotton Bucket Hat | GOTS Certified | Eco-Friendly Adjustable Unisex | Summer",
    },
    379: {
        "de": "Bucket Hat Fischerhut Sommer | Festival Unisex Verstellbar | Y2K Trend Baumwolle | Damen Herren",
        "fr": "Bob Chapeau Été Festival | Casquette Pêcheur Unisex Réglable | Coton Tendance Y2K | Femme Homme",
        "it": "Cappello Pescatore Estate Festival | Unisex Regolabile | Cotone Tendenza Y2K | Donna Uomo",
        "es": "Sombrero Bob Verano Festival | Gorra Pescador Unisex Ajustable | Algodón Tendencia Y2K",
        "nl": "Bucket Hat Zomer Festival | Unisex Verstelbaar | Y2K Katoenen Vissershoed | Dames Heren",
        "sv": "Bucket Hat Sommar Festival | Unisex Justerbar Fiskarhatt | Y2K Bomull | Dam Herr",
        "pl": "Bucket Hat Lato Festiwal | Unisex Regulowana Czapka Wędkarska | Y2K Bawełna | Kobiety Mężczyźni",
        "en": "Bucket Hat Summer Festival | Unisex Adjustable Fisherman Hat | Y2K Cotton Cap | Women Men",
    },
    396: {
        "de": "Vintage Distressed Dad Hat | Used Look Basecap | Retro Unisex Verstellbar | Frühling 2026",
        "fr": "Casquette Dad Hat Vintage Délavée | Style Usé Rétro | Réglable Unisex | Printemps 2026",
        "it": "Dad Hat Vintage Consumato | Berretto Retrò Regolabile | Look Vissuto Unisex | Primavera 2026",
        "es": "Gorra Dad Hat Vintage Desgastada | Retro Ajustable Unisex | Estilo Usado | Primavera 2026",
        "nl": "Distressed Dad Hat | Vintage Retro Look | Verstelbaar Unisex Baseball Cap | Lente 2026",
        "sv": "Distressed Dad Hat | Vintage Retro Basebollkeps | Sliten Look | Justerbar Unisex 2026",
        "pl": "Vintage Distressed Dad Hat | Retro Czapka z Daszkiem | Used Look | Regulowana Unisex 2026",
        "en": "Distressed Dad Hat | Vintage Retro Baseball Cap | Used Look | Adjustable Unisex | Spring 2026",
    },
    532: {
        "de": "Cord Cap Corduroy Kappe | Kord Basecap Herren Damen | Verstellbar Unisex | Stickerei Frühling 2026",
        "fr": "Casquette Velours Côtelé | Corduroy Cap Brodée Unisex Réglable | Mode Printemps 2026",
        "it": "Cappellino Velluto a Coste | Corduroy Cap Ricamato Unisex Regolabile | Moda Primavera 2026",
        "es": "Gorra Pana Corduroy | Gorra Béisbol Bordada Unisex Ajustable | Moda Primavera 2026",
        "nl": "Corduroy Cap | Ribfluwelen Baseball Pet | Verstelbaar Unisex | Geborduurde Lente 2026",
        "sv": "Manchesterkeps Corduroy Cap | Justerbar Unisex | Broderad | Vårmode 2026",
        "pl": "Sztruksowa Czapka Corduroy | Regulowana Unisex | Haftowana | Moda Wiosna 2026",
        "en": "Corduroy Cap Baseball Hat | Adjustable Unisex | Premium Embroidered | Spring Fashion 2026",
    },
}

# Validate titles are ≤200 chars
def validate_titles():
    all_ok = True
    for pid, langs in AMAZON_TITLE_KEYWORDS.items():
        for lang, title in langs.items():
            if len(title) > 200:
                print(f"  WARNING: Title too long ({len(title)} chars) — PID {pid} / {lang}: {title[:60]}...")
                all_ok = False
            # Check no forbidden words
            forbidden = ["nesell", "otto cap", "yupoong", "beechfield", "capstone", "big accessories"]
            for fw in forbidden:
                if fw.lower() in title.lower():
                    print(f"  WARNING: Forbidden word '{fw}' in title PID {pid} / {lang}")
                    all_ok = False
    return all_ok

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("  CREATE PFT HATS — BASELINKER EU LISTINGS")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    # Validate titles
    print("\n[0] Validating Amazon titles (max 200 chars, no forbidden words)...")
    if validate_titles():
        print("  ✓ All titles valid")
    else:
        print("  ✗ Title validation failed — fix before proceeding")
        sys.exit(1)

    bl = BL(BL_TOKEN)
    results = {"created": [], "errors": []}

    for p in PRODUCTS:
        pid = p["printful_id"]
        code = p["internal_code"]
        parent_sku = f"PFT-{code}"

        print(f"\n{'─' * 60}")
        print(f"  Product: {code} (Printful ID {pid})")
        print(f"  Parent SKU: {parent_sku} | Price: €{p['price_eur']}")
        print(f"  Colors: {', '.join(c['code'] for c in p['colors'])}")

        # Prepare images (0-based keys, url: prefix)
        images = {}
        for i, img_url in enumerate(PRINTFUL_IMAGES.get(pid, [])):
            images[str(i)] = f"url:{img_url}"

        # Build parent product payload
        parent_payload = {
            "product_id": "0",  # create new
            "sku": parent_sku,
            "tax_rate": p["tax_rate"],
            "weight": p["weight"],
            "category_id": str(CATEGORY_ID),
            "average_cost": p["average_cost"],
            "prices": {str(PRICE_GROUP_ID): p["price_eur"]},
            "stock": {WAREHOUSE_ID: 999},
            "text_fields": p["text_fields"],
            "images": images,
        }

        try:
            result = bl.add_product(parent_payload)
            parent_id = result["product_id"]
            print(f"  ✓ Parent created: BL product_id={parent_id}")
            results["created"].append({"code": code, "printful_id": pid, "bl_product_id": parent_id, "sku": parent_sku})
        except Exception as e:
            print(f"  ✗ Parent creation failed: {e}")
            results["errors"].append({"code": code, "error": str(e)})
            continue

        # Create color variants
        for c in p["colors"]:
            variant_sku = f"PFT-{code}-{c['code']}"
            variant_tf = {
                "name": c["name_en"],
                "name|de": c["name_de"],
                "name|fr": c["name_fr"],
                "name|it": c["name_it"],
                "name|es": c["name_es"],
                "name|nl": c["name_nl"],
                "name|sv": c["name_se"],
                "name|pl": c["name_pl"],
                "features": {"Color": c["name_en"]},
                "features|de": {"Farbe": c["name_de"]},
                "features|fr": {"Couleur": c["name_fr"]},
                "features|it": {"Colore": c["name_it"]},
                "features|es": {"Color": c["name_es"]},
                "features|nl": {"Kleur": c["name_nl"]},
                "features|pl": {"Kolor": c["name_pl"]},
            }
            variant_payload = {
                "product_id": "0",
                "parent_id": str(parent_id),
                "sku": variant_sku,
                "prices": {str(PRICE_GROUP_ID): p["price_eur"]},
                "stock": {WAREHOUSE_ID: 999},
                "text_fields": variant_tf,
                "images": images,  # same images as parent initially
            }
            try:
                vr = bl.add_product(variant_payload)
                print(f"    ✓ Variant {variant_sku}: BL variant_id={vr['product_id']}")
            except Exception as e:
                print(f"    ✗ Variant {variant_sku} failed: {e}")
                results["errors"].append({"sku": variant_sku, "error": str(e)})

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'=' * 70}")
    print(f"  SUMMARY")
    print(f"{'=' * 70}")
    print(f"  Products created: {len(results['created'])}/5")
    print(f"  Errors:           {len(results['errors'])}")

    if results["created"]:
        print("\n  Created products:")
        for r in results["created"]:
            print(f"    {r['code']} (PFT ID {r['printful_id']}) → BL {r['bl_product_id']} / SKU {r['sku']}")

    if results["errors"]:
        print("\n  Errors:")
        for e in results["errors"]:
            print(f"    {e}")

    # Print Amazon keyword sheet
    print(f"\n{'=' * 70}")
    print("  AMAZON EU KEYWORD-RICH TITLES (max 200 chars, ready to use)")
    print(f"{'=' * 70}")
    market_lang = {"DE": "de", "FR": "fr", "IT": "it", "ES": "es", "NL": "nl", "SE": "sv", "PL": "pl", "BE": "fr"}
    product_names = {
        961: "Washed Dad Hat",
        547: "Organic Bucket Hat",
        379: "BX003 Bucket Hat",
        396: "Distressed Dad Hat",
        532: "Corduroy Cap",
    }
    for pid, langs in AMAZON_TITLE_KEYWORDS.items():
        print(f"\n  ── {product_names[pid]} (Printful {pid}) ──")
        for market in ["DE", "FR", "IT", "ES", "NL", "SE", "PL", "BE"]:
            lang = market_lang[market]
            title = langs.get(lang, langs["en"])
            chars = len(title)
            print(f"    {market} ({chars:3d} chars): {title}")

    # Save results
    out_path = Path(__file__).parent / "create_pft_hats_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  Results saved to: {out_path}")

    return results


if __name__ == "__main__":
    main()
