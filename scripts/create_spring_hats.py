"""Create Spring 2026 Hat Listings on Amazon EU (8 markets).

5 new products:
  PFT-S26-WASHED    — Garment Washed Dad Hat (Printful catalog ID 961)
  PFT-S26-ORGBUCKET — Capstone Organic Bucket Hat (catalog ID 547)
  PFT-S26-BUCKET    — BX003 Standard Bucket Hat (catalog ID 379)
  PFT-S26-DIST      — Distressed Dad Hat (catalog ID 396)
  PFT-S26-CORD      — Corduroy Cap (catalog ID 532)

Usage:
    cd ~/nesell-analytics
    python3.11 scripts/create_spring_hats.py --dry-run
    python3.11 scripts/create_spring_hats.py --product washed --market DE
    python3.11 scripts/create_spring_hats.py --product all
"""
import argparse
import json
import time
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etl.amazon_listings import (
    MARKETPLACE_IDS, LANG_TAGS, CURRENCIES, SIZE_SYSTEMS,
    SELLER_ID, put_listing, check_listing_exists, api_put
)

# ── Price tables ────────────────────────────────────────────────────────────

PRICES = {
    "washed": {
        "DE": 22.99, "FR": 22.99, "IT": 22.99, "ES": 22.99,
        "NL": 22.99, "BE": 22.99, "PL": 99.99, "SE": 249.00,
    },
    "orgbucket": {
        "DE": 34.99, "FR": 34.99, "IT": 34.99, "ES": 34.99,
        "NL": 34.99, "BE": 34.99, "PL": 149.99, "SE": 379.00,
    },
    "bucket": {
        "DE": 26.99, "FR": 26.99, "IT": 26.99, "ES": 26.99,
        "NL": 26.99, "BE": 26.99, "PL": 114.99, "SE": 289.00,
    },
    "dist": {
        "DE": 24.99, "FR": 24.99, "IT": 24.99, "ES": 24.99,
        "NL": 24.99, "BE": 24.99, "PL": 104.99, "SE": 269.00,
    },
    "cord": {
        "DE": 26.99, "FR": 26.99, "IT": 26.99, "ES": 26.99,
        "NL": 26.99, "BE": 26.99, "PL": 114.99, "SE": 289.00,
    },
}

# ── Color definitions per product ───────────────────────────────────────────

WASHED_COLORS = {
    "KHAKI": {
        "DE": "Khaki", "FR": "Kaki", "IT": "Cachi", "ES": "Caqui",
        "NL": "Khaki", "PL": "Khaki", "SE": "Khaki", "BE": "Khaki",
    },
    "STONE": {
        "DE": "Steingrau", "FR": "Pierre", "IT": "Pietra", "ES": "Piedra",
        "NL": "Steengrijs", "PL": "Kamienny", "SE": "Stengrå", "BE": "Steengrijs",
    },
    "SAGE": {
        "DE": "Salbeigrün", "FR": "Sauge", "IT": "Salvia", "ES": "Salvia",
        "NL": "Saliegroen", "PL": "Szałwiowy", "SE": "Salvia", "BE": "Saliegroen",
    },
    "NAVY": {
        "DE": "Marineblau", "FR": "Bleu Marine", "IT": "Blu Marina", "ES": "Azul Marino",
        "NL": "Marineblauw", "PL": "Granatowy", "SE": "Marinblå", "BE": "Marineblauw",
    },
}

ORGBUCKET_COLORS = {
    "BLACK": {
        "DE": "Schwarz", "FR": "Noir", "IT": "Nero", "ES": "Negro",
        "NL": "Zwart", "PL": "Czarny", "SE": "Svart", "BE": "Zwart",
    },
    "NATURAL": {
        "DE": "Naturweiß", "FR": "Naturel", "IT": "Naturale", "ES": "Natural",
        "NL": "Naturel", "PL": "Naturalny", "SE": "Natur", "BE": "Naturel",
    },
    "NAVY": {
        "DE": "Marineblau", "FR": "Bleu Marine", "IT": "Blu Marina", "ES": "Azul Marino",
        "NL": "Marineblauw", "PL": "Granatowy", "SE": "Marinblå", "BE": "Marineblauw",
    },
}

BUCKET_COLORS = {
    "BLACK": {
        "DE": "Schwarz", "FR": "Noir", "IT": "Nero", "ES": "Negro",
        "NL": "Zwart", "PL": "Czarny", "SE": "Svart", "BE": "Zwart",
    },
    "NAVY": {
        "DE": "Marineblau", "FR": "Bleu Marine", "IT": "Blu Marina", "ES": "Azul Marino",
        "NL": "Marineblauw", "PL": "Granatowy", "SE": "Marinblå", "BE": "Marineblauw",
    },
    "WHITE": {
        "DE": "Weiss", "FR": "Blanc", "IT": "Bianco", "ES": "Blanco",
        "NL": "Wit", "PL": "Biały", "SE": "Vit", "BE": "Wit",
    },
}

DIST_COLORS = {
    "BLACK": {
        "DE": "Schwarz", "FR": "Noir", "IT": "Nero", "ES": "Negro",
        "NL": "Zwart", "PL": "Czarny", "SE": "Svart", "BE": "Zwart",
    },
    "CHARCOAL": {
        "DE": "Anthrazit", "FR": "Anthracite", "IT": "Antracite", "ES": "Antracita",
        "NL": "Antraciet", "PL": "Antracytowy", "SE": "Antracit", "BE": "Antraciet",
    },
    "KHAKI": {
        "DE": "Khaki", "FR": "Kaki", "IT": "Cachi", "ES": "Caqui",
        "NL": "Khaki", "PL": "Khaki", "SE": "Khaki", "BE": "Khaki",
    },
    "NAVY": {
        "DE": "Marineblau", "FR": "Bleu Marine", "IT": "Blu Marina", "ES": "Azul Marino",
        "NL": "Marineblauw", "PL": "Granatowy", "SE": "Marinblå", "BE": "Marineblauw",
    },
}

CORD_COLORS = {
    "BLACK": {
        "DE": "Schwarz", "FR": "Noir", "IT": "Nero", "ES": "Negro",
        "NL": "Zwart", "PL": "Czarny", "SE": "Svart", "BE": "Zwart",
    },
    "CAMEL": {
        "DE": "Kamel", "FR": "Camel", "IT": "Cammello", "ES": "Camello",
        "NL": "Camel", "PL": "Camelowy", "SE": "Kamel", "BE": "Camel",
    },
    "OLIVE": {
        "DE": "Dunkeloliv", "FR": "Kaki Foncé", "IT": "Oliva Scuro", "ES": "Oliva Oscuro",
        "NL": "Donker Olijf", "PL": "Ciemna Oliwka", "SE": "Mörk Oliv", "BE": "Donker Olijf",
    },
    "NAVYD": {
        "DE": "Dunkelmarineblau", "FR": "Bleu Marine Foncé", "IT": "Blu Marina Scuro", "ES": "Azul Marino Oscuro",
        "NL": "Donker Marineblauw", "PL": "Ciemny Granat", "SE": "Mörkmarinblå", "BE": "Donker Marineblauw",
    },
}

PRODUCT_COLORS = {
    "washed": WASHED_COLORS,
    "orgbucket": ORGBUCKET_COLORS,
    "bucket": BUCKET_COLORS,
    "dist": DIST_COLORS,
    "cord": CORD_COLORS,
}

PARENT_SKUS = {
    "washed": "PFT-S26-WASHED",
    "orgbucket": "PFT-S26-ORGBUCKET",
    "bucket": "PFT-S26-BUCKET",
    "dist": "PFT-S26-DIST",
    "cord": "PFT-S26-CORD",
}

# ── Localized content per product per market ────────────────────────────────

CONTENT = {
    "washed": {
        "hat_form_type": "baseball_cap",
        "material_en": "Cotton",
        "fabric_type_en": "100% Cotton",
        "DE": {
            "item_name": "Vintage Basecap gewaschen bestickt | Garment Washed Dad Hat Baumwolle | Unisex verstellbar Frühling Sommer",
            "keywords": "vintage basecap gewaschen garment washed dad hat bestickt baumwolle unisex verstellbar frühling sommer baseball cap kappe",
            "bullets": [
                "GARMENT-WASHED VINTAGE-LOOK — Einzigartiger Washed-Effekt für authentisch verblasstes Retro-Finish. Der angesagteste Cap-Style Frühjahr 2026",
                "PREMIUM MASCHINENSTICKEREI — Hochwertiges Embroidery das nicht verblasst, nicht abblättert und nicht reißt wie Druckverfahren",
                "100% WEICHE BAUMWOLLE — Chino Twill, niedriges Profil, vorgebogener Schirm. Angenehm leicht und atmungsaktiv für täglich Tragen",
                "VERSTELLBARER METALLVERSCHLUSS — Passt jedem Kopfumfang. Einheitsgröße für Damen und Herren unisex",
                "VIELSEITIG EINSETZBAR — Ideal für Freizeit, Reisen, Festivals, Outdoor und den täglichen Gebrauch bei jedem Wetter",
            ],
            "description": "Vintage Garment Washed Dad Hat mit hochwertiger Maschinenstickerei. Der Washed-Effekt verleiht jeder Cap einen einzigartigen, authentisch verblassten Retro-Look. 100% Baumwolle Chino Twill, verstellbarer Metallverschluss, eine Größe für alle. Ideal als Geschenk oder Alltagsbegleiter.",
            "pattern": "Einfarbig",
            "age": "Erwachsener",
            "care": "Handwäsche",
            "fabric": "100% Baumwolle",
            "material": "Baumwolle",
            "style": "Vintage",
            "unit_type": "Stück",
        },
        "FR": {
            "item_name": "Casquette Dad Hat Délavée Vintage Brodée | Réglable Coton Unisexe | Baseball Cap Printemps Été",
            "keywords": "casquette dad hat délavée vintage brodée coton unisexe réglable baseball cap printemps été garment washed",
            "bullets": [
                "EFFET DÉLAVÉ VINTAGE — Teinte garment-washed unique pour un look rétro authentiquement vieilli. La tendance capeline printemps 2026",
                "BRODERIE MACHINE PREMIUM — Broderie de qualité qui ne décolore pas, ne s'écaille pas et ne se déchire pas comme les impressions",
                "100% COTON DOUX — Twill chino, faible profil, visière pré-courbée. Léger et respirant pour un port quotidien confortable",
                "FERMETURE RÉGLABLE EN MÉTAL — S'adapte à toutes les tailles de tête. Taille unique pour femmes et hommes",
                "POLYVALENTE — Idéale pour les loisirs, voyages, festivals, outdoor et le quotidien",
            ],
            "description": "Casquette Dad Hat délavée vintage avec broderie machine premium. L'effet garment-washed confère un look rétro authentiquement vieilli. 100% coton twill chino, fermeture réglable en métal, taille unique.",
            "pattern": "Uni",
            "age": "Adulte",
            "care": "Lavage à la main",
            "fabric": "100% Coton",
            "material": "Coton",
            "style": "Vintage",
            "unit_type": "pièce",
        },
        "IT": {
            "item_name": "Cappellino Dad Hat Lavato Vintage Ricamato | Regolabile Cotone Unisex | Berretto Baseball Primavera",
            "keywords": "cappellino dad hat lavato vintage ricamato cotone unisex regolabile berretto baseball primavera estate garment washed",
            "bullets": [
                "EFFETTO LAVATO VINTAGE — Tintura garment-washed unica per un look retrò autenticamente invecchiato. La tendenza cappelli primavera 2026",
                "RICAMO A MACCHINA PREMIUM — Ricamo di qualità che non sbiadisce, non si stacca e non si strappa come le stampe",
                "100% COTONE MORBIDO — Twill chino, profilo basso, visiera pre-curvata. Leggero e traspirante per uso quotidiano",
                "CHIUSURA REGOLABILE IN METALLO — Si adatta a tutte le misure di testa. Taglia unica per donna e uomo",
                "VERSATILE — Ideale per tempo libero, viaggi, festival, outdoor e uso quotidiano",
            ],
            "description": "Cappellino Dad Hat lavato vintage con ricamo a macchina premium. L'effetto garment-washed conferisce un look retrò autenticamente invecchiato. 100% cotone twill chino, chiusura regolabile in metallo.",
            "pattern": "Tinta Unita",
            "age": "Adulto",
            "care": "Lavaggio a mano",
            "fabric": "100% Cotone",
            "material": "Cotone",
            "style": "Vintage",
            "unit_type": "pezzo",
        },
        "ES": {
            "item_name": "Gorra Dad Hat Desgastada Vintage Bordada | Ajustable Algodón Unisex | Béisbol Primavera Verano",
            "keywords": "gorra dad hat desgastada vintage bordada algodón unisex ajustable béisbol primavera verano garment washed",
            "bullets": [
                "EFECTO DESGASTADO VINTAGE — Tinte garment-washed único para un look retro auténticamente envejecido. La tendencia gorras primavera 2026",
                "BORDADO A MÁQUINA PREMIUM — Bordado de calidad que no destiñe, no se descascarilla y no se rompe como las impresiones",
                "100% ALGODÓN SUAVE — Twill chino, perfil bajo, visera pre-curvada. Ligera y transpirable para uso diario cómodo",
                "CIERRE AJUSTABLE EN METAL — Se adapta a todos los tamaños de cabeza. Talla única para mujer y hombre",
                "VERSÁTIL — Ideal para ocio, viajes, festivales, outdoor y uso diario",
            ],
            "description": "Gorra Dad Hat desgastada vintage con bordado a máquina premium. El efecto garment-washed otorga un look retro auténticamente envejecido. 100% algodón twill chino, cierre ajustable en metal.",
            "pattern": "Liso",
            "age": "Adulto",
            "care": "Lavado a mano",
            "fabric": "100% Algodón",
            "material": "Algodón",
            "style": "Vintage",
            "unit_type": "pieza",
        },
        "NL": {
            "item_name": "Vintage Washed Baseball Cap Geborduurd | Verstelbare Dad Hat Katoen Unisex | Lente Zomer",
            "keywords": "vintage washed baseball cap geborduurd verstelbare dad hat katoen unisex lente zomer garment washed",
            "bullets": [
                "GARMENT-WASHED VINTAGE-LOOK — Uniek gewassen effect voor authentiek vervaagde retro-look. De trendiest cap-stijl lente 2026",
                "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert en niet scheurt zoals prints",
                "100% ZACHTE KATOEN — Chino twill, laag profiel, voorgebogen klep. Licht en ademend voor dagelijks dragen",
                "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size voor dames en heren",
                "VEELZIJDIG — Ideaal voor vrije tijd, reizen, festivals, outdoor en dagelijks gebruik",
            ],
            "description": "Vintage garment-washed dad hat met premium machineborduurwerk. Het gewassen effect geeft elke cap een uniek, authentiek vervaagd retro-uiterlijk. 100% katoenen chino twill, verstelbare metalen sluiting.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Katoen",
            "material": "Katoen",
            "style": "Vintage",
            "unit_type": "stuk",
        },
        "PL": {
            "item_name": "Vintage Washed Baseball Cap Haftowany | Regulowany Dad Hat Bawełna Unisex | Wiosna Lato",
            "keywords": "vintage washed baseball cap haftowany regulowany dad hat bawełna unisex wiosna lato garment washed",
            "bullets": [
                "EFEKT VINTAGE WASHED — Unikalne płukanie garment-washed dla autentycznego wyblakłego retro wyglądu. Najmodniejszy styl czapek wiosna 2026",
                "PREMIUM HAFT MASZYNOWY — Wysokiej jakości haft, który nie blaknie, nie odpryskuje i nie rwie się jak nadruki",
                "100% MIĘKKA BAWEŁNA — Twill chino, niski profil, zakrzywiony daszek. Lekka i oddychająca do codziennego noszenia",
                "REGULOWANE ZAPIĘCIE METALOWE — Dopasowuje się do każdego obwodu głowy. Jeden rozmiar dla kobiet i mężczyzn",
                "WSZECHSTRONNA — Idealna na co dzień, podróże, festiwale, outdoor i wszelkie okazje",
            ],
            "description": "Vintage garment washed dad hat z premium haftem maszynowym. Efekt prania nadaje czapce unikalny, autentycznie wyblakły retro wygląd. 100% bawełna twill chino, regulowane metalowe zapięcie.",
            "pattern": "Jednobarwny",
            "age": "Dorosły",
            "care": "Pranie ręczne",
            "fabric": "100% Bawełna",
            "material": "Bawełna",
            "style": "Vintage",
            "unit_type": "sztuka",
        },
        "SE": {
            "item_name": "Vintage Tvättad Baseball Keps Broderad | Justerbar Dad Hat Bomull Unisex | Vår Sommar",
            "keywords": "vintage tvättad baseball keps broderad justerbar dad hat bomull unisex vår sommar garment washed",
            "bullets": [
                "GARMENT-WASHED VINTAGE-LOOK — Unikt tvättat utseende för autentiskt urblekt retro-finish. Årets trendtrendiga keps vår 2026",
                "PREMIUM MASKINBRODERI — Kvalitetsbroderi som inte bleknar, flagnar eller rivs som tryck",
                "100% MJUK BOMULL — Chino twill, låg profil, förböjd skärm. Lätt och andningsbar för dagligt bruk",
                "JUSTERBAR METALLSPÄNNE — Passar alla huvudstorlekar. One size för dam och herr",
                "MÅNGSIDIG — Perfekt för fritid, resor, festivaler, outdoor och dagligt bruk",
            ],
            "description": "Vintage garment-washed dad hat med premium maskinbroderi. Det tvättade utseendet ger kepsen ett unikt, autentiskt urblekt retro-utseende. 100% bomull chino twill, justerbar metallspänne.",
            "pattern": "Enfärgad",
            "age": "Vuxen",
            "care": "Handtvätt",
            "fabric": "100% Bomull",
            "material": "Bomull",
            "style": "Vintage",
            "unit_type": "styck",
        },
        "BE": {
            "item_name": "Vintage Washed Baseball Cap Geborduurd | Verstelbare Dad Hat Katoen Unisex | Lente",
            "keywords": "vintage washed baseball cap geborduurd verstelbare dad hat katoen unisex lente zomer garment washed",
            "bullets": [
                "GARMENT-WASHED VINTAGE-LOOK — Uniek gewassen effect voor authentiek vervaagde retro-look. De trendiest cap-stijl lente 2026",
                "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert en niet scheurt zoals prints",
                "100% ZACHTE KATOEN — Chino twill, laag profiel, voorgebogen klep. Licht en ademend voor dagelijks dragen",
                "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size voor dames en heren",
                "VEELZIJDIG — Ideaal voor vrije tijd, reizen, festivals, outdoor en dagelijks gebruik",
            ],
            "description": "Vintage garment-washed dad hat met premium machineborduurwerk. Het gewassen effect geeft een uniek, authentiek vervaagd retro-uiterlijk. 100% katoenen chino twill, verstelbare metalen sluiting.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Katoen",
            "material": "Katoen",
            "style": "Vintage",
            "unit_type": "stuk",
        },
    },

    "orgbucket": {
        "hat_form_type": "bucket_hat",
        "material_en": "Organic Cotton",
        "fabric_type_en": "100% Organic Cotton",
        "DE": {
            "item_name": "Bio-Baumwolle Bucket Hat bestickt nachhaltig | GOTS Organic Fischerhut Unisex | Sommer Outdoor",
            "keywords": "bio baumwolle bucket hat bestickt nachhaltig organic cotton fischerhut unisex sommer outdoor gots zertifiziert",
            "bullets": [
                "ZERTIFIZIERTE BIO-BAUMWOLLE — GOTS-zertifiziertes organisches Baumwollgewebe ohne schädliche Chemikalien. Gut für dich und die Umwelt",
                "PREMIUM MASCHINENSTICKEREI — Hochwertiges Embroidery das nicht verblasst, nicht abblättert. Kein billiger Druck",
                "BREITE KREMPE — Klassischer Bucket-Hat-Stil mit allseitigem UV-Schutz. Perfekt für Strand, Festival und Outdoor",
                "VERSTELLBAR — Kinnband für sichere Passform bei Wind. Einheitsgröße für Damen und Herren",
                "NACHHALTIG — Organisch angebaute Baumwolle, umweltfreundliche Produktion, OEKO-TEX geprüft",
            ],
            "description": "Bucket Hat aus zertifizierter Bio-Baumwolle mit hochwertiger Maschinenstickerei. GOTS-zertifiziert, umweltfreundlich produziert. Breite Krempe für optimalen UV-Schutz. Perfekt für Strand, Outdoor und nachhaltigen Lifestyle.",
            "pattern": "Einfarbig",
            "age": "Erwachsener",
            "care": "Handwäsche",
            "fabric": "100% Bio-Baumwolle",
            "material": "Bio-Baumwolle",
            "style": "Casual",
            "unit_type": "Stück",
        },
        "FR": {
            "item_name": "Bob Coton Bio Brodé Durable | Chapeau Bob Organic Unisexe | Fischerhut Été Plage Festival",
            "keywords": "bob coton bio brodé durable organic unisexe été plage festival bucket hat eco-responsable gots",
            "bullets": [
                "COTON BIO CERTIFIÉ GOTS — Tissu coton biologique certifié sans produits chimiques nocifs. Bon pour vous et l'environnement",
                "BRODERIE MACHINE PREMIUM — Broderie de qualité qui ne décolore pas, ne s'écaille pas. Pas d'impression bon marché",
                "BORD LARGE — Style bob classique avec protection UV tout autour. Parfait pour la plage, festival et outdoor",
                "RÉGLABLE — Cordon menton pour maintien sécurisé par vent. Taille unique pour femmes et hommes",
                "ÉCO-RESPONSABLE — Coton bio, production respectueuse de l'environnement, certifié OEKO-TEX",
            ],
            "description": "Bob en coton bio certifié GOTS avec broderie machine premium. Écologique, durable. Large bord pour une protection UV optimale. Parfait pour plage, outdoor et mode de vie durable.",
            "pattern": "Uni",
            "age": "Adulte",
            "care": "Lavage à la main",
            "fabric": "100% Coton Bio",
            "material": "Coton Bio",
            "style": "Casual",
            "unit_type": "pièce",
        },
        "IT": {
            "item_name": "Cappello Pescatore Cotone Bio Ricamato Sostenibile | Bob Organic Unisex | Estate Spiaggia",
            "keywords": "cappello pescatore cotone bio ricamato sostenibile organic unisex estate spiaggia bucket hat gots",
            "bullets": [
                "COTONE BIO CERTIFICATO GOTS — Tessuto in cotone biologico certificato senza sostanze chimiche nocive. Buono per te e l'ambiente",
                "RICAMO A MACCHINA PREMIUM — Ricamo di qualità che non sbiadisce, non si stacca. Nessuna stampa economica",
                "TESA LARGA — Stile bob classico con protezione UV su tutti i lati. Perfetto per spiaggia, festival e outdoor",
                "REGOLABILE — Cinturino sottomento per una vestibilità sicura nel vento. Taglia unica per donna e uomo",
                "SOSTENIBILE — Cotone coltivato organicamente, produzione eco-friendly, certificato OEKO-TEX",
            ],
            "description": "Cappello pescatore in cotone bio certificato GOTS con ricamo a macchina premium. Ecologico e sostenibile. Tesa larga per ottima protezione UV. Perfetto per spiaggia, outdoor e stile di vita sostenibile.",
            "pattern": "Tinta Unita",
            "age": "Adulto",
            "care": "Lavaggio a mano",
            "fabric": "100% Cotone Bio",
            "material": "Cotone Biologico",
            "style": "Casual",
            "unit_type": "pezzo",
        },
        "ES": {
            "item_name": "Sombrero Bob Algodón Orgánico Bordado Sostenible | Bucket Hat Eco Unisex | Verano Playa",
            "keywords": "sombrero bob algodón orgánico bordado sostenible bucket hat eco unisex verano playa gots",
            "bullets": [
                "ALGODÓN ORGÁNICO CERTIFICADO GOTS — Tejido de algodón biológico certificado sin productos químicos nocivos. Bueno para ti y el medio ambiente",
                "BORDADO A MÁQUINA PREMIUM — Bordado de calidad que no destiñe, no se descascarilla. Sin impresión barata",
                "ALA ANCHA — Estilo bob clásico con protección UV en todos los lados. Perfecto para playa, festival y outdoor",
                "AJUSTABLE — Correa barbilla para un ajuste seguro con viento. Talla única para mujer y hombre",
                "SOSTENIBLE — Algodón cultivado orgánicamente, producción ecológica, certificado OEKO-TEX",
            ],
            "description": "Sombrero bucket hat de algodón orgánico certificado GOTS con bordado a máquina premium. Ecológico y sostenible. Ala ancha para óptima protección UV. Perfecto para playa, outdoor y estilo de vida sostenible.",
            "pattern": "Liso",
            "age": "Adulto",
            "care": "Lavado a mano",
            "fabric": "100% Algodón Orgánico",
            "material": "Algodón Orgánico",
            "style": "Casual",
            "unit_type": "pieza",
        },
        "NL": {
            "item_name": "Biologisch Katoen Bucket Hat Geborduurd Duurzaam | Organic Vissershoed Unisex | Zomer Strand",
            "keywords": "biologisch katoen bucket hat geborduurd duurzaam organic vissershoed unisex zomer strand gots",
            "bullets": [
                "GECERTIFICEERD BIOLOGISCH KATOEN GOTS — Biologisch katoenen stof zonder schadelijke chemicaliën. Goed voor jou en het milieu",
                "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert. Geen goedkope print",
                "BREDE RAND — Klassieke bucket hat stijl met UV-bescherming rondom. Perfect voor strand, festival en outdoor",
                "VERSTELBAAR — Kinriem voor veilige pasvorm bij wind. One size voor dames en heren",
                "DUURZAAM — Biologisch geteeld katoen, milieuvriendelijke productie, OEKO-TEX gecertificeerd",
            ],
            "description": "Bucket hat van gecertificeerd biologisch GOTS-katoen met premium machineborduurwerk. Ecologisch en duurzaam. Brede rand voor optimale UV-bescherming. Perfect voor strand, outdoor en duurzame levensstijl.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Biologisch Katoen",
            "material": "Biologisch Katoen",
            "style": "Casual",
            "unit_type": "stuk",
        },
        "PL": {
            "item_name": "Bawełna Organiczna Bucket Hat Haftowany Ekologiczny | Kapelusz Wędkarski Unisex | Lato Plaża",
            "keywords": "bawełna organiczna bucket hat haftowany ekologiczny kapelusz wędkarski unisex lato plaża gots",
            "bullets": [
                "CERTYFIKOWANA BAWEŁNA ORGANICZNA GOTS — Certyfikowana organiczna tkanina bawełniana bez szkodliwych chemikaliów. Dobra dla ciebie i środowiska",
                "PREMIUM HAFT MASZYNOWY — Wysokiej jakości haft, który nie blaknie, nie odpryskuje. Bez taniego nadruku",
                "SZEROKIE RONDO — Klasyczny styl bucket hat z ochroną UV ze wszystkich stron. Idealne na plażę, festiwal i outdoor",
                "REGULOWANY — Pasek podbródkowy dla bezpiecznego dopasowania przy wietrze. Jeden rozmiar dla kobiet i mężczyzn",
                "EKOLOGICZNY — Organicznie uprawiana bawełna, przyjazna środowisku produkcja, certyfikat OEKO-TEX",
            ],
            "description": "Bucket hat z certyfikowanej organicznej bawełny GOTS z premium haftem maszynowym. Ekologiczny i zrównoważony. Szerokie rondo dla optymalnej ochrony UV. Idealny na plażę, outdoor i ekologiczny tryb życia.",
            "pattern": "Jednobarwny",
            "age": "Dorosły",
            "care": "Pranie ręczne",
            "fabric": "100% Bawełna Organiczna",
            "material": "Bawełna Organiczna",
            "style": "Casual",
            "unit_type": "sztuka",
        },
        "SE": {
            "item_name": "Ekologisk Bomull Bucket Hat Broderad Hållbar | Organic Fiskarhatt Unisex | Sommar Strand",
            "keywords": "ekologisk bomull bucket hat broderad hållbar organic fiskarhatt unisex sommar strand gots",
            "bullets": [
                "GOTS-CERTIFIERAD EKOLOGISK BOMULL — Certifierat ekologiskt bomullstyg utan skadliga kemikalier. Bra för dig och miljön",
                "PREMIUM MASKINBRODERI — Kvalitetsbroderi som inte bleknar, flagnar. Ingen billig tryckning",
                "BRED BRÄTTE — Klassisk bucket hat-stil med UV-skydd runt om. Perfekt för strand, festival och outdoor",
                "JUSTERBAR — Hakrem för säker passform vid blåst. One size för dam och herr",
                "HÅLLBAR — Ekologiskt odlad bomull, miljövänlig produktion, OEKO-TEX-certifierad",
            ],
            "description": "Bucket hat av GOTS-certifierad ekologisk bomull med premium maskinbroderi. Ekologisk och hållbar. Brett brätte för optimal UV-skydd. Perfekt för strand, outdoor och hållbar livsstil.",
            "pattern": "Enfärgad",
            "age": "Vuxen",
            "care": "Handtvätt",
            "fabric": "100% Ekologisk Bomull",
            "material": "Ekologisk Bomull",
            "style": "Casual",
            "unit_type": "styck",
        },
        "BE": {
            "item_name": "Biologisch Katoen Bucket Hat Geborduurd Duurzaam | Organic Vissershoed Unisex | Zomer",
            "keywords": "biologisch katoen bucket hat geborduurd duurzaam organic vissershoed unisex zomer strand gots",
            "bullets": [
                "GECERTIFICEERD BIOLOGISCH KATOEN GOTS — Biologisch katoenen stof zonder schadelijke chemicaliën. Goed voor jou en het milieu",
                "PREMIUM MACHINEBORDUURWERK — Kwaliteitsborduurwerk dat niet vervaagt, niet afschilfert. Geen goedkope print",
                "BREDE RAND — Klassieke bucket hat stijl met UV-bescherming rondom. Perfect voor strand, festival en outdoor",
                "VERSTELBAAR — Kinriem voor veilige pasvorm bij wind. One size voor dames en heren",
                "DUURZAAM — Biologisch geteeld katoen, milieuvriendelijke productie, OEKO-TEX gecertificeerd",
            ],
            "description": "Bucket hat van gecertificeerd biologisch GOTS-katoen met premium machineborduurwerk. Ecologisch en duurzaam. Brede rand voor optimale UV-bescherming.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Biologisch Katoen",
            "material": "Biologisch Katoen",
            "style": "Casual",
            "unit_type": "stuk",
        },
    },

    "bucket": {
        "hat_form_type": "bucket_hat",
        "material_en": "Cotton",
        "fabric_type_en": "100% Cotton",
        "DE": {
            "item_name": "Bucket Hat Fischerhut bestickt Baumwolle | Unisex Festival Sommer Outdoor verstellbar | Y2K",
            "keywords": "bucket hat fischerhut bestickt baumwolle unisex festival sommer outdoor verstellbar y2k strand sport",
            "bullets": [
                "TREND BUCKET HAT — Der angesagteste Hut-Style Sommer 2026. Y2K Revival trifft auf zeitloses Design mit hochwertiger Stickerei",
                "PREMIUM MASCHINENSTICKEREI — Professionelles Embroidery, das nicht verblasst, nicht abblättert und nicht reißt wie Drucke",
                "100% WEICHE BAUMWOLLE — Breite Krempe für optimalen UV-Schutz. Atmungsaktiv und leicht für täglich Tragen",
                "VERSTELLBARER KINNVERSCHLUSS — Perfekte Passform bei Wind. Einheitsgröße für Damen und Herren unisex",
                "VIELSEITIG — Ideal für Festival, Strand, Outdoor, Sport und den täglichen Gebrauch im Frühling und Sommer",
            ],
            "description": "Hochwertiger Bucket Hat aus 100% Baumwolle mit Maschinenstickerei. Breite Krempe für UV-Schutz. Der Trend-Style für Festival, Strand und Outdoor. Unisex, eine Größe, verstellbarer Kinnverschluss.",
            "pattern": "Einfarbig",
            "age": "Erwachsener",
            "care": "Handwäsche",
            "fabric": "100% Baumwolle",
            "material": "Baumwolle",
            "style": "Casual",
            "unit_type": "Stück",
        },
        "FR": {
            "item_name": "Bob Chapeau Été Brodé Coton | Festival Plage Unisexe Tendance | Bucket Hat Y2K Réglable",
            "keywords": "bob chapeau été brodé coton festival plage unisexe tendance bucket hat y2k réglable sport outdoor",
            "bullets": [
                "TENDANCE BOB ÉTÉ — Le style chapeau le plus tendance été 2026. Revival Y2K avec broderie de qualité",
                "BRODERIE MACHINE PREMIUM — Broderie professionnelle qui ne décolore pas, ne s'écaille pas et ne se déchire pas",
                "100% COTON DOUX — Large bord pour protection UV optimale. Léger et respirant pour port quotidien",
                "CORDON MENTON RÉGLABLE — Maintien parfait par vent. Taille unique pour femmes et hommes",
                "POLYVALENT — Parfait pour festival, plage, outdoor, sport et quotidien au printemps et été",
            ],
            "description": "Bob de qualité en 100% coton avec broderie machine premium. Large bord pour protection UV. Le style tendance pour festival, plage et outdoor. Unisexe, taille unique, cordon réglable.",
            "pattern": "Uni",
            "age": "Adulte",
            "care": "Lavage à la main",
            "fabric": "100% Coton",
            "material": "Coton",
            "style": "Casual",
            "unit_type": "pièce",
        },
        "IT": {
            "item_name": "Cappello Pescatore Ricamato Cotone | Festival Spiaggia Unisex Tendenza | Bucket Hat Y2K",
            "keywords": "cappello pescatore ricamato cotone festival spiaggia unisex tendenza bucket hat y2k regolabile sport",
            "bullets": [
                "TENDENZA BUCKET HAT — Lo stile cappello più trendy estate 2026. Revival Y2K con ricamo di qualità",
                "RICAMO A MACCHINA PREMIUM — Ricamo professionale che non sbiadisce, non si stacca e non si strappa",
                "100% COTONE MORBIDO — Tesa larga per protezione UV ottimale. Leggero e traspirante per uso quotidiano",
                "CORDICELLA MENTO REGOLABILE — Tenuta perfetta con vento. Taglia unica per donna e uomo",
                "VERSATILE — Perfetto per festival, spiaggia, outdoor, sport e quotidiano in primavera ed estate",
            ],
            "description": "Cappello pescatore di qualità in 100% cotone con ricamo a macchina premium. Tesa larga per protezione UV. Lo stile tendenza per festival, spiaggia e outdoor. Unisex, taglia unica.",
            "pattern": "Tinta Unita",
            "age": "Adulto",
            "care": "Lavaggio a mano",
            "fabric": "100% Cotone",
            "material": "Cotone",
            "style": "Casual",
            "unit_type": "pezzo",
        },
        "ES": {
            "item_name": "Sombrero Pescador Bordado Algodón | Festival Playa Unisex Tendencia | Bucket Hat Y2K",
            "keywords": "sombrero pescador bordado algodón festival playa unisex tendencia bucket hat y2k ajustable sport",
            "bullets": [
                "TENDENCIA BUCKET HAT — El estilo sombrero más trendy verano 2026. Revival Y2K con bordado de calidad",
                "BORDADO A MÁQUINA PREMIUM — Bordado profesional que no destiñe, no se descascarilla y no se rompe",
                "100% ALGODÓN SUAVE — Ala ancha para protección UV óptima. Ligero y transpirable para uso diario",
                "CORREA BARBILLA AJUSTABLE — Sujeción perfecta con viento. Talla única para mujer y hombre",
                "VERSÁTIL — Perfecto para festival, playa, outdoor, deporte y uso diario en primavera y verano",
            ],
            "description": "Sombrero bucket hat de calidad en 100% algodón con bordado a máquina premium. Ala ancha para protección UV. El estilo tendencia para festival, playa y outdoor. Unisex, talla única.",
            "pattern": "Liso",
            "age": "Adulto",
            "care": "Lavado a mano",
            "fabric": "100% Algodón",
            "material": "Algodón",
            "style": "Casual",
            "unit_type": "pieza",
        },
        "NL": {
            "item_name": "Vissershoed Bucket Hat Geborduurd Katoen | Festival Strand Unisex Trend | Y2K Verstelbaar",
            "keywords": "vissershoed bucket hat geborduurd katoen festival strand unisex trend y2k verstelbaar sport outdoor",
            "bullets": [
                "TREND BUCKET HAT — De meest trendy hoedenstijl zomer 2026. Y2K revival met kwaliteitsborduurwerk",
                "PREMIUM MACHINEBORDUURWERK — Professioneel borduurwerk dat niet vervaagt, niet afschilfert en niet scheurt",
                "100% ZACHTE KATOEN — Brede rand voor optimale UV-bescherming. Licht en ademend voor dagelijks dragen",
                "VERSTELBAAR KINRIEMPJE — Perfecte pasvorm bij wind. One size voor dames en heren",
                "VEELZIJDIG — Ideaal voor festival, strand, outdoor, sport en dagelijks gebruik in de lente en zomer",
            ],
            "description": "Kwaliteits bucket hat in 100% katoen met premium machineborduurwerk. Brede rand voor UV-bescherming. De trendstijl voor festival, strand en outdoor. Unisex, one size, verstelbaar.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Katoen",
            "material": "Katoen",
            "style": "Casual",
            "unit_type": "stuk",
        },
        "PL": {
            "item_name": "Kapelusz Wędkarski Bucket Hat Haftowany Bawełna | Festival Plaża Unisex Trend | Y2K",
            "keywords": "kapelusz wędkarski bucket hat haftowany bawełna festival plaża unisex trend y2k regulowany sport",
            "bullets": [
                "TREND BUCKET HAT — Najmodniejszy styl kapelusza lato 2026. Revival Y2K z wysokiej jakości haftem",
                "PREMIUM HAFT MASZYNOWY — Profesjonalny haft, który nie blaknie, nie odpryskuje i nie rwie się",
                "100% MIĘKKA BAWEŁNA — Szerokie rondo dla optymalnej ochrony UV. Lekki i oddychający do codziennego noszenia",
                "REGULOWANY PASEK PODBRÓDKOWY — Idealne dopasowanie przy wietrze. Jeden rozmiar dla kobiet i mężczyzn",
                "WSZECHSTRONNY — Idealny na festiwal, plażę, outdoor, sport i na co dzień wiosna i lato",
            ],
            "description": "Wysokiej jakości bucket hat ze 100% bawełny z premium haftem maszynowym. Szerokie rondo dla ochrony UV. Styl trendu na festiwal, plażę i outdoor. Unisex, jeden rozmiar.",
            "pattern": "Jednobarwny",
            "age": "Dorosły",
            "care": "Pranie ręczne",
            "fabric": "100% Bawełna",
            "material": "Bawełna",
            "style": "Casual",
            "unit_type": "sztuka",
        },
        "SE": {
            "item_name": "Fiskarhatt Bucket Hat Broderad Bomull | Festival Strand Unisex Trend | Y2K Justerbar",
            "keywords": "fiskarhatt bucket hat broderad bomull festival strand unisex trend y2k justerbar sport outdoor",
            "bullets": [
                "TREND BUCKET HAT — Den trendigaste hattstilen sommar 2026. Y2K revival med kvalitetsbroderi",
                "PREMIUM MASKINBRODERI — Professionellt broderi som inte bleknar, flagnar och inte rivs",
                "100% MJUK BOMULL — Brett brätte för optimal UV-skydd. Lätt och andningsbar för dagligt bruk",
                "JUSTERBAR HAKREM — Perfekt passform vid blåst. One size för dam och herr",
                "MÅNGSIDIG — Perfekt för festival, strand, outdoor, sport och dagligt bruk vår och sommar",
            ],
            "description": "Kvalitetsbucket hat i 100% bomull med premium maskinbroderi. Brett brätte för UV-skydd. Trendstilen för festival, strand och outdoor. Unisex, one size, justerbar.",
            "pattern": "Enfärgad",
            "age": "Vuxen",
            "care": "Handtvätt",
            "fabric": "100% Bomull",
            "material": "Bomull",
            "style": "Casual",
            "unit_type": "styck",
        },
        "BE": {
            "item_name": "Vissershoed Bucket Hat Geborduurd Katoen | Festival Strand Unisex Trend | Y2K",
            "keywords": "vissershoed bucket hat geborduurd katoen festival strand unisex trend y2k verstelbaar sport",
            "bullets": [
                "TREND BUCKET HAT — De meest trendy hoedenstijl zomer 2026. Y2K revival met kwaliteitsborduurwerk",
                "PREMIUM MACHINEBORDUURWERK — Professioneel borduurwerk dat niet vervaagt, niet afschilfert en niet scheurt",
                "100% ZACHTE KATOEN — Brede rand voor optimale UV-bescherming. Licht en ademend voor dagelijks dragen",
                "VERSTELBAAR KINRIEMPJE — Perfecte pasvorm bij wind. One size voor dames en heren",
                "VEELZIJDIG — Ideaal voor festival, strand, outdoor, sport en dagelijks gebruik",
            ],
            "description": "Kwaliteits bucket hat in 100% katoen met premium machineborduurwerk. Brede rand voor UV-bescherming. De trendstijl voor festival, strand en outdoor. Unisex, one size.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Katoen",
            "material": "Katoen",
            "style": "Casual",
            "unit_type": "stuk",
        },
    },

    "dist": {
        "hat_form_type": "baseball_cap",
        "material_en": "Cotton",
        "fabric_type_en": "100% Cotton",
        "DE": {
            "item_name": "Distressed Vintage Baseball Cap bestickt | Used-Look Dad Hat Baumwolle Unisex | Retro Kappe",
            "keywords": "distressed vintage baseball cap bestickt used look dad hat baumwolle unisex retro kappe verstellbar",
            "bullets": [
                "DISTRESSED VINTAGE USED-LOOK — Authentisch verarbeiteter Worn-In-Effekt für den charaktervollen Retro-Style Frühling 2026",
                "PREMIUM MASCHINENSTICKEREI — Professionelles Embroidery das nicht verblasst, nicht abblättert und nicht reißt wie Drucke",
                "100% WEICHE BAUMWOLLE — Niedriges Profil, leicht strukturierte Vorderseite. Bequem für langen täglichen Einsatz",
                "VERSTELLBARER METALLVERSCHLUSS — Passt jedem Kopfumfang. Einheitsgröße für Damen und Herren unisex",
                "RETRO-STYLE — Perfekt für Streetwear, Outdoor, Reisen, Festivals und urbanen Alltag",
            ],
            "description": "Distressed Vintage Dad Hat aus 100% Baumwolle mit Maschinenstickerei. Authentischer Used-Look für charaktervollen Retro-Style. Verstellbarer Metallverschluss, eine Größe für alle. Ideal für Streetwear und Alltag.",
            "pattern": "Unifarben",
            "age": "Erwachsener",
            "care": "Handwäsche",
            "fabric": "100% Baumwolle",
            "material": "Baumwolle",
            "style": "Vintage",
            "unit_type": "Stück",
        },
        "FR": {
            "item_name": "Casquette Vintage Délavée Brodée | Dad Hat Used Look Coton Unisexe | Rétro Baseball Cap",
            "keywords": "casquette vintage délavée brodée dad hat used look coton unisexe rétro baseball cap ajustable",
            "bullets": [
                "EFFET DÉLAVÉ DISTRESSED — Aspect vieilli authentique pour un style rétro caractéristique printemps 2026",
                "BRODERIE MACHINE PREMIUM — Broderie professionnelle qui ne décolore pas, ne s'écaille pas et ne se déchire pas",
                "100% COTON DOUX — Faible profil, face avant légèrement structurée. Confortable pour port quotidien prolongé",
                "FERMETURE RÉGLABLE EN MÉTAL — S'adapte à toutes les tailles de tête. Taille unique pour femmes et hommes",
                "STYLE RÉTRO — Parfait pour streetwear, outdoor, voyages, festivals et quotidien urbain",
            ],
            "description": "Casquette dad hat délavée vintage en 100% coton avec broderie machine premium. Aspect used-look authentique pour un style rétro. Fermeture réglable en métal, taille unique.",
            "pattern": "Uni",
            "age": "Adulte",
            "care": "Lavage à la main",
            "fabric": "100% Coton",
            "material": "Coton",
            "style": "Vintage",
            "unit_type": "pièce",
        },
        "IT": {
            "item_name": "Cappellino Vintage Consumato Ricamato | Dad Hat Used Look Cotone Unisex | Berretto Retrò",
            "keywords": "cappellino vintage consumato ricamato dad hat used look cotone unisex berretto retrò regolabile",
            "bullets": [
                "EFFETTO CONSUMATO DISTRESSED — Aspetto invecchiato autentico per uno stile retrò caratteristico primavera 2026",
                "RICAMO A MACCHINA PREMIUM — Ricamo professionale che non sbiadisce, non si stacca e non si strappa",
                "100% COTONE MORBIDO — Profilo basso, parte anteriore leggermente strutturata. Comodo per uso quotidiano prolungato",
                "CHIUSURA REGOLABILE IN METALLO — Si adatta a tutte le misure di testa. Taglia unica per donna e uomo",
                "STILE RETRÒ — Perfetto per streetwear, outdoor, viaggi, festival e quotidiano urbano",
            ],
            "description": "Cappellino dad hat consumato vintage in 100% cotone con ricamo a macchina premium. Aspetto used-look autentico per uno stile retrò. Chiusura regolabile in metallo.",
            "pattern": "Tinta Unita",
            "age": "Adulto",
            "care": "Lavaggio a mano",
            "fabric": "100% Cotone",
            "material": "Cotone",
            "style": "Vintage",
            "unit_type": "pezzo",
        },
        "ES": {
            "item_name": "Gorra Vintage Desgastada Bordada | Dad Hat Used Look Algodón Unisex | Béisbol Retro",
            "keywords": "gorra vintage desgastada bordada dad hat used look algodón unisex béisbol retro ajustable",
            "bullets": [
                "EFECTO DESGASTADO DISTRESSED — Apariencia envejecida auténtica para un estilo retro característico primavera 2026",
                "BORDADO A MÁQUINA PREMIUM — Bordado profesional que no destiñe, no se descascarilla y no se rompe",
                "100% ALGODÓN SUAVE — Perfil bajo, parte delantera ligeramente estructurada. Cómoda para uso diario prolongado",
                "CIERRE AJUSTABLE EN METAL — Se adapta a todos los tamaños de cabeza. Talla única para mujer y hombre",
                "ESTILO RETRO — Perfecto para streetwear, outdoor, viajes, festivales y quotidiano urbano",
            ],
            "description": "Gorra dad hat desgastada vintage en 100% algodón con bordado a máquina premium. Apariencia used-look auténtica para estilo retro. Cierre ajustable en metal.",
            "pattern": "Liso",
            "age": "Adulto",
            "care": "Lavado a mano",
            "fabric": "100% Algodón",
            "material": "Algodón",
            "style": "Vintage",
            "unit_type": "pieza",
        },
        "NL": {
            "item_name": "Vintage Distressed Baseball Cap Geborduurd | Used Look Dad Hat Katoen Unisex | Retro Kep",
            "keywords": "vintage distressed baseball cap geborduurd used look dad hat katoen unisex retro kep verstelbaar",
            "bullets": [
                "DISTRESSED VINTAGE USED-LOOK — Authentiek versleten effect voor karaktervolle retro-stijl lente 2026",
                "PREMIUM MACHINEBORDUURWERK — Professioneel borduurwerk dat niet vervaagt, niet afschilfert en niet scheurt",
                "100% ZACHTE KATOEN — Laag profiel, licht gestructureerde voorkant. Comfortabel voor lang dagelijks gebruik",
                "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size voor dames en heren",
                "RETRO-STIJL — Ideaal voor streetwear, outdoor, reizen, festivals en urban dagelijks leven",
            ],
            "description": "Vintage distressed dad hat in 100% katoen met premium machineborduurwerk. Authentiek used-look voor karaktervolle retro-stijl. Verstelbare metalen sluiting.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Katoen",
            "material": "Katoen",
            "style": "Vintage",
            "unit_type": "stuk",
        },
        "PL": {
            "item_name": "Vintage Distressed Baseball Cap Haftowany | Used Look Dad Hat Bawełna Unisex | Retro",
            "keywords": "vintage distressed baseball cap haftowany used look dad hat bawełna unisex retro regulowany",
            "bullets": [
                "EFEKT DISTRESSED VINTAGE — Autentyczne przetarcia dla charakterystycznego retro stylu wiosna 2026",
                "PREMIUM HAFT MASZYNOWY — Profesjonalny haft, który nie blaknie, nie odpryskuje i nie rwie się",
                "100% MIĘKKA BAWEŁNA — Niski profil, lekko ustrukturyzowany przód. Wygodny do długiego codziennego noszenia",
                "REGULOWANE METALOWE ZAPIĘCIE — Dopasowuje się do każdego obwodu głowy. Jeden rozmiar dla kobiet i mężczyzn",
                "RETRO STYL — Idealny do streetwear, outdoor, podróży, festiwali i miejskiego stylu życia",
            ],
            "description": "Vintage distressed dad hat ze 100% bawełny z premium haftem maszynowym. Autentyczny used-look dla retro stylu. Regulowane metalowe zapięcie, jeden rozmiar.",
            "pattern": "Jednobarwny",
            "age": "Dorosły",
            "care": "Pranie ręczne",
            "fabric": "100% Bawełna",
            "material": "Bawełna",
            "style": "Vintage",
            "unit_type": "sztuka",
        },
        "SE": {
            "item_name": "Vintage Distressed Baseball Keps Broderad | Used Look Dad Hat Bomull Unisex | Retro",
            "keywords": "vintage distressed baseball keps broderad used look dad hat bomull unisex retro justerbar",
            "bullets": [
                "DISTRESSED VINTAGE USED-LOOK — Autentisk slitageeffekt för karaktärsfull retrostil vår 2026",
                "PREMIUM MASKINBRODERI — Professionellt broderi som inte bleknar, flagnar och inte rivs",
                "100% MJUK BOMULL — Låg profil, något strukturerad framsida. Bekväm för långvarigt dagligt bruk",
                "JUSTERBAR METALLSPÄNNE — Passar alla huvudstorlekar. One size för dam och herr",
                "RETROSTIL — Perfekt för streetwear, outdoor, resor, festivaler och urban vardag",
            ],
            "description": "Vintage distressed dad hat i 100% bomull med premium maskinbroderi. Autentisk used-look för karaktärsfull retrostil. Justerbar metallspänne, one size.",
            "pattern": "Enfärgad",
            "age": "Vuxen",
            "care": "Handtvätt",
            "fabric": "100% Bomull",
            "material": "Bomull",
            "style": "Vintage",
            "unit_type": "styck",
        },
        "BE": {
            "item_name": "Vintage Distressed Baseball Cap Geborduurd | Used Look Dad Hat Katoen Unisex | Retro",
            "keywords": "vintage distressed baseball cap geborduurd used look dad hat katoen unisex retro verstelbaar",
            "bullets": [
                "DISTRESSED VINTAGE USED-LOOK — Authentiek versleten effect voor karaktervolle retro-stijl lente 2026",
                "PREMIUM MACHINEBORDUURWERK — Professioneel borduurwerk dat niet vervaagt, niet afschilfert en niet scheurt",
                "100% ZACHTE KATOEN — Laag profiel, licht gestructureerde voorkant. Comfortabel voor dagelijks gebruik",
                "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size voor dames en heren",
                "RETRO-STIJL — Ideaal voor streetwear, outdoor, reizen, festivals en urban dagelijks leven",
            ],
            "description": "Vintage distressed dad hat in 100% katoen met premium machineborduurwerk. Authentiek used-look voor karaktervolle retro-stijl. Verstelbare metalen sluiting.",
            "pattern": "Effen",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "100% Katoen",
            "material": "Katoen",
            "style": "Vintage",
            "unit_type": "stuk",
        },
    },

    "cord": {
        "hat_form_type": "baseball_cap",
        "material_en": "Corduroy",
        "fabric_type_en": "Corduroy",
        "DE": {
            "item_name": "Cord Cap Corduroy Kappe bestickt | Verstellbare Baseball Cap Unisex | Herbst Frühling 2026",
            "keywords": "cord cap corduroy kappe bestickt verstellbar baseball cap unisex herbst frühling kord mütze stickerei",
            "bullets": [
                "PREMIUM CORD-STOFF — Hochwertige Corduroy-Textur für den angesagtesten Cap-Trend Frühling/Herbst 2026. Fashion-Forward und zeitlos",
                "MASCHINENSTICKEREI — Professionelles Embroidery in zurückhaltenden Naturtönen das nicht verblasst und nicht abblättert",
                "WEICHER CORDUROY — Angenehm weicher Rippenstoff, niedriges Profil, vorgebogener Schirm. Ideal für kühles Wetter",
                "VERSTELLBARER METALLVERSCHLUSS — Passt jedem Kopfumfang. Einheitsgröße für Damen und Herren unisex",
                "FASHION STATEMENT — Perfekt für Freizeit, Reisen, Urban Style und den täglichen Gebrauch im Frühling und Herbst",
            ],
            "description": "Cord Cap aus hochwertigem Corduroy mit Maschinenstickerei. Der Fashion-Trend Frühling/Herbst 2026 vereint Textur und Style. Weicher Rippenstoff, verstellbarer Metallverschluss, eine Größe für alle.",
            "pattern": "Gestreift",
            "age": "Erwachsener",
            "care": "Handwäsche",
            "fabric": "Cord (Baumwolle)",
            "material": "Cord",
            "style": "Fashion",
            "unit_type": "Stück",
        },
        "FR": {
            "item_name": "Casquette Velours Côtelé Brodée | Baseball Cap Côtelé Unisexe Réglable | Tendance 2026",
            "keywords": "casquette velours côtelé brodée baseball cap cotelé unisexe réglable tendance printemps automne 2026",
            "bullets": [
                "VELOURS CÔTELÉ PREMIUM — Texture côtelée haut de gamme pour la tendance casquette printemps/automne 2026. Fashion-Forward",
                "BRODERIE MACHINE — Broderie professionnelle dans des tons neutres naturels qui ne décolore pas et ne s'écaille pas",
                "VELOURS DOUX — Tissu côtelé confortable, faible profil, visière pré-courbée. Idéal pour temps frais",
                "FERMETURE RÉGLABLE EN MÉTAL — S'adapte à toutes les tailles de tête. Taille unique pour femmes et hommes",
                "STATEMENT FASHION — Parfait pour loisirs, voyages, style urbain et quotidien au printemps et automne",
            ],
            "description": "Casquette en velours côtelé premium avec broderie machine. La tendance fashion printemps/automne 2026 alliant texture et style. Velours doux, fermeture réglable en métal.",
            "pattern": "Rayures",
            "age": "Adulte",
            "care": "Lavage à la main",
            "fabric": "Velours Côtelé (Coton)",
            "material": "Velours Côtelé",
            "style": "Fashion",
            "unit_type": "pièce",
        },
        "IT": {
            "item_name": "Cappellino Velluto a Coste Ricamato | Baseball Cap Corduroy Unisex Regolabile | Moda 2026",
            "keywords": "cappellino velluto a coste ricamato baseball cap corduroy unisex regolabile moda primavera autunno 2026",
            "bullets": [
                "VELLUTO A COSTE PREMIUM — Texture a coste pregiata per la tendenza berretto primavera/autunno 2026. Fashion-Forward",
                "RICAMO A MACCHINA — Ricamo professionale in toni neutri naturali che non sbiadisce e non si stacca",
                "VELLUTO MORBIDO — Tessuto a coste confortevole, profilo basso, visiera pre-curvata. Ideale per clima fresco",
                "CHIUSURA REGOLABILE IN METALLO — Si adatta a tutte le misure di testa. Taglia unica per donna e uomo",
                "FASHION STATEMENT — Perfetto per tempo libero, viaggi, stile urbano e uso quotidiano",
            ],
            "description": "Cappellino in velluto a coste premium con ricamo a macchina. La tendenza fashion primavera/autunno 2026 unisce texture e stile. Velluto morbido, chiusura regolabile in metallo.",
            "pattern": "Righe",
            "age": "Adulto",
            "care": "Lavaggio a mano",
            "fabric": "Velluto a Coste (Cotone)",
            "material": "Velluto a Coste",
            "style": "Fashion",
            "unit_type": "pezzo",
        },
        "ES": {
            "item_name": "Gorra Pana Bordada | Baseball Cap Corduroi Unisex Ajustable | Moda Primavera Otoño 2026",
            "keywords": "gorra pana bordada baseball cap corduroi unisex ajustable moda primavera otoño 2026 tendencia",
            "bullets": [
                "PANA PREMIUM — Textura de pana de alta calidad para la tendencia gorra primavera/otoño 2026. Fashion-Forward",
                "BORDADO A MÁQUINA — Bordado profesional en tonos neutros naturales que no destiñe y no se descascarilla",
                "PANA SUAVE — Tejido acanalado suave, perfil bajo, visera pre-curvada. Ideal para clima fresco",
                "CIERRE AJUSTABLE EN METAL — Se adapta a todos los tamaños de cabeza. Talla única para mujer y hombre",
                "STATEMENT FASHION — Perfecto para ocio, viajes, estilo urbano y uso diario",
            ],
            "description": "Gorra de pana premium con bordado a máquina. La tendencia fashion primavera/otoño 2026 que combina textura y estilo. Pana suave, cierre ajustable en metal.",
            "pattern": "Rayas",
            "age": "Adulto",
            "care": "Lavado a mano",
            "fabric": "Pana (Algodón)",
            "material": "Pana",
            "style": "Fashion",
            "unit_type": "pieza",
        },
        "NL": {
            "item_name": "Corduroy Cap Geborduurd | Baseball Cap Ribfluweel Unisex Verstelbaar | Trend 2026",
            "keywords": "corduroy cap geborduurd baseball cap ribfluweel unisex verstelbaar trend lente herfst 2026",
            "bullets": [
                "PREMIUM CORDUROY STOF — Hoogwaardige ribfluwelen textuur voor de trendiest cap-stijl lente/herfst 2026",
                "MACHINEBORDUURWERK — Professioneel borduurwerk in zachte natuurtinten dat niet vervaagt en niet afschilfert",
                "ZACHTE CORDUROY — Comfortabele ribbeltjes stof, laag profiel, voorgebogen klep. Ideaal voor koeler weer",
                "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size voor dames en heren",
                "FASHION STATEMENT — Ideaal voor vrije tijd, reizen, urban style en dagelijks gebruik",
            ],
            "description": "Corduroy cap van premium ribfluweel met machineborduurwerk. De fashion trend lente/herfst 2026 combineert textuur en stijl. Zachte corduroy, verstelbare metalen sluiting.",
            "pattern": "Gestreept",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "Corduroy (Katoen)",
            "material": "Corduroy",
            "style": "Fashion",
            "unit_type": "stuk",
        },
        "PL": {
            "item_name": "Czapka Sztruksowa Haftowana | Baseball Cap Sztruks Unisex Regulowana | Trend 2026",
            "keywords": "czapka sztruksowa haftowana baseball cap sztruks unisex regulowana trend wiosna jesień 2026",
            "bullets": [
                "PREMIUM SZTRUKS — Wysokiej jakości sztruksowa tekstura dla najmodniejszego stylu czapek wiosna/jesień 2026",
                "HAFT MASZYNOWY — Profesjonalny haft w stonowanych naturalnych tonach, który nie blaknie i nie odpryskuje",
                "MIĘKKI SZTRUKS — Wygodna żeberkowa tkanina, niski profil, zakrzywiony daszek. Idealna na chłodniejszą pogodę",
                "REGULOWANE METALOWE ZAPIĘCIE — Dopasowuje się do każdego obwodu głowy. Jeden rozmiar dla kobiet i mężczyzn",
                "MODNY LOOK — Idealna na co dzień, podróże, miejski styl i wszelkie okazje",
            ],
            "description": "Czapka sztruksowa z premium sztruksu z haftem maszynowym. Modny trend wiosna/jesień 2026 łączący teksturę ze stylem. Miękki sztruks, regulowane metalowe zapięcie.",
            "pattern": "Prążkowany",
            "age": "Dorosły",
            "care": "Pranie ręczne",
            "fabric": "Sztruks (Bawełna)",
            "material": "Sztruks",
            "style": "Fashion",
            "unit_type": "sztuka",
        },
        "SE": {
            "item_name": "Manchesterkeps Broderad | Baseball Keps Corduroy Unisex Justerbar | Trend 2026",
            "keywords": "manchesterkeps broderad baseball keps corduroy unisex justerbar trend vår höst 2026",
            "bullets": [
                "PREMIUM MANCHESTER-TYGET — Högkvalitativt ribbat tyg för den trendigaste kepsstilen vår/höst 2026",
                "MASKINBRODERI — Professionellt broderi i dämpade naturtoner som inte bleknar och inte flagnar",
                "MJUK MANCHESTER — Bekvämt ribbat tyg, låg profil, förböjd skärm. Idealisk för svalare väder",
                "JUSTERBAR METALLSPÄNNE — Passar alla huvudstorlekar. One size för dam och herr",
                "FASHION STATEMENT — Perfekt för fritid, resor, urban stil och dagligt bruk",
            ],
            "description": "Manchesterkeps av premium corduroy med maskinbroderi. Trendstilen vår/höst 2026 förenar textur och stil. Mjukt manchestertyg, justerbar metallspänne.",
            "pattern": "Randigt",
            "age": "Vuxen",
            "care": "Handtvätt",
            "fabric": "Manchester (Bomull)",
            "material": "Manchester",
            "style": "Fashion",
            "unit_type": "styck",
        },
        "BE": {
            "item_name": "Corduroy Cap Geborduurd | Baseball Cap Ribfluweel Unisex Verstelbaar | Trend Lente 2026",
            "keywords": "corduroy cap geborduurd baseball cap ribfluweel unisex verstelbaar trend lente herfst 2026",
            "bullets": [
                "PREMIUM CORDUROY STOF — Hoogwaardige ribfluwelen textuur voor de trendiest cap-stijl lente/herfst 2026",
                "MACHINEBORDUURWERK — Professioneel borduurwerk in zachte natuurtinten dat niet vervaagt en niet afschilfert",
                "ZACHTE CORDUROY — Comfortabele ribbeltjes stof, laag profiel, voorgebogen klep. Ideaal voor koeler weer",
                "VERSTELBARE METALEN SLUITING — Past elke hoofdomtrek. One size voor dames en heren",
                "FASHION STATEMENT — Ideaal voor vrije tijd, reizen, urban style en dagelijks gebruik",
            ],
            "description": "Corduroy cap van premium ribfluweel met machineborduurwerk. De fashion trend lente/herfst 2026. Zachte corduroy, verstelbare metalen sluiting.",
            "pattern": "Gestreept",
            "age": "Volwassene",
            "care": "Handwas",
            "fabric": "Corduroy (Katoen)",
            "material": "Corduroy",
            "style": "Fashion",
            "unit_type": "stuk",
        },
    },
}

# ── Listing builder ──────────────────────────────────────────────────────────

# Browse nodes for hats in EU
BROWSE_NODES = {
    "DE": "1981316031",
    "FR": "1981316031",
    "IT": "1981316031",
    "ES": "1981316031",
    "NL": "1981316031",
    "PL": "1981316031",
    "SE": "1981316031",
    "BE": "1981316031",
}


def build_listing_attrs(product_key: str, mkt_code: str, color_key: str | None,
                        parent_sku: str, is_parent: bool) -> dict:
    """Build Amazon listing attributes for a spring hat product."""
    mkt_id = MARKETPLACE_IDS[mkt_code]
    lang = LANG_TAGS[mkt_code]
    currency = CURRENCIES[mkt_code]
    price = PRICES[product_key][mkt_code]
    trans = CONTENT[product_key][mkt_code]
    hat_form = CONTENT[product_key]["hat_form_type"]
    material_en = CONTENT[product_key]["material_en"]
    fabric_en = CONTENT[product_key]["fabric_type_en"]

    colors_map = PRODUCT_COLORS[product_key]

    if is_parent:
        color_value = {
            "DE": "Mehrfarbig", "FR": "Multicolore", "IT": "Multicolore",
            "ES": "Multicolor", "NL": "Meerkleurig", "PL": "Wielokolorowy",
            "SE": "Flerfärgad", "BE": "Meerkleurig",
        }[mkt_code]
        item_name = trans["item_name"]
    else:
        color_value = colors_map[color_key][mkt_code]
        # Append color to title for child, keep under 200 chars
        base_name = trans["item_name"]
        child_suffix = f" | {color_value}"
        if len(base_name) + len(child_suffix) <= 200:
            item_name = base_name + child_suffix
        else:
            item_name = base_name[:197] + "..."

    attrs = {
        "item_type_name": [{"value": "baseball-caps", "language_tag": lang, "marketplace_id": mkt_id}],
        "color": [{"value": color_value, "language_tag": lang, "marketplace_id": mkt_id}],
        "variation_theme": [{"name": "COLOR"}],
        "weave_type": [{"value": "Twill", "language_tag": lang, "marketplace_id": mkt_id}],
        "pattern": [{"value": trans["pattern"], "language_tag": lang, "marketplace_id": mkt_id}],
        "dsa_responsible_party_address": [{"value": "support@printful.com", "marketplace_id": mkt_id}],
        "manufacturer": [{"value": "Printful Latvia AS", "language_tag": lang, "marketplace_id": mkt_id}],
        "model_name": [{"value": f"Spring 2026 {product_key.upper()} Hat", "language_tag": lang, "marketplace_id": mkt_id}],
        "age_range_description": [{"value": trans["age"], "language_tag": lang, "marketplace_id": mkt_id}],
        "recommended_browse_nodes": [{"value": BROWSE_NODES[mkt_code], "marketplace_id": mkt_id}],
        "bullet_point": [{"value": b, "language_tag": lang, "marketplace_id": mkt_id} for b in trans["bullets"]],
        "product_description": [{"value": trans["description"], "language_tag": lang, "marketplace_id": mkt_id}],
        "department": [{"value": "Unisex", "language_tag": lang, "marketplace_id": mkt_id}],
        "brand": [{"value": "Printful", "language_tag": lang, "marketplace_id": mkt_id}],
        "supplier_declared_dg_hz_regulation": [{"value": "not_applicable", "marketplace_id": mkt_id}],
        "supplier_declared_has_product_identifier_exemption": [{"value": True, "marketplace_id": mkt_id}],
        "generic_keyword": [{"value": trans["keywords"], "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_safety_attestation": [{"value": True, "marketplace_id": mkt_id}],
        "headwear_size": [{"size": "one_size", "size_system": SIZE_SYSTEMS.get(mkt_code, "as3"), "size_class": "alpha", "marketplace_id": mkt_id}],
        "outer": [{"material": [{"value": material_en, "language_tag": lang}], "marketplace_id": mkt_id}],
        "country_of_origin": [{"value": "CN", "marketplace_id": mkt_id}],
        "special_size_type": [{"value": "Standard", "language_tag": lang, "marketplace_id": mkt_id}],
        "gpsr_manufacturer_reference": [{"gpsr_manufacturer_email_address": "support@printful.com", "marketplace_id": mkt_id}],
        "item_name": [{"value": item_name, "language_tag": lang, "marketplace_id": mkt_id}],
        "merchant_shipping_group": [{"value": "legacy-template-id", "marketplace_id": mkt_id}],
        "list_price": [{"value_with_tax": price, "currency": currency, "marketplace_id": mkt_id}],
        "batteries_required": [{"value": False, "marketplace_id": mkt_id}],
        "fabric_type": [{"value": trans["fabric"], "language_tag": lang, "marketplace_id": mkt_id}],
        "condition_type": [{"value": "new_new", "marketplace_id": mkt_id}],
        "material": [{"value": trans["material"], "language_tag": lang, "marketplace_id": mkt_id}],
        "style": [{"value": trans["style"], "language_tag": lang, "marketplace_id": mkt_id}],
        "hat_form_type": [{"value": hat_form, "marketplace_id": mkt_id}],
        "care_instructions": [{"value": trans["care"], "language_tag": lang, "marketplace_id": mkt_id}],
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


# ── Task runner ──────────────────────────────────────────────────────────────

ALL_MARKETS = ["DE", "FR", "IT", "ES", "NL", "PL", "SE", "BE"]
ALL_PRODUCTS = ["washed", "orgbucket", "bucket", "dist", "cord"]


def create_product_listings(product_key: str, markets: list[str], dry_run: bool = False) -> dict:
    """Create parent + child listings for a single product across specified markets."""
    parent_sku = PARENT_SKUS[product_key]
    colors = PRODUCT_COLORS[product_key]
    results = {"created": 0, "skipped": 0, "errors": 0}

    print(f"\n{'=' * 70}")
    print(f"Product: {product_key.upper()} | Parent SKU: {parent_sku}")
    print(f"Colors: {list(colors.keys())} | Markets: {markets}")
    print(f"{'=' * 70}")

    for mkt_code in markets:
        mkt_id = MARKETPLACE_IDS[mkt_code]
        print(f"\n  ── Market: {mkt_code} ──")

        # ── Parent ──────────────────────────────────────────────────────────
        if not dry_run and check_listing_exists(parent_sku, mkt_id):
            print(f"    Parent {parent_sku} already exists on {mkt_code}, skipping")
            results["skipped"] += 1
        else:
            attrs = build_listing_attrs(product_key, mkt_code, None, parent_sku, is_parent=True)
            title = attrs["item_name"][0]["value"]
            print(f"    [PARENT] {parent_sku} — \"{title[:80]}...\"" if len(title) > 80 else f"    [PARENT] {parent_sku} — \"{title}\"")
            assert len(title) <= 200, f"Title too long ({len(title)} chars): {title}"
            status, resp = put_listing(parent_sku, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202) or dry_run:
                results["created"] += 1
            else:
                results["errors"] += 1
                print(f"    ERROR: {resp}")
            time.sleep(1.5)

        # ── Children ─────────────────────────────────────────────────────────
        for color_key in colors:
            child_sku = f"{parent_sku}-{color_key}"
            if not dry_run and check_listing_exists(child_sku, mkt_id):
                print(f"    Child {child_sku} already exists on {mkt_code}, skipping")
                results["skipped"] += 1
                continue

            attrs = build_listing_attrs(product_key, mkt_code, color_key, parent_sku, is_parent=False)
            title = attrs["item_name"][0]["value"]
            color_val = attrs["color"][0]["value"]
            print(f"    [CHILD] {child_sku} [{color_val}] — \"{title[:70]}...\"" if len(title) > 70 else f"    [CHILD] {child_sku} [{color_val}]")
            assert len(title) <= 200, f"Title too long ({len(title)} chars): {title}"
            status, resp = put_listing(child_sku, mkt_id, attrs, dry_run=dry_run)
            if status in (200, 202) or dry_run:
                results["created"] += 1
            else:
                results["errors"] += 1
                print(f"    ERROR: {resp}")
            time.sleep(1)

    return results


def main():
    parser = argparse.ArgumentParser(description="Create Spring 2026 Hat Listings on Amazon EU")
    parser.add_argument(
        "--product", default="all",
        choices=ALL_PRODUCTS + ["all"],
        help="Product to create (default: all)"
    )
    parser.add_argument(
        "--market", default="all",
        choices=ALL_MARKETS + ["all"],
        help="Market to target (default: all)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print what would be created without calling API"
    )
    args = parser.parse_args()

    products = ALL_PRODUCTS if args.product == "all" else [args.product]
    markets = ALL_MARKETS if args.market == "all" else [args.market]

    print(f"\nSpring 2026 Hat Listing Creator")
    print(f"Products: {products}")
    print(f"Markets:  {markets}")
    print(f"Dry-run:  {args.dry_run}")

    total = {"created": 0, "skipped": 0, "errors": 0}
    results_log = []

    for product_key in products:
        r = create_product_listings(product_key, markets, dry_run=args.dry_run)
        for k in total:
            total[k] += r[k]
        results_log.append({"product": product_key, **r})

    print(f"\n{'=' * 70}")
    print("SUMMARY")
    print(f"{'=' * 70}")
    for entry in results_log:
        print(f"  {entry['product']:12s} — created: {entry['created']}, skipped: {entry['skipped']}, errors: {entry['errors']}")
    print(f"  {'TOTAL':12s} — created: {total['created']}, skipped: {total['skipped']}, errors: {total['errors']}")

    # Save results
    out_path = os.path.join(os.path.dirname(__file__), "create_spring_hats_results.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({
            "run_at": __import__("datetime").datetime.utcnow().isoformat() + "Z",
            "dry_run": args.dry_run,
            "products": products,
            "markets": markets,
            "results": results_log,
            "total": total,
        }, f, ensure_ascii=False, indent=2)
    print(f"\nResults saved to {out_path}")


if __name__ == "__main__":
    main()
