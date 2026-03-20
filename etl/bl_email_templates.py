"""BL Email Templates — generates HTML/text templates ready to paste into Baselinker panel.

These templates use BL's variable syntax: {order_id}, {name}, {tracking_nr}, etc.

Usage:
  python3.11 -m etl.bl_email_templates              # print all templates
  python3.11 -m etl.bl_email_templates --template 1  # specific template
  python3.11 -m etl.bl_email_templates --save        # save to ~/nesell-analytics/data/email_templates/

Template list:
  1. Tracking notification (after shipping)
  2. Review request (24h after delivery)
  3. Invoice attached (when invoice issued)
  4. Return confirmation (return registered)
  5. Out of stock apology (if order cancelled due to OOS)
"""
import argparse
import json
from pathlib import Path

# ── Templates ─────────────────────────────────────────────────────────────────

TEMPLATES = {
    1: {
        "name": "Tracking — Potwierdzenie wysyłki",
        "trigger": "Automatyczna akcja: Zmiana statusu → 'Wysłane'",
        "subject": "Twoje zamówienie #{order_id} zostało wysłane!",
        "body_pl": """\
Cześć {delivery_fullname}!

Twoje zamówienie #{order_id} właśnie opuściło nasz magazyn i jest w drodze do Ciebie.

📦 Numer śledzenia: {package_number}
🚚 Kurier: {delivery_package_module}

Możesz śledzić swoją paczkę tutaj:
https://www.dpd.com/pl/pl/tracking/?parcelnr={package_number}

Szacowany czas dostawy: 2-5 dni roboczych.

Jeśli masz jakiekolwiek pytania, odpowiedz na tę wiadomość.

Pozdrawiamy,
Zespół Nesell
""",
        "body_de": """\
Hallo {delivery_fullname},

Ihre Bestellung #{order_id} wurde soeben aus unserem Lager versandt.

📦 Sendungsnummer: {package_number}
🚚 Kurier: {delivery_package_module}

Verfolgen Sie Ihre Sendung hier:
https://www.dpdgroup.com/de/mydpd/my-parcels/track?parcelno={package_number}

Geschätzte Lieferzeit: 3-7 Werktage.

Bei Fragen antworten Sie einfach auf diese E-Mail.

Mit freundlichen Grüßen,
Das Nesell-Team
""",
        "body_en": """\
Hello {delivery_fullname},

Your order #{order_id} has just left our warehouse and is on its way to you!

📦 Tracking number: {package_number}
🚚 Carrier: {delivery_package_module}

Track your package here:
https://www.dpdgroup.com/en/mydpd/my-parcels/track?parcelno={package_number}

Estimated delivery: 3-7 business days.

If you have any questions, simply reply to this email.

Best regards,
The Nesell Team
""",
    },

    2: {
        "name": "Review Request — Prośba o opinię (24h po dostarczeniu)",
        "trigger": "Automatyczna akcja: Status kuriera 'Dostarczono' → Poczekaj 24h → Wyślij email",
        "subject": "Jak minęło zamówienie #{order_id}? Zostaw opinię!",
        "body_pl": """\
Cześć {delivery_fullname}!

Mamy nadzieję, że Twoje zamówienie dotarło bezpiecznie 😊

Twoja opinia jest dla nas bardzo ważna! Zajmie Ci to dosłownie minutę.

⭐ Zostaw opinię tutaj:
{order_page}

Jeśli cokolwiek nie spełniło Twoich oczekiwań — napisz do nas ZANIM wystawisz opinię.
Zawsze znajdziemy rozwiązanie: {buyer_email}

Dziękujemy za zakupy!

Pozdrawiamy,
Zespół Nesell
""",
        "body_de": """\
Hallo {delivery_fullname},

wir hoffen, dass Ihre Bestellung gut angekommen ist! 😊

Ihre Bewertung bedeutet uns sehr viel. Es dauert nur eine Minute:

⭐ Jetzt bewerten:
{order_page}

Falls etwas nicht Ihren Erwartungen entsprochen hat, schreiben Sie uns BEVOR Sie eine Bewertung abgeben.
Wir finden immer eine Lösung: {buyer_email}

Vielen Dank für Ihren Einkauf!

Mit freundlichen Grüßen,
Das Nesell-Team
""",
        "body_en": """\
Hello {delivery_fullname},

We hope your order arrived safely! 😊

Your feedback means a lot to us. It only takes a minute:

⭐ Leave a review here:
{order_page}

If anything didn't meet your expectations, please write to us BEFORE leaving a review.
We always find a solution: {buyer_email}

Thank you for shopping with us!

Best regards,
The Nesell Team
""",
    },

    3: {
        "name": "Faktura — Powiadomienie o wystawieniu faktury",
        "trigger": "Automatyczna akcja: Wystaw fakturę → Wyślij email do klienta",
        "subject": "Faktura do zamówienia #{order_id}",
        "body_pl": """\
Cześć {delivery_fullname},

w załączniku znajdziesz fakturę do zamówienia #{order_id}.

Dane faktury:
- Numer: {invoice_number}
- Data: {invoice_date}
- Kwota: {order_total} {currency}

Faktura jest również dostępna w Twoim koncie na platformie sprzedażowej.

W razie pytań — odpowiedz na tę wiadomość.

Pozdrawiamy,
Zespół Nesell
""",
        "body_de": """\
Hallo {delivery_fullname},

im Anhang finden Sie die Rechnung zu Ihrer Bestellung #{order_id}.

Rechnungsdetails:
- Nummer: {invoice_number}
- Datum: {invoice_date}
- Betrag: {order_total} {currency}

Bei Fragen antworten Sie einfach auf diese E-Mail.

Mit freundlichen Grüßen,
Das Nesell-Team
""",
    },

    4: {
        "name": "Zwrot — Potwierdzenie przyjęcia zwrotu",
        "trigger": "Automatyczna akcja: Utworzono zwrot → Wyślij email",
        "subject": "Potwierdzenie przyjęcia zwrotu #ZWR-{order_id}",
        "body_pl": """\
Cześć {delivery_fullname},

potwierdzamy przyjęcie Twojego zgłoszenia zwrotu do zamówienia #{order_id}.

Co dalej?
1. Wyślij produkt na nasz adres (jeśli jeszcze tego nie zrobiłeś)
2. Po otrzymaniu paczki przetworzymy zwrot w ciągu 3-5 dni roboczych
3. Środki wrócą na Twoje konto w ciągu 5-10 dni roboczych

Masz pytania? Odpisz na tę wiadomość.

Przepraszamy za wszelkie niedogodności.

Pozdrawiamy,
Zespół Nesell
""",
        "body_de": """\
Hallo {delivery_fullname},

wir bestätigen den Eingang Ihrer Rücksendeanfrage zur Bestellung #{order_id}.

Nächste Schritte:
1. Senden Sie das Produkt an unsere Adresse (falls noch nicht geschehen)
2. Nach Eingang bearbeiten wir die Rückgabe innerhalb von 3-5 Werktagen
3. Die Rückerstattung erfolgt innerhalb von 5-10 Werktagen

Bei Fragen antworten Sie einfach auf diese E-Mail.

Wir entschuldigen uns für etwaige Unannehmlichkeiten.

Mit freundlichen Grüßen,
Das Nesell-Team
""",
    },

    5: {
        "name": "Brak towaru — Przeprosiny za anulowanie",
        "trigger": "Automatyczna akcja: Zmiana statusu → 'Anulowane (brak towaru)'",
        "subject": "Ważna informacja o zamówieniu #{order_id}",
        "body_pl": """\
Cześć {delivery_fullname},

z przykrością informujemy, że Twoje zamówienie #{order_id} zostało anulowane z powodu braku towaru w magazynie.

Środki w kwocie {order_total} {currency} zostaną zwrócone na Twoje konto w ciągu 5-10 dni roboczych.

Przepraszamy za wszelkie niedogodności. Jeśli masz pytania, odpowiedz na tę wiadomość.

Pozdrawiamy,
Zespół Nesell
""",
        "body_de": """\
Hallo {delivery_fullname},

leider müssen wir Ihnen mitteilen, dass Ihre Bestellung #{order_id} aufgrund von Nichtverfügbarkeit des Artikels storniert wurde.

Der Betrag von {order_total} {currency} wird innerhalb von 5-10 Werktagen auf Ihr Konto zurücküberwiesen.

Wir entschuldigen uns für die Unannehmlichkeiten.

Mit freundlichen Grüßen,
Das Nesell-Team
""",
    },
}

# ── BL Variable Reference ─────────────────────────────────────────────────────

BL_VARIABLES = """
Baselinker email variables (paste these into BL panel):
  {order_id}              — numer zamówienia BL
  {delivery_fullname}     — imię i nazwisko do dostawy
  {delivery_address}      — adres dostawy
  {delivery_city}         — miasto dostawy
  {delivery_country_code} — kod kraju
  {package_number}        — numer śledzenia paczki
  {delivery_package_module} — nazwa kuriera (DPD, InPost...)
  {order_total}           — kwota zamówienia
  {currency}              — waluta
  {buyer_email}           — email kupującego
  {order_page}            — link do zamówienia na marketplace
  {invoice_number}        — numer faktury
  {invoice_date}          — data wystawienia faktury
  {product_name}          — nazwa produktu (pierwszego)
  {product_sku}           — SKU produktu (pierwszego)

Note: variable availability depends on the trigger event type.
"""

# ── Main ──────────────────────────────────────────────────────────────────────

def print_template(t_id: int):
    t = TEMPLATES[t_id]
    print(f"\n{'='*70}")
    print(f"TEMPLATE {t_id}: {t['name']}")
    print(f"Trigger: {t['trigger']}")
    print(f"{'='*70}")
    print(f"\nSubject: {t['subject']}")
    print(f"\n--- PL ---\n{t['body_pl']}")
    if "body_de" in t:
        print(f"\n--- DE ---\n{t['body_de']}")
    if "body_en" in t:
        print(f"\n--- EN ---\n{t['body_en']}")


def save_templates(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    for t_id, t in TEMPLATES.items():
        file = output_dir / f"template_{t_id:02d}_{t['name'].replace(' ', '_').replace('/', '-')[:30]}.json"
        file.write_text(json.dumps(t, indent=2, ensure_ascii=False))
        print(f"Saved: {file}")
    # Also save BL variables reference
    (output_dir / "bl_variables_reference.txt").write_text(BL_VARIABLES)
    print(f"Saved: {output_dir}/bl_variables_reference.txt")


def main():
    p = argparse.ArgumentParser(description="BL Email Templates Generator")
    p.add_argument("--template", type=int, choices=TEMPLATES.keys(), help="Print specific template")
    p.add_argument("--save", action="store_true", help="Save templates to data/email_templates/")
    args = p.parse_args()

    if args.template:
        print_template(args.template)
        print(BL_VARIABLES)
    elif args.save:
        out = Path(__file__).parent.parent / "data" / "email_templates"
        save_templates(out)
        print(f"\nAll templates saved to {out}")
    else:
        for t_id in TEMPLATES:
            print_template(t_id)
        print(BL_VARIABLES)


if __name__ == "__main__":
    main()
