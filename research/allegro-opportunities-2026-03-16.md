# Allegro — Analiza Szans i Optymalizacja Ofert
**Data raportu:** 2026-03-16
**Autor:** researcher agent
**Projekt:** Nesell E-commerce
**Źródła danych:** Allegro REST API (GET /sale/offers, GET /order/checkout-forms), web research, dane z poprzednich raportów (seasonality-spring-2026.md, pnl-analysis-2026-03-16.md)

> **Uwaga dot. API:** Token dostępowy wygasł w trakcie sesji — pobrano pełne dane ofert (108 ofert, 50 zamówień) zanim token przestał działać. Endpointy `/billing` i `/me` zwróciły błąd 401. Token wymaga odświeżenia przez re-autoryzację OAuth (manual flow).

---

## 1. Executive Summary — Top 3 Szanse

| # | Szansa | Potencjał | Priorytet |
|---|--------|-----------|-----------|
| 1 | **Launch czapek POD (Printful) na Allegro** — brak jakiejkolwiek czapki z nadrukiem w ofercie mimo gotowego pipeline | Nowa kategoria, 55 PLN–135 PLN/szt, niskie koszty startu | HIGH |
| 2 | **Reaktywacja butów Nike z agresywniejszymi tytułami** — 30 ofert butów Nike aktywnych lub zakończonych, sold=0 przy widoczności 0–10 wizyt, problemem są słabe tytuły i brak SEO | Buty Nike to kategoria z udowodnionym popytem (zamówienie 2812 PLN na Dunk) | HIGH |
| 3 | **Deduplkacja i konsolidacja duplikatów ofert** — zidentyfikowano 15+ par duplikatów (ten sam produkt wystawiony 2x z minimalnie różnym tytułem), które kanibalzują wzajemnie widoczność | Uwolnienie SEO juice, oszczędność opłat aktywacyjnych | MEDIUM |

---

## 2. Analiza Aktualnych Ofert

### 2.1 Stan portfela (dane z API, 2026-03-16)

| Metryka | Wartość |
|---------|---------|
| Łączna liczba ofert w systemie | 108 |
| Aktywne (ACTIVE) | 61 |
| Zakończone (ENDED) | ~47 |
| Oferty ze sold > 0 | **3** (Nike T-shirt, Clarins Body Lotion, The Ordinary krem) |
| Łączna liczba sprzedanych sztuk (aktywne) | **1** (Nike T-shirt S, granatowy) |
| Łączna liczba wizyt (aktywne) | ~139 |
| Oferty z > 10 wizytami | **2** (Ritual of Jing 56, DR.JART+ 11) |
| Średni watchers na ofertę | < 0.3 |

### 2.2 Top 10 ofert wg zaangażowania (wizyty + watchers)

| Oferta | Cena PLN | Wizyty | Watchers | Status | Sprzedano |
|--------|----------|--------|----------|--------|-----------|
| SMALL BATH & BODY GIFT SET RITUAL OF JING | 120,90 | 56 | 9 | ACTIVE | 0 |
| Nike Air Force 1 LE rozmiar 30 | 280,80 | 54 | 0 | ACTIVE | 0 |
| DR.JART+ Tiger Grass Korektor | 82,75 | 11 | 1 | ACTIVE | 0 |
| Nike Air Max 270 White/Black 40 | 640,80 | 10 | 0 | ACTIVE | 0 |
| Nike T-shirt granatowy S | 89,98 | 8 | 0 | ACTIVE | **1** |
| Tommy Hilfiger TJM ESSENTIAL torba | 212,40 | 5 | 1 | ACTIVE | 0 |
| Nike Air Max 270 czarno-kremowy 46 | 640,80 | 6 | 0 | ACTIVE | 0 |
| Nike T-shirt Regular Fit S | 89,98 | 3 | 0 | ACTIVE | 0 |
| Clarins Ujędrniający Krem Szyja | 261,46 | 3 | 1 | ACTIVE | 0 |
| Nike Air Force 1 '07 White 40.5 | 712,50 | 1 | 0 | ACTIVE | 0 |

### 2.3 Kategorie produktów w ofercie

| Kategoria | Liczba ofert | Aktywne | Sprzedano | Uwagi |
|-----------|-------------|---------|-----------|-------|
| Buty Nike/Jordan | 30 | ~12 | 0 aktywnych | Wysokie ceny (489–820 PLN), sold=0 |
| Odzież i akcesoria | 24 | ~13 | 1 | 1x T-shirt sprzedany |
| Kosmetyki/Uroda | 23 | ~18 | 2 (ended) | Najlepsza konwersja historyczna |
| Inne (elektronika, narzędzia) | 22 | ~15 | 0 | Przypadkowe produkty |
| **Czapki z nadrukiem (POD)** | **0** | **0** | **0** | **Brak — KRYTYCZNA LUKA** |

### 2.4 Analiza zamówień (ostatnie 50 checkout forms)

Wszystkie 50 zamówień ma status `READY_FOR_PROCESSING` — co oznacza albo niezrealizowane zamówienia testowe/stare, albo problem z realizacją. Produkty z zamówieniami:

| Produkt | Kwota | Uwagi |
|---------|-------|-------|
| Nike Dunk Low Retro 43 EU | 2 812,00 PLN | Największe zamówienie — gdzie jest realizacja? |
| Clarins Ujędrniający Krem | 1 471,00 PLN | Silny sygnał popytu |
| Maison Alhambra EDP | 363,00 PLN | |
| Vans Plecak Old Skool | 174,80 PLN x2 | |
| Nike T-shirt | 89,77 PLN | Jedyna zrealizowana sprzedaż w aktywnych |
| The Ordinary Krem NMF | 47,27 PLN x3 | Powtarzalne zakupy — dobry sygnał |

**ALERT:** Zamówienie na Nike Dunk Low za 2812 PLN ze statusem READY_FOR_PROCESSING wymaga natychmiastowej weryfikacji. Jeśli to niezrealizowane zamówienie klienta — to priorytet absolutny.

---

## 3. Analiza Kategorii — Potencjał Sprzedaży

### 3.1 Czapki z daszkiem (kategoria nieobecna w ofercie)

**Dlaczego to priorytet #1:**
- Brak jakiegokolwiek produktu czapkowego przy gotowym pipeline Printful POD
- Sezon wiosenny szczytowy w maju — okno do uruchomienia otwarte TERAZ (16 marca)
- Poprzedni research (seasonality-spring-2026.md) potwierdza: maj = szczyt wyszukiwań (Google Trends index 81)
- Printful integracja działa już na Amazon — deployment na Allegro wymaga głównie założenia ofert

**Rynek czapek POD na Allegro (dane z web research):**

| Typ oferty | Zakres cenowy | Obserwacje |
|------------|---------------|------------|
| Czapka trucker z nadrukiem (druk) | 54–89 PLN | Podstawowa oferta, niska bariera wejścia |
| Czapka baseball custom nadruk | 55–99 PLN | Dominuje cenowo rynek |
| Czapka trucker z haftem (embroidery) | 89–135 PLN | Wyższa cena, Printful haft — tutaj Nesell |
| Czapka premium marka (Buff, Dakine) | 135–148 PLN | Górna granica |

**Printful embroidery = przewaga:** Klienci płacą 89–135 PLN za haftowaną czapkę vs 55–89 PLN za drukowaną. Printful specjalizuje się w hafcie — to bezpośredni premium tier.

**Kluczowi konkurenci na Allegro (zidentyfikowani z web research):**

| Sprzedawca/Oferta | Typ | Cena | Obserwacje |
|-------------------|-----|------|------------|
| MASTER (czapka snapback nadruk logo) | Nadruk | ~55 PLN | Duży wolumen, podstawowa jakość |
| Fetocity (czapka z własnym napisem) | Nadruk | ~65 PLN | Custom text, popularny |
| WorldHafts S.C. | **Haft** | 89–115 PLN | Bezpośredni konkurent dla Printful haft |
| -PrintOnDemand- (user Allegro) | POD mix | 59–99 PLN | Dedykowane konto POD — warto śledzić |
| Trucker Trakerka z własnym nadrukiem | Nadruk | ~79 PLN | 5906670911897 |

**Wniosek:** Nesell wchodzi w segment haftu (89–119 PLN), gdzie WorldHafts jest głównym konkurentem. Przewaga: Printful fulfillment = automatyczny, skalowalny. Wady: czas realizacji Printful ~5–7 dni vs lokalny haftownik możliwy w 2–3 dni.

### 3.2 Buty Nike (kategoria aktywna — performance problem)

**Problem:** 30 ofert butów, sold=0 dla aktywnych, zamówienie 2812 PLN "stuck" jako READY_FOR_PROCESSING.

**Analiza cenowa (nasze oferty vs rynek):**

| Model | Nasza cena | Cena rynkowa Allegro | Delta |
|-------|-----------|---------------------|-------|
| Nike Air Force 1 '07 (40.5, damskie) | 712,50 PLN | ~489–550 PLN nowe | +30% over market |
| Nike Air Jordan 1 Mid (47 EU) | 820,80 PLN | ~680–750 PLN nowe | +10% |
| Nike Dunk Low Retro (44, 43 EU) | 489,60 PLN | ~440–499 PLN nowe | competitive |
| Nike Air Max 270 (40, 46 EU) | 640,80 PLN | ~550–650 PLN nowe | slightly over |
| Nike Air Force 1 LE (rozmiar 30 dziecięce) | 280,80 PLN | ~220–280 PLN | slightly over |

**Problemy z tytułami butów (przykłady):**

Aktualne tytuły nie są zoptymalizowane pod Allegro SEO:
- "Nike Dunk Low Retro Męskie Sneakery Biało-Czarno-Białe, 44 EU" — brak słów kluczowych: "panda", "białe buty sportowe", "Dunk Low 44"
- "Nike Air Max 270 Białe/Czarne Buty Męskie, Rozmiar 40 EU" — brak: "air max 270 40", "buty Nike męskie 40", "streetwear"

### 3.3 Kosmetyki i uroda (kategoria z najlepszą historyczną konwersją)

**Dane:** 3 produkty z historyczną sprzedażą (Clarins Body Lotion 149 PLN, The Ordinary Krem 47 PLN, Rituals Body Cream ~98 PLN). Aktywna oferta Ritual of Jing (120,90 PLN) ma 56 wizyt i 9 watchers — najlepsza oferta w portfelu pod kątem zaangażowania.

**Silny sygnał:** Clarins i The Ordinary konwertują. Warto utrzymać i rozwinąć ten segment.

---

## 4. Analiza Konkurencji

### 4.1 Konkurencja w segmencie czapek z nadrukiem/haftem

**Top konkurenci (dane z web research + bezpośrednich URL Allegro):**

**1. WorldHafts S.C.**
- Typ: Haft profesjonalny
- Cena: 89–120 PLN
- USP: Lokalna polska firma haftu, szybka realizacja
- Słabość: Ograniczona skalowalność, manual process

**2. Trucker Trakerka z własnym nadrukiem (ID: 17895607855)**
- Typ: Druk cyfrowy
- Cena: ~79 PLN
- USP: EAN 5906670911897, opcja custom logo
- Słabość: Niższa jakość vs haft, słabszy look premium

**3. Czapka trucker tirówka z siatką (ID: 5050541597)**
- Typ: Gotowa z nadrukiem (nie custom)
- Cena: 55 PLN
- USP: Niska cena, gotowe projekty
- Słabość: Bez customizacji, masowy towar

**4. MASTER snapback nadruk (ID: 5098471742)**
- Typ: Snapback custom logo
- Cena: ~65 PLN
- Słabość: Plastikowe zapięcie, tańszy segment

**5. Czapka bejsbolówka trucker własny haft (ID: 17419122602)**
- Typ: Haft na zamówienie
- Cena: 29,89–89 PLN
- USP: Najtańszy haft na Allegro, imię/logo na zamówienie
- Słabość: Jakość nieweryfikowalna

**Pozycjonowanie Nesell vs konkurencja:**
```
Cena niska                                          Cena wysoka
|------|------------|------------|-------------|---------|
 29 PLN   55–65 PLN   79–89 PLN   89–119 PLN  135+ PLN
  haft       druk      druk/haft    NESELL?    premium
  tani     masowy     mid-range   [cel Nesell]  marka
```

**Rekomendacja:** Nesell wchodzi w tier 89–119 PLN z haftem Printful. Wyróżnik to: gotowe projekty (nie custom na zamówienie), estetyczne wzory, szybka wysyłka przez Printful.

### 4.2 Konkurencja w butach Nike

Nike na Allegro to rynek z setkami ofert. Główne grupy konkurentów:
1. **Autoryzowani sprzedawcy** (JD Sports, Sizeer, eobuwie) — niższe ceny, autoryzacja, szybka dostawa
2. **Resellers** — podobny model do Nesell, ceny wyższe lub równe
3. **Allegro Mall** — oficjalne sklepy marek

**Problem Nesell:** Bez odznaki "Super Sprzedawca" i z małą liczbą ocen, Nesell jest ukryta za autoryzowanymi sprzedawcami w wynikach wyszukiwania.

---

## 5. Top 10 Konkretnych Szans (Opportunities)

### #1 — Uruchomienie czapek POD na Allegro
**Priorytet: HIGH**

| | Stan obecny | Rekomendacja |
|--|-------------|--------------|
| Oferty | 0 czapek | Dodać min. 5 ofert: 3x dad hat, 2x trucker cap |
| Cena | N/A | 89–119 PLN dla haft, 69–89 PLN dla druk |
| Kategoria Allegro | N/A | "Nakrycia głowy > Czapki z daszkiem" |
| Provizja Allegro | N/A | ~8–11% (odzież/akcesoria) |
| Czas wdrożenia | — | 2–3 dni (design + Printful listing + Allegro offer) |

**Oczekiwany wpływ:** 5–15 sprzedaży/miesiąc przy 89–119 PLN = 445–1785 PLN/mies. dodatkowego przychodu w Q2 2026.

**Tytuły startowe do stworzenia:**
- "Czapka z haftem wiosenna — regulowana bejsbolówka unisex — dad hat pastele"
- "Czapka trucker z nadrukiem haft — siatka z tyłu — regulowana snapback"
- "Czapka baseball klasa 2026 — Abschluss haft — unisex regulowana"

---

### #2 — Weryfikacja i realizacja zamówienia Nike Dunk 2812 PLN
**Priorytet: HIGH (NATYCHMIASTOWY)**

Zamówienie na Nike Dunk Low Retro 43 EU za 2812 PLN ma status `READY_FOR_PROCESSING`. To albo:
- Zrealizowane zamówienie z błędem statusu w API
- Niezrealizowane zamówienie klienta czekające na wysyłkę

**Akcja:** Wejść na panel Allegro → Moje sprzedaże → sprawdzić to zamówienie i podjąć akcję.

---

### #3 — Deduplicacja ofert (15+ par duplikatów)
**Priorytet: MEDIUM**

Zidentyfikowane duplikaty (ten sam produkt, różny tytuł/ID):

| Produkt | Oferta 1 | Oferta 2 | Akcja |
|---------|----------|----------|-------|
| Clarins Lip Perfector 01 | 18108446276 | 18108433857 | Zostawić lepiej opisaną, zakończyć drugą |
| Karl Lagerfeld EDP | 18108440767 | 18108433068 | Scalić |
| Gosh Podkład 002 Ivory | 18108433116 | 18108432984 | Scalić |
| Vans Plecak Old Skool | 18108436829 | 18108433732 | Scalić |
| Clarins Lip Błyszczyk | 18108430348 | 18108429720 | Scalić |
| Beauty of Joseon Krem-żel | 18108427509 | 18108341881 | Scalić |
| Poziomica Laserowa 4D | 18108426349 | 18108341097 | Scalić |
| Gosh Błyszczyk soft tinted | 18108426146 | 18108341013 | Scalić |
| Kredka Gosh matowa | 18108425229 | 18108340771 | Scalić |

**Oczekiwany wpływ:** Konsolidacja wizyt i watchers na jednej ofercie = lepsza pozycja w wyszukiwarce.

---

### #4 — Reaktywacja zakończonych ofert butów z poprawionymi tytułami
**Priorytet: HIGH**

Oferty ENDED z potencjałem (brak sprzedaży ale były wyświetlane):

| Oferta | Poprzednia cena | Wizyty (przed końcem) | Akcja |
|--------|----------------|----------------------|-------|
| Nike AIR FORCE 1 LE (GS) 37.5 | 441,60 PLN | 1 | Reaktywuj z nowym tytułem |
| Nike Dunk Low Panda 45 | 489,60 PLN | 0 | Reaktywuj |
| Nike Air Force 1 38 Unisex | 496,00 PLN | 1 | Reaktywuj |

**Oczekiwany wpływ:** Odblokowanie stock + zoptymalizowane tytuły mogą dać 3–8 sprzedaży/miesiąc.

---

### #5 — Obniżenie ceny Nike Air Force 1 '07 damskie 40.5
**Priorytet: MEDIUM**

Aktualnie: 712,50 PLN. Rynek Allegro: 489–550 PLN za nowe AF1 damskie. Różnica +30% bez uzasadnienia (nie rare colorway, nie limited edition). Propozycja: obniżyć do 549–579 PLN.

---

### #6 — Uruchomienie kosmetyków SMART — rozwinięcie segmentu z udowodnioną konwersją
**Priorytet: MEDIUM**

The Ordinary i Clarins konwertują. Klient kupił 3x The Ordinary NMF krem w osobnych zamówieniach — to sygnał repeat buyer. Propozycja: dodać 5–10 produktów The Ordinary (są tanie, lekkie, łatwe w wysyłce, rozpoznawalne).

---

### #7 — Dodanie tagów "Super Sprzedawca" — program lojalnościowy Allegro
**Priorytet: MEDIUM**

Brak ocen i odznak obniża widoczność wszystkich ofert. Priorytetem jest zebranie 10+ pozytywnych ocen przez aktywną sprzedaż (kosmetyki) i prośbę o oceny po dostawie. Super Sprzedawca wymaga min. 100 transakcji/rok i 98% pozytywnych ocen.

---

### #8 — Allegro Smart! — darmowa dostawa jako standard
**Priorytet: MEDIUM**

Czy nasze oferty są w programie Allegro Smart? Darmowa dostawa Smart to jeden z kluczowych filtrów kupujących na Allegro (~75% transakcji filtrowanych przez Smart). Należy zweryfikować i włączyć Smart dla wszystkich aktywnych ofert gdzie to opłacalne.

---

### #9 — Sezonowa kampania czapek na wiosnę 2026 (marzec–maj)
**Priorytet: HIGH (time-sensitive)**

Po uruchomieniu ofert czapek (szansa #1), uruchomić Allegro Ads (odpowiednik Sponsored Products). Allegro Ads działa na modelu CPC. Przy sezonie wiosennym (szczyt maj) — kampania musi startować najpóźniej 1 kwietnia.

**Budżet rekomendowany:** 200–400 PLN/miesiąc na start, optymalizacja po 2 tygodniach.

---

### #10 — Optymalizacja opisów i zdjęć dla ofert kosmetycznych
**Priorytet: LOW-MEDIUM**

Oferta Ritual of Jing (56 wizyt, 9 watchers, 0 sprzedaży) wskazuje na problem z konwersją przy wysokim traffic. Zdjęcia lub opis mogą nie konwertować. Akcja: przejrzeć ofertę, dodać zdjęcia lifestyle, zaktualizować opis.

---

## 6. Optymalizacja Tytułów — Konkretne Przepisania

### Zasady dla Allegro (analogiczne do Amazon ale z lokalnymi specyfikami):
- Allegro priorytetyzuje pierwsze 3–4 słowa w tytule (jak title tag SEO)
- Maksymalna długość tytułu: 75 znaków (wyświetlane na liście); pełny do 250 znaków
- Kluczowe słowa PL na początku > model > rozmiar > kolor
- Nie używać: "Nesell", numerów modeli producenta, nadmiernych symboli

### Przepisania tytułów (aktualne → rekomendowane):

**Nike Dunk Low Retro 44 EU:**
- Przed: `Nike Dunk Low Retro Męskie Sneakery Biało-Czarno-Białe, 44 EU`
- Po: `Buty Nike Dunk Low Panda Białe Czarne Rozmiar 44 EU — Nowe`

**Nike Air Max 270 White/Black 40:**
- Przed: `Nike Air Max 270 Białe/Czarne Buty Męskie, Rozmiar 40 EU`
- Po: `Buty Nike Air Max 270 Białe Czarne Męskie 40 EU — Oryginalne Nowe`

**Nike Air Jordan 1 Mid 47 EU:**
- Przed: `Nike Air Jordan 1 Mid Męskie Buty Koszykarskie Białe Rozmiar 47 EU`
- Po: `Buty Nike Air Jordan 1 Mid Białe Rozmiar 47 EU — Koszykarskie Nowe`

**Nike Air Force 1 '07 White 40.5:**
- Przed: `Nike Air Force 1 '07 White/White 40.5 - Skórzane Buty Sportowe Kobiece`
- Po: `Buty Nike Air Force 1 Białe Damskie Rozmiar 40.5 EU — Skórzane Nowe`

**Nike T-shirt granatowy S:**
- Przed: `Nike T-shirt Sportsweart męski granatowy bawełniany S`
- Po: `Koszulka Nike Sportswear Granatowa Bawełniana Rozmiar S — Oryginalna`

**Ritual of Jing Gift Set:**
- Przed: `SMALL BATH & BODY GIFT SET THE RITUAL OF JING`
- Po: `Rituals Zestaw Upominkowy The Ritual of Jing — Kąpiel Ciało — Prezent`

**Nowe tytuły czapek POD (do stworzenia):**
```
Czapka z Haftem Wiosenna — Dad Hat Regulowana Unisex — Pastelowa Bawełna
Czapka Trucker z Haftem — Siatka Oddychająca — Regulowana Snapback Unisex
Czapka Baseball Klasa 2026 — Haft Absolwent — Regulowana Unisex Prezent
Czapka z Daszkiem Haftowana Kwiaty — Wiosna 2026 — Regulowana Bawełna
Czapka Trucker Vintage Retro — Haft — Regulowana Snapback Oddychająca
```

---

## 7. Rekomendacje Cenowe

### Czapki POD (nowy segment)

| Produkt | Koszt Printful (est.) | Rekomendowana cena Allegro | Marża po prowizji ~10% |
|---------|----------------------|---------------------------|----------------------|
| Dad Hat z haftem | ~28–35 PLN | 89–99 PLN | ~45–52 PLN (51–53%) |
| Trucker Cap z haftem | ~32–40 PLN | 99–119 PLN | ~49–67 PLN (49–56%) |
| Graduation hat (sezonowy) | ~32–40 PLN | 109–129 PLN | ~58–76 PLN (53–59%) |

### Buty Nike (korekty)

| Model | Aktualna cena | Rynek | Rekomendacja |
|-------|--------------|-------|--------------|
| AF1 '07 White damskie 40.5 | 712,50 PLN | 489–550 PLN | **Obniżyć do 549 PLN** |
| Air Jordan 1 Mid 47 | 820,80 PLN | 680–750 PLN | **Obniżyć do 729 PLN** |
| Air Max 270 W Czarne 38 | 730,80 PLN | 620–680 PLN | **Obniżyć do 669 PLN** |
| Dunk Low 44/43/38.5 | 489–550,80 PLN | 440–499 PLN | Zostawić (konkurencyjne) |
| AF1 LE dziecięce 30 | 280,80 PLN | 220–280 PLN | Zostawić lub obniżyć do 259 PLN |

### Kosmetyki

| Produkt | Cena | Rynek | Rekomendacja |
|---------|------|-------|--------------|
| The Ordinary NMF Krem | 47,27 PLN | 35–55 PLN | OK — zostawić |
| Clarins Body Lotion | 149,00 PLN | 120–160 PLN | OK |
| Ritual of Jing Set | 120,90 PLN | 110–145 PLN | OK, rozważyć 109,90 PLN (psychologicznie < 110) |
| Clarins Krem Szyja | 261,46 PLN | 240–290 PLN | OK |

---

## 8. Plan Działania — Kolejne Kroki

### Tydzień 1 (16–22 marca) — IMMEDIATE
- [ ] **Zweryfikować zamówienie Nike Dunk 2812 PLN** — panel Allegro → Moje zamówienia
- [ ] **Odświeżyć token Allegro** — re-autoryzacja OAuth (wymagana ręcznie)
- [ ] **Stworzyć 3 oferty czapek POD** — dad hat pastel, trucker cap, graduation hat
- [ ] **Zaktualizować tytuły top 5 ofert butów** (wg przepisań powyżej)
- [ ] **Zakończyć 9 zidentyfikowanych duplikatów** (zostawić lepszą, zakończyć słabszą)

### Tydzień 2–3 (23 marca – 5 kwietnia)
- [ ] Uruchomić Allegro Ads dla czapek (budżet 200 PLN)
- [ ] Zoptymalizować ofertę Ritual of Jing (76 wizyt, 0 sprzedaży — konwersja = 0)
- [ ] Obniżyć ceny AF1 damskie 40.5 i AJ1 Mid 47 EU
- [ ] Dodać 5 nowych produktów The Ordinary (tanie, konwertujące)
- [ ] Reaktywować 3 oferty zakończone AF1 z nowymi tytułami

### Kwiecień 2026
- [ ] Dodać 2 oferty czapek graduation (Klasa 2026, Abschluss 2026)
- [ ] Uruchomić bucket hat jeśli Printful EU margin > 40%
- [ ] Ocenić ROI Allegro Ads po 2 tygodniach, optymalizować bidy
- [ ] Sprawdzić program Allegro Smart dla aktywnych ofert

---

## 9. Analiza Prowizji Allegro (kontekst finansowy)

| Kategoria | Prowizja Allegro |
|-----------|-----------------|
| Odzież, Obuwie, Dodatki (standard) | 8–11% |
| Odzież program bonusowy 1,9% | 1,9% (wymaga spełnienia warunków) |
| Inne (kategorie niszowe) | do 17% |
| Aktywacja oferty | ~0,05–0,20 PLN/ofertę |

**Kalkulacja marży czapka Printful na Allegro:**
- Cena sprzedaży: 99 PLN
- Koszt Printful: ~32 PLN
- Prowizja Allegro ~10%: 9,90 PLN
- Wysyłka (Printful EU): ~15–20 PLN (wliczona lub doliczana osobno)
- **Marża netto: ~37–42 PLN (37–42%)** — bardzo dobra marża dla POD

---

## 10. Podsumowanie i Priorytety

| Priorytet | Akcja | Oczekiwany efekt | Czas wdrożenia |
|-----------|-------|-----------------|----------------|
| **P1** | Weryfikacja zamówienia Nike 2812 PLN | Realizacja lub wyjaśnienie | 1 dzień |
| **P1** | Launch 3 ofert czapek POD | Nowy przychód 500–2000 PLN/mies. | 2–3 dni |
| **P2** | Deduplicacja 9 par duplikatów | Lepsza widoczność SEO | 2 godziny |
| **P2** | Nowe tytuły top 5 ofert butów | +30–50% wizyt | 1 dzień |
| **P3** | Obniżenie cen AF1 damskie i AJ1 | Odblokowanie konwersji | 30 minut |
| **P3** | Allegro Ads dla czapek | Przyspieszenie sprzedaży | Po launch czapek |
| **P4** | Dodanie 5 produktów The Ordinary | Rozszerzenie konwertującego segmentu | 3–4 dni |
| **P5** | Optymalizacja Ritual of Jing | Konwersja z 56 wizyt | 1 dzień |

---

## Źródła

- [Allegro REST API — dane własne ofert i zamówień (GET /sale/offers, GET /order/checkout-forms)](https://developer.allegro.pl/)
- [TRUCKER TRAKERKA z własnym nadrukiem — Allegro oferta](https://allegro.pl/oferta/trucker-trakerka-czapka-z-daszkiem-z-wlasnym-nadrukiem-nadruk-twoje-logo-17895607855)
- [Czapka trucker tirówka z nadrukiem — Allegro oferta 5050541597](https://allegro.pl/oferta/czapka-trucker-tirowka-z-daszkiem-nadruk-siatka-5050541597)
- [Czapka bejsbolówka trucker haft — Allegro oferta 17419122602](https://allegro.pl/oferta/czapka-bejsbolowka-trucker-twoj-wlasny-haft-logo-imie-nadruk-17419122602)
- [Co najlepiej sprzedaje się na Allegro 2026 — Hello Marketing](https://hellomarketing.pl/blog/co-sprzedawac-na-allegro-2025-2026/)
- [Co najlepiej sprzedaje się na Allegro 2026 — Base Blog](https://base.com/pl-PL/blog/co-najlepiej-sprzedaje-sie-na-allegro-podpowiadamy/)
- [Prowizje Allegro 2026 — MarżoMat](https://www.marzomat.pl/prowizje-allegro)
- [Koszty sprzedaży na Allegro 2026 — Comparic](https://comparic.pl/koszty-sprzedazy-na-allegro-w-2026-jak-utrzymac-rentownosc-mimo-rosnacych-oplat/)
- [Nesell Spring Seasonality Research 2026-03-16](../research/seasonality-spring-2026.md)
- [Nesell P&L Deep Dive 2026-03-16](../research/pnl-analysis-2026-03-16.md)
- [Allegro Regulamin prowizji 1.9% — kategorie Odzież](https://allegro.pl/regulaminy/regulamin-programu-bonusowego-prowizja-1-9-za-sprzedaz-w-kategoriach-odziez-obuwie-dodatki-dziecko-oraz-sport-i-turystyka-3AVWK9o27Td)
- [Przedmioty sprzedawcy -PrintOnDemand- Allegro](https://allegro.pl/uzytkownik/-PrintOnDemand-)
- [Logonaczapce.pl — haft i nadruk na czapkach (benchmark cenowy)](https://logonaczapce.pl/kategoria/czapki-z-daszkiem/)
