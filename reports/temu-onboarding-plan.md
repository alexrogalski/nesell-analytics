# Temu EU Marketplace — Seller Registration & Onboarding Plan

**Date:** 2026-03-16 (updated)
**Prepared by:** Researcher Agent (nesell)
**Task ID:** task_KZAiZmnNHdPA

---

## 1. Summary

- **Baselinker natively supports Temu** — full integration (orders, listings, inventory, prices, shipping). Zero extra subscription cost.
- **Onboarding takes ~7-12 business days** (account approval + listing review). Empik is faster (~1-3 days) but Poland-only.
- **"0% commission" is misleading** — real fees are 2-15% by category. Budget 8-15% total (commission + ads + fulfillment).
- **Semi-managed model** is best for nesell: you ship from Exportivo, you control pricing, Temu handles customer service.
- **EU compliance is the main blocker** — GPSR, EPR registrations must be ready before listing. Reuse existing Amazon compliance data.
- **Temu handles 95%+ of customer service** but has a 90-day return window (vs Amazon's 30 days).

---

## 2. Methodology

- Web research across 30+ sources (March 2026), English and Polish
- Cross-referenced: Temu seller portal, Base.com help center, seller forums, e-commerce news
- Compared with Empik marketplace from official sources (empik.com, shoper.pl, dealavo.com)
- **Limitations:** Temu's fee schedule is not publicly published in full detail; some figures are estimates from seller reports. Temu's policies change frequently. Poland Local Seller Program status unconfirmed.

---

## 3. Registration Requirements

### Required Documents

| Document | Details |
|----------|---------|
| **Business registration** | KRS extract (sp. z o.o.) or CEIDG printout (JDG) |
| **NIP (Tax ID)** | Required for all business types |
| **EU VAT ID** | PL-prefixed VAT number (required for cross-border EU sales) |
| **OSS certificate** | One-Stop Shop VAT — if selling cross-border B2C within EU |
| **Bank account** | PLN or EUR IBAN; supporting docs for account ownership verification |
| **Contact person ID** | Passport or national ID (must be 18+) |
| **Proof of address** | Utility bill or bank statement, issued within last 180 days |
| **Phone number** | Must be from DE, IT, FR, ES, UK, NL, AT, BE, or PL |
| **Store info** | Name, logo (min 800×800 px), contact details |

### EU Compliance (Mandatory)

| Requirement | Status for nesell | Action needed |
|-------------|-------------------|---------------|
| **GPSR** (General Product Safety Regulation) | Already handled for Amazon | Reuse same EU Authorized Representative data |
| **CE marking** | N/A for caps/hats/tote bags | Mugs may need food-contact compliance check |
| **EPR** (Extended Producer Responsibility) | Existing registrations for Amazon | Verify they cover Temu as additional sales channel |
| **OSS** (One-Stop Shop VAT) | Check current status | Needed for cross-border B2C within EU |

> **Warning:** Business location selected at registration determines tax settings and is **non-editable** later. Choose carefully.

### Seller Portal URLs

| Portal | URL |
|--------|-----|
| EU Seller Center | `https://seller-eu.temu.com` |
| EU Partner Platform (API) | `https://partner-eu.temu.com` |
| Contact for invitation | `merchandise@temu.com` |

---

## 4. Seller Models Available

| Model | Pricing Control | Fulfillment | Commission | Best For |
|-------|----------------|-------------|------------|----------|
| **Fully Managed** | Temu sets prices | Temu warehouse | 2-5% | Bulk sellers, passive approach |
| **Semi-Managed** | Seller sets prices | Own logistics (Exportivo/DPD) | 8-15% | **nesell's best fit** |

**Recommendation for nesell:** Semi-managed model — keeps pricing control, allows Exportivo/DPD fulfillment, integrates with existing Baselinker workflow. Temu handles customer service.

---

## 5. Product Feed & Baselinker Integration

### Baselinker (Base.com) Integration — CONFIRMED & PRODUCTION-READY

| Feature | Supported |
|---------|-----------|
| Order download | Yes — from "NOT SENT" status with full address data |
| Product listing | Yes — bulk from warehouse/ERP |
| Inventory sync | Yes — automatic stock levels |
| Price sync | Yes — with multiplier support |
| Shipping/tracking | Yes — tracking numbers forwarded to Temu |
| Multi-region | Yes — separate integration per Temu region |
| Extra cost | None — unlimited Temu accounts |

**Setup:** Base.com panel → Add integration → Temu → Select region → Generate API token in Temu Seller Center → Enter token in Base.com.

**Setup guide:** https://base.com/en-EN/help/knowledgebase/temu-integration-setup/

### Listing Constraints

- Max 2,000 characters description
- Max 6 photos per product (min 800×800 px)
- Primary listing paths: (a) Baselinker bulk upload, (b) Temu Seller Center manual, (c) Partner API

### Official Temu API

- EU API docs: https://partner-eu.temu.com/documentation
- Covers: Product management, Price management, Orders, Fulfillment, Returns/Refunds, Promotions, Webhooks, Ads, Compliance
- OAuth-style authorization flow for ERP systems
- Not fully public — designed for authorized partners
- Documentation quality below Amazon/eBay standard; API changes frequently

---

## 6. Fees & Commission Structure

| Fee Type | Amount | Notes |
|----------|--------|-------|
| Registration | **0 PLN** | Free |
| Listing fees | **0** | No per-listing charges |
| Monthly subscription | **0** | No fixed monthly fee |
| Commission (promo period) | **0%** | Active for EU local sellers, **no announced end date** |
| Commission (semi-managed, steady-state) | **8-15%** | Category-dependent, not publicly published |
| Commission (fully managed) | **2-5%** | Temu controls pricing |
| **Realistic total budget** | **8-15% of revenue** | Commission + potential ads |

> **Warning:** The "0% commission" marketing refers to temporary onboarding promotions. Duration is unspecified and can end without notice. Budget for 8-15% from day one.

### Platform Fee Comparison

| Platform | Commission | Monthly Fee | Pricing Control |
|----------|-----------|-------------|-----------------|
| Amazon | 15.45% | €39/mo | Seller sets price |
| Empik | 2.5-15.5% | 0 PLN | Seller sets price |
| Allegro | ~10% | Varies | Seller sets price |
| **Temu (promo)** | **0%** | **0 PLN** | Semi-managed: yes |
| **Temu (steady-state)** | **8-15%** | **0 PLN** | Semi-managed: yes |

---

## 7. Fulfillment & Shipping

For **semi-managed model** (recommended):
- Seller ships from own warehouse using own carriers
- **Must ship within 48 hours** of order
- Must provide tracking numbers (forwarded via Baselinker)
- Local EU delivery: 1-3 days to customer
- No explicit minimum stock requirement found

**nesell's DPD contract rates:**

| Destination | Rate |
|-------------|------|
| DE | 2.86 EUR |
| FR | 3.52 EUR |
| IT | 3.52 EUR |
| ES | 3.52 EUR |
| NL | 2.86 EUR |
| SE | 5.03 EUR |
| PL | ~2.50 EUR (est.) |
| BE | 2.86 EUR |

---

## 8. Returns & Customer Service

| Policy | Details |
|--------|---------|
| Return window | **90 days** from purchase date (vs Amazon's 30) |
| Return shipping deadline | 14 days from request submission |
| Customer service | **Temu handles centrally** (24/7 chat) |
| Fully managed returns | Temu handles entirely |
| Semi-managed returns | **Seller handles reverse logistics** |
| Low-value items | Temu may refund without requiring physical return |

**Impact for nesell:** With semi-managed model, Exportivo would need to process returns. Confirm Exportivo's return handling capacity and costs. Factor higher return rate into pricing (~5-8% estimated vs Amazon's ~3%).

---

## 9. Timeline: Registration to First Sale

| Phase | Duration | Notes |
|-------|----------|-------|
| Document preparation | 1-3 days | Gather KRS/CEIDG, VAT, ID, bank details |
| Account application | 1 day | Submit via seller-eu.temu.com |
| Automated verification | 1-2 days | OCR + government database check |
| Manual KYC review | 3-5 days | Regional compliance team |
| Final audit + activation | 2-5 days | Category permissions, store URL assigned |
| **Total to first listing** | **~7-12 business days** | |
| First sale after listing | ~20 days median | Temu data: 50% of merchants within 20 days |

**#1 cause of delays:** Inconsistent signatory names across documents (68% of delays). Ensure all docs use exactly the same name spelling.

---

## 10. Temu vs Empik — Onboarding Comparison

| Criterion | Temu EU | Empik Marketplace |
|-----------|---------|-------------------|
| **Access model** | Open (was invite-only until mid-2025) | Open, self-service |
| **Approval time** | 7-12 business days | ~1 business day |
| **Time to first listing** | 7-12 business days | 1-3 business days |
| **Median time to first sale** | ~20 days after listing | Depends on category |
| **Fees (ongoing)** | 2-15% commission | 2.5-15.5% commission |
| **Intro offer** | 0% (unspecified duration) | 0% first 1-3 months + 1000 PLN ad credit |
| **Business age requirement** | Not specified | **Minimum 3 months** |
| **Reputation requirement** | None stated | **4.5+ Google rating** |
| **EAN/GTIN required** | Not explicitly | Yes, mandatory (GS1 verified) |
| **Pricing control** | Semi-managed: yes / Fully-managed: no | Yes |
| **Geographic reach** | **8+ EU countries** | Poland only |
| **Monthly active users** | ~19.7M (PL peak) | ~11M |
| **Fulfillment** | Consignment or self-fulfillment | Seller handles |
| **Customer service** | **Temu handles** | Seller handles (in Polish) |
| **Returns window** | 90 days | 14 days |
| **Language** | English OK | Polish required |
| **Regulatory risk** | Higher (8.8M EUR fine Jan 2026) | Low |

**Verdict:** Temu is slower to onboard but offers pan-EU reach and higher traffic. Empik is faster but Poland-only with stricter seller requirements (3+ months operating, 4.5+ rating). For nesell, Temu is the higher-leverage play given existing Amazon EU presence and Exportivo fulfillment.

---

## 11. Action Plan

### Phase 1: Pre-Registration (Week 1)

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 1.1 | Verify EU VAT ID is active and correct (PL prefix) | **Alex** | ⬜ |
| 1.2 | Prepare KRS/CEIDG extract (fresh, <3 months old) | **Alex** | ⬜ |
| 1.3 | Prepare passport/ID scan of contact person | **Alex** | ⬜ |
| 1.4 | Prepare proof of business address (utility bill <180 days) | **Alex** | ⬜ |
| 1.5 | Confirm bank account for payouts (EUR IBAN preferred) | **Alex** | ⬜ |
| 1.6 | Check existing GPSR compliance — do all products have EU Authorized Representative? | **Alex** | ⬜ |
| 1.7 | Check existing EPR registrations — do they cover Temu as a sales channel? | **Alex** | ⬜ |
| 1.8 | Verify OSS (One-Stop Shop) VAT registration status | **Alex** | ⬜ |
| 1.9 | Prepare store name, logo (min 800×800 px), contact details | **Alex** | ⬜ |

### Phase 2: Registration (Week 2)

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 2.1 | Register at `seller-eu.temu.com` — choose **semi-managed** model | **Alex** | ⬜ |
| 2.2 | Upload all documents — **ensure names match exactly across all docs** | **Alex** | ⬜ |
| 2.3 | Select target markets (start with DE, PL — highest volume) | **Alex** | ⬜ |
| 2.4 | Complete online identity verification | **Alex** | ⬜ |
| 2.5 | Wait for approval (7-12 business days) | — | ⬜ |
| 2.6 | Alternative: email `merchandise@temu.com` if portal registration stalls | **Alex** | Backup |

### Phase 3: Baselinker Setup (During Verification Wait)

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 3.1 | In Base.com panel: Add integration → Temu → Select region | **Developer** | ⬜ |
| 3.2 | Once Temu account approved: generate API token in Temu Seller Center | **Alex** | ⬜ |
| 3.3 | Enter token in Base.com integration settings | **Developer** | ⬜ |
| 3.4 | Configure inventory sync (map Baselinker SKUs → Temu products) | **Developer** | ⬜ |
| 3.5 | Configure price sync (set multiplier if needed vs Amazon pricing) | **Developer** | ⬜ |
| 3.6 | Configure Temu Shipping integration (Exportivo → Temu tracking) | **Developer** | ⬜ |

### Phase 4: Product Launch (Week 3-4)

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 4.1 | Select initial batch: **top 20 dad hats by margin** (>40% margin after Temu fees) | **Researcher** | ⬜ |
| 4.2 | Prepare product photos (min 800×800 px, max 6 per product) | **Alex** | ⬜ |
| 4.3 | Write Temu-optimized titles (max 200 chars, keyword-rich, NO brand prefix, NO model numbers) | **Marketer** | ⬜ |
| 4.4 | Write descriptions (max 2,000 chars) | **Marketer** | ⬜ |
| 4.5 | Set up GPSR info on Temu listings (reuse Amazon data) | **Developer** | ⬜ |
| 4.6 | Bulk upload via Baselinker | **Developer** | ⬜ |
| 4.7 | Verify listings live on Temu storefront | **Developer** | ⬜ |

### Phase 5: Monitor & Scale (Week 5-8)

| # | Action | Owner | Status |
|---|--------|-------|--------|
| 5.1 | Monitor first 50 orders — delivery times, return rate, customer feedback | **Researcher** | ⬜ |
| 5.2 | Confirm Exportivo can handle Temu shipments within 48h SLA | **Alex** | ⬜ |
| 5.3 | Confirm return handling process with Exportivo | **Alex** | ⬜ |
| 5.4 | Track actual commission rate vs estimates | **Business Analyst** | ⬜ |
| 5.5 | Compare Temu unit economics vs Amazon/Empik (revenue per unit after all costs) | **Business Analyst** | ⬜ |
| 5.6 | If first 20 SKUs perform well: expand to full catalog (879 products) | **Alex** | ⬜ |
| 5.7 | Evaluate Temu Ads for visibility boost (only after 0% promo ends) | **Marketer** | ⬜ |
| 5.8 | Monthly review: Temu share of total revenue should not exceed 20% | **Business Analyst** | Ongoing |

### Decision Points

| Decision | When | Criteria |
|----------|------|----------|
| Expand beyond 20 SKUs? | After 50 orders | Return rate <8%, margin >30% after all costs |
| Add Nike socks to Temu? | After 100 orders | Only if pricing allows >25% margin |
| Invest in Temu advertising? | When 0% promo ends | Only if ROAS >3x on test budget |
| Pause Temu entirely? | Any time | If commission >15% AND margin <20% |

---

## 12. Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Commission higher than expected | Medium | Medium | Budget 15% from start; track actuals weekly |
| Temu controls pricing (fully managed) | N/A | — | Use semi-managed model only |
| GPSR non-compliance blocks listing | Medium | High | Audit all products for GPSR before registration |
| EPR gap for new channel | Low | Medium | Confirm with EPR provider that Temu is covered |
| Slow onboarding (>2 weeks) | Medium | Low | Start process early; use wait time for Baselinker setup |
| Return volume higher than other channels | Medium | Medium | Factor 5-8% return rate into pricing; confirm Exportivo capacity |
| 90-day return window impact on cash flow | Medium | Medium | Hold Temu revenue buffer for 90 days before counting as profit |
| Temu ends 0% promo suddenly | High | Medium | Already budgeted at 8-15%; no dependency on 0% |
| Platform instability / regulatory issues | Medium | Medium | Cap Temu at <20% of total revenue for first 6 months |
| Brand dilution from price-focused platform | Low-Medium | Low | Consider separate product selection or branding for Temu |
| Poland Local Seller Program not available | Medium | Low | Register as EU cross-border seller; monitor for PL program launch |

---

## 13. Sources

### Registration & Requirements
1. [AMZPrep — How to Become a Temu Seller (UK/EU)](https://amzprep.com/sell-on-temu-guide/)
2. [OneCart — How to Sell on Temu 2026](https://www.getonecart.com/how-to-sell-on-temu-a-complete-guide-for-new-and-multichannel-sellers/)
3. [LitCommerce — How to Sell on Temu 2026](https://litcommerce.com/blog/how-to-sell-on-temu/)
4. [Marketplace Universe — Temu Opens for EU Sellers](https://marketplace-universe.com/temu-opens-marketplace-eusellers/)
5. [EcommerceNews.eu — Temu European Marketplace](https://ecommercenews.eu/temu-declares-european-marketplace-open/)

### Integration & API
6. [Base.com — Temu Integration Setup](https://base.com/en-EN/help/knowledgebase/temu-integration-setup/)
7. [Base.com — Temu Integration Page](https://base.com/en-US/integrations/temu/)
8. [Base.com — Temu Shipping Integration](https://base.com/en-US/integrations/temu_shipping/)
9. [Temu EU Partner Platform](https://partner-eu.temu.com/)
10. [API2Cart — Temu API Guide](https://api2cart.com/api-technology/temu-api/)

### Fees & Commission
11. [M2E Pro — Temu Seller Fees Guide](https://docs-m2.m2epro.com/docs/temu-seller-fees-a-complete-guide-for-2025/)
12. [Darwynn Fulfillment — Temu Semi-Managed Model](https://www.darwynnfulfillment.com/temu-semi-managed-model-2025/)

### Returns & Fulfillment
13. [Temu Official Return Policy](https://www.temu.com/return-and-refund-policy.html)

### Empik Comparison
14. [vsprint.pl — Jak sprzedawać na Empik Marketplace](https://vsprint.pl/blog/jak-zaczac-sprzedawac-na-empik-marketplace/)
15. [Shoper Learn — Empik Marketplace Guide](https://www.shoper.pl/learn/artykul/empik-marketplace-jak-zaczac-sprzedawac-na-empikplace)
16. [Dealavo — Empik Marketplace Facts](https://dealavo.com/en/empik-marketplace-what-do-you-need-to-know-about-it/)
17. [MarzoMat — Prowizje EmpikPlace 2026](https://www.marzomat.pl/prowizje-empikplace)

### Market Data & News
18. [Base.com — Temu x Base UK Marketplace Growth](https://base.com/en-EN/blog/temu-x-base-powering-the-next-wave-of-uk-marketplace-growth/)
19. [EcommerceNews.eu — Temu 80% EU Sales via Local Warehouses](https://ecommercenews.eu/temu-80-of-european-sales-via-local-warehouses/)
20. [ChannelX — Baselinker's Kevin Steward on Marketplaces](https://channelx.world/2024/06/baselinkers-kevin-steward-talks-marketplaces-tesco-temu-onbuy-debenhams-bq/)
