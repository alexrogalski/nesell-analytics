"""Amazon SP-API direct API calls: Inventory, Catalog, Pricing, Sales.

These use real-time APIs (not reports) for current snapshots.
"""
import time
from datetime import datetime, timedelta
from . import amazon_api, db, config


# ── FBA Inventory API (real-time stock levels) ───────────────────────

def sync_inventory_api(conn):
    """Fetch real-time FBA inventory via Inventory API.

    GET /fba/inventory/v1/summaries — real-time stock levels per SKU.
    Rate limit: 2 req/sec, burst 2.
    """
    print("  [Inventory API] Fetching real-time FBA inventory...")
    all_items = []
    next_token = None
    marketplace_ids = ",".join(amazon_api.ALL_EU_MARKETPLACE_IDS)

    while True:
        params = {
            "details": "true",
            "granularityType": "Marketplace",
            "granularityId": amazon_api.ALL_EU_MARKETPLACE_IDS[0],  # primary marketplace
            "marketplaceIds": marketplace_ids,
        }
        if next_token:
            params["nextToken"] = next_token

        data = amazon_api.api_get("/fba/inventory/v1/summaries", params)
        payload = data.get("payload", {})
        summaries = payload.get("inventorySummaries", [])
        all_items.extend(summaries)

        next_token = payload.get("pagination", {}).get("nextToken")
        if not next_token:
            break
        time.sleep(0.5)

    print(f"  [Inventory API] Got {len(all_items)} inventory items")

    records = []
    for item in all_items:
        inv = item.get("inventoryDetails", {})
        records.append({
            "sku": item.get("sellerSku", ""),
            "fnsku": item.get("fnSku", ""),
            "asin": item.get("asin", ""),
            "product_name": (item.get("productName") or "")[:200],
            "fulfillable_qty": inv.get("fulfillableQuantity", 0) or 0,
            "inbound_working_qty": inv.get("inboundWorkingQuantity", 0) or 0,
            "inbound_shipped_qty": inv.get("inboundShippedQuantity", 0) or 0,
            "inbound_receiving_qty": inv.get("inboundReceivingQuantity", 0) or 0,
            "reserved_qty": (inv.get("reservedQuantity", {}) or {}).get("totalReservedQuantity", 0) or 0,
            "unfulfillable_qty": (inv.get("unfulfillableQuantity", {}) or {}).get("totalUnfulfillableQuantity", 0) or 0,
            "total_qty": item.get("totalQuantity", 0) or 0,
        })

    if records:
        count = db.upsert_amazon_inventory(conn, records)
        print(f"  [Inventory API] Upserted {count} inventory records")
    return len(records)


# ── Catalog Items API (BSR snapshots) ────────────────────────────────

def _get_our_asins(conn):
    """Get list of ASINs from our products and orders."""
    # From products table
    products = db._get("products", {"select": "asin", "asin": "not.is.null"})
    asins = {p["asin"] for p in products if p.get("asin")}

    # From order_items table (last 90 days)
    items = db._get("order_items", {
        "select": "asin",
        "asin": "not.is.null",
        "limit": "5000",
        "order": "created_at.desc",
    })
    for it in items:
        if it.get("asin"):
            asins.add(it["asin"])

    return list(asins)


def sync_bsr(conn):
    """Fetch BSR (Best Sellers Rank) for our ASINs via Catalog Items API.

    GET /catalog/2022-04-01/items/{asin} — includes salesRanks.
    Rate limit: 2 req/sec, burst 2.
    """
    print("  [BSR] Fetching Best Sellers Rank snapshots...")
    asins = _get_our_asins(conn)
    if not asins:
        print("    No ASINs found")
        return 0

    print(f"    Found {len(asins)} ASINs to check")

    records = []
    for idx, asin in enumerate(asins):
        # Fetch from primary marketplace first
        for mkt_id in amazon_api.ALL_EU_MARKETPLACE_IDS[:3]:  # Top 3 marketplaces
            data = amazon_api.api_get(
                f"/catalog/2022-04-01/items/{asin}",
                params={
                    "marketplaceIds": mkt_id,
                    "includedData": "salesRanks,dimensions,summaries",
                },
            )

            # Parse sales ranks
            for rank_list in data.get("salesRanks", []):
                for rank in rank_list.get("classificationRanks", []):
                    records.append({
                        "asin": asin,
                        "marketplace_id": mkt_id,
                        "bsr_rank": rank.get("rank"),
                        "category_id": rank.get("classificationId", ""),
                        "category_name": rank.get("title", ""),
                    })
                for rank in rank_list.get("displayGroupRanks", []):
                    records.append({
                        "asin": asin,
                        "marketplace_id": mkt_id,
                        "bsr_rank": rank.get("rank"),
                        "category_id": rank.get("websiteDisplayGroup", ""),
                        "category_name": rank.get("title", ""),
                    })

            time.sleep(0.6)  # respect 2 req/sec limit

        if (idx + 1) % 50 == 0:
            print(f"    BSR progress: {idx+1}/{len(asins)}")

    if records:
        count = db.upsert_amazon_bsr(conn, records)
        print(f"  [BSR] Upserted {count} BSR records")
    return len(records)


# ── Product Pricing API (competitive pricing) ────────────────────────

def sync_pricing(conn):
    """Fetch competitive pricing for our ASINs.

    POST /batches/products/pricing/2022-05-01/items/competitiveSummary
    Batch up to 20 ASINs per request. Rate limit: 0.5 req/sec.
    """
    print("  [Pricing] Fetching competitive pricing...")
    asins = _get_our_asins(conn)
    if not asins:
        print("    No ASINs found")
        return 0

    print(f"    Found {len(asins)} ASINs to price-check")

    records = []
    # Process in batches of 20 per marketplace
    for mkt_id in amazon_api.ALL_EU_MARKETPLACE_IDS[:3]:  # Top 3 marketplaces
        plat_code = config.MARKETPLACE_TO_PLATFORM.get(mkt_id, "")

        for i in range(0, len(asins), 20):
            batch = asins[i:i+20]
            requests_body = []
            for asin in batch:
                requests_body.append({
                    "asin": asin,
                    "marketplaceId": mkt_id,
                    "includedData": ["featuredBuyingOptions", "lowestPricedOffers", "referencePrices"],
                    "method": "GET",
                    "uri": f"/products/pricing/2022-05-01/items/{asin}/competitiveSummary",
                })

            data = amazon_api.api_post(
                "/batches/products/pricing/2022-05-01/items/competitiveSummary",
                body={"requests": requests_body},
            )

            for resp in data.get("responses", []):
                body = resp.get("body", {})
                asin = body.get("asin", "")
                if not asin:
                    continue

                # Parse featured buying option (Buy Box)
                buy_box_price = None
                buy_box_shipping = None
                buy_box_landed = None
                for opt in body.get("featuredBuyingOptions", []):
                    listing_price = opt.get("listingPrice", {})
                    shipping = opt.get("shippingPrice", {})
                    buy_box_price = float(listing_price.get("amount", 0) or 0)
                    buy_box_shipping = float(shipping.get("amount", 0) or 0)
                    buy_box_landed = buy_box_price + buy_box_shipping
                    currency = listing_price.get("currencyCode", "EUR")
                    break

                # Parse lowest offers
                lowest_fba = None
                lowest_fbm = None
                num_new = 0
                num_used = 0
                for offer in body.get("lowestPricedOffers", []):
                    condition = offer.get("condition", "")
                    if condition != "new":
                        num_used += offer.get("numberOfOffers", 0) or 0
                        continue
                    num_new += offer.get("numberOfOffers", 0) or 0
                    price_val = float(offer.get("price", {}).get("listingPrice", {}).get("amount", 0) or 0)
                    fulfillment = offer.get("fulfillmentType", "")
                    if fulfillment == "Amazon" and (lowest_fba is None or price_val < lowest_fba):
                        lowest_fba = price_val
                    elif fulfillment == "Merchant" and (lowest_fbm is None or price_val < lowest_fbm):
                        lowest_fbm = price_val

                # Parse list price
                list_price = None
                for ref in body.get("referencePrices", []):
                    if ref.get("type") == "LIST_PRICE":
                        list_price = float(ref.get("price", {}).get("amount", 0) or 0)

                records.append({
                    "asin": asin,
                    "marketplace_id": mkt_id,
                    "buy_box_price": buy_box_price,
                    "buy_box_shipping": buy_box_shipping,
                    "buy_box_landed_price": buy_box_landed,
                    "lowest_fba_price": lowest_fba,
                    "lowest_fbm_price": lowest_fbm,
                    "num_offers_new": num_new,
                    "num_offers_used": num_used,
                    "list_price": list_price,
                    "currency": currency if buy_box_price else "EUR",
                })

            time.sleep(2)  # 0.5 req/sec rate limit

        print(f"    {plat_code}: {len(records)} pricing records")

    if records:
        count = db.upsert_amazon_pricing(conn, records)
        print(f"  [Pricing] Upserted {count} pricing records")
    return len(records)


# ── Sales API (aggregated metrics) ───────────────────────────────────

def sync_sales_metrics(conn, days_back=30):
    """Fetch aggregated sales metrics via Sales API.

    GET /sales/v1/orderMetrics — daily aggregated sales data.
    Rate limit: 0.5 req/sec, burst 15.
    """
    print("  [Sales] Fetching aggregated sales metrics...")
    start = (datetime.utcnow() - timedelta(days=days_back)).strftime("%Y-%m-%dT00:00:00Z")
    end = datetime.utcnow().strftime("%Y-%m-%dT23:59:59Z")

    all_metrics = []
    for mkt_id in amazon_api.ALL_EU_MARKETPLACE_IDS:
        plat_code = config.MARKETPLACE_TO_PLATFORM.get(mkt_id, "")

        data = amazon_api.api_get("/sales/v1/orderMetrics", {
            "marketplaceIds": mkt_id,
            "interval": f"{start}--{end}",
            "granularity": "Day",
            "granularityTimeZone": "Europe/Berlin",
            "buyerType": "All",
            "fulfillmentNetwork": "AFN",
        })

        metrics = data.get("payload", [])
        for m in metrics:
            interval = m.get("interval", "")
            day = interval[:10] if interval else ""
            total_sales = m.get("totalSales", {})

            all_metrics.append({
                "date": day,
                "marketplace_id": mkt_id,
                "platform_code": plat_code,
                "units_ordered": m.get("unitCount", 0) or 0,
                "order_count": m.get("orderCount", 0) or 0,
                "order_item_count": m.get("orderItemCount", 0) or 0,
                "total_sales": float(total_sales.get("amount", 0) or 0),
                "currency": total_sales.get("currencyCode", "EUR"),
                "avg_unit_price": float(m.get("averageUnitPrice", {}).get("amount", 0) or 0),
                "avg_items_per_order": m.get("averageItemsPerOrder", 0) or 0,
            })

        time.sleep(2)

    print(f"  [Sales] Got {len(all_metrics)} daily sales metrics across marketplaces")
    # Store in daily_metrics or a separate table — for now just log
    for m in all_metrics[:5]:
        if m["total_sales"] > 0:
            print(f"    {m['date']} {m['platform_code']}: {m['order_count']} orders, "
                  f"{m['units_ordered']} units, {m['total_sales']:.2f} {m['currency']}")

    return len(all_metrics)


# ── Master sync ──────────────────────────────────────────────────────

def sync_all_data(conn, days_back=30):
    """Run all direct Amazon API syncs."""
    results = {}

    try:
        results["inventory"] = sync_inventory_api(conn)
    except Exception as e:
        print(f"  [ERROR] Inventory API: {e}")
        results["inventory"] = 0

    time.sleep(3)

    try:
        results["bsr"] = sync_bsr(conn)
    except Exception as e:
        print(f"  [ERROR] BSR sync: {e}")
        results["bsr"] = 0

    time.sleep(3)

    try:
        results["pricing"] = sync_pricing(conn)
    except Exception as e:
        print(f"  [ERROR] Pricing sync: {e}")
        results["pricing"] = 0

    time.sleep(3)

    try:
        results["sales"] = sync_sales_metrics(conn, days_back)
    except Exception as e:
        print(f"  [ERROR] Sales metrics: {e}")
        results["sales"] = 0

    print(f"\n  Data sync complete: {results}")
    return results
