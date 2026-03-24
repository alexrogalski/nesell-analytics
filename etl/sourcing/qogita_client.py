"""Qogita B2B wholesale API client for sourcing analysis.

Qogita is Europe's largest B2B wholesale marketplace (500K+ GTINs,
10K+ brands, Health & Beauty + Household).

Authentication: JWT tokens via email/password login.
Key endpoint: GET /variants/{gtin}/ for product lookup by EAN.

Usage:
    from etl.sourcing.qogita_client import QogitaClient

    client = QogitaClient()  # loads creds from ~/.keys/qogita.env
    product = client.get_variant("4006381333931")
    offers = client.search_catalog(brand="stabilo", page=1, size=20)
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

# ── Constants ────────────────────────────────────────────────────────

BASE_URL = "https://api.qogita.com"
KEYS_PATH = Path.home() / ".keys" / "qogita.env"


# ── Data models ──────────────────────────────────────────────────────

@dataclass
class QogitaOffer:
    """A single supplier offer for a product on Qogita."""
    price: float = 0.0
    currency: str = "EUR"
    available_quantity: int = 0
    offer_qid: str = ""
    is_deal: bool = False
    deal_min_quantity: int = 0
    deal_max_quantity: int = 0


@dataclass
class QogitaProduct:
    """Product data from Qogita variant lookup."""
    gtin: str = ""
    name: str = ""
    brand: str = ""
    brand_slug: str = ""
    category: str = ""
    category_path: list[str] = field(default_factory=list)
    qid: str = ""
    slug: str = ""
    fid: str = ""
    images: list[str] = field(default_factory=list)
    # Pricing
    lowest_price: float | None = None
    currency: str = "EUR"
    offers: list[QogitaOffer] = field(default_factory=list)
    offer_count: int = 0
    # Availability
    total_available_qty: int = 0
    # Metadata
    hs_code: str = ""
    errors: list[str] = field(default_factory=list)


# ── Client ───────────────────────────────────────────────────────────

class QogitaClient:
    """Qogita API client with JWT authentication."""

    def __init__(self, email: str | None = None, password: str | None = None):
        self._email = email
        self._password = password
        self._access_token: str = ""
        self._refresh_token: str = ""
        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"

        if not self._email:
            self._load_credentials()

    def _load_credentials(self):
        """Load email/password from ~/.keys/qogita.env."""
        if not KEYS_PATH.exists():
            raise FileNotFoundError(
                f"Qogita credentials not found at {KEYS_PATH}. "
                "Create the file with QOGITA_EMAIL and QOGITA_PASSWORD."
            )

        for line in KEYS_PATH.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip("'\"")
                if key == "QOGITA_EMAIL":
                    self._email = value
                elif key == "QOGITA_PASSWORD":
                    self._password = value

        if not self._email or not self._password:
            raise ValueError(
                f"Missing QOGITA_EMAIL or QOGITA_PASSWORD in {KEYS_PATH}"
            )

    # ── Auth ─────────────────────────────────────────────────────────

    def _ensure_auth(self):
        """Login or refresh token if needed."""
        if self._access_token:
            return
        self._login()

    def _login(self):
        """Authenticate and store JWT tokens."""
        resp = self._session.post(
            f"{BASE_URL}/auth/login/",
            json={"email": self._email, "password": self._password},
            timeout=15,
        )

        if resp.status_code == 429:
            raise RuntimeError("Qogita auth rate limited (429). Try again later.")
        if resp.status_code != 200:
            raise RuntimeError(
                f"Qogita login failed ({resp.status_code}): {resp.text[:300]}"
            )

        data = resp.json()
        self._access_token = data.get("accessToken") or data.get("access", "")
        self._refresh_token = data.get("refreshToken") or data.get("refresh", "")
        self._session.headers["Authorization"] = f"Bearer {self._access_token}"

    def _refresh(self) -> bool:
        """Refresh the access token. Returns True on success."""
        if not self._refresh_token:
            # No refresh token: just re-login
            try:
                self._access_token = ""
                self._login()
                return True
            except Exception:
                return False

        resp = self._session.post(
            f"{BASE_URL}/auth/refresh/",
            json={"refresh": self._refresh_token},
            timeout=15,
        )

        if resp.status_code == 200:
            data = resp.json()
            self._access_token = data.get("accessToken") or data.get("access", "")
            self._refresh_token = data.get("refreshToken") or data.get("refresh", self._refresh_token)
            self._session.headers["Authorization"] = f"Bearer {self._access_token}"
            return True

        # Refresh failed: re-login
        try:
            self._access_token = ""
            self._login()
            return True
        except Exception:
            return False

    # ── HTTP helper ──────────────────────────────────────────────────

    def _get(self, path: str, params: dict | None = None) -> dict | list | None:
        """GET request with auto-auth and retry on 401."""
        self._ensure_auth()

        for attempt in range(3):
            resp = self._session.get(
                f"{BASE_URL}{path}",
                params=params,
                timeout=30,
            )

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code == 401:
                if attempt < 2 and self._refresh():
                    continue
                self._access_token = ""
                self._login()
                continue

            if resp.status_code == 429:
                wait = 5 * (attempt + 1)
                print(f"    [qogita] Rate limited, waiting {wait}s")
                time.sleep(wait)
                continue

            if resp.status_code == 404:
                return None

            if resp.status_code >= 500:
                wait = 3 * (attempt + 1)
                time.sleep(wait)
                continue

            raise RuntimeError(
                f"Qogita {path} returned {resp.status_code}: {resp.text[:300]}"
            )

        return None

    # ── Product lookup ───────────────────────────────────────────────

    def get_variant(self, gtin: str) -> QogitaProduct | None:
        """Look up a product by GTIN/EAN.

        Returns QogitaProduct with pricing and availability,
        or None if not found.
        """
        data = self._get(f"/variants/{gtin}/")
        if not data:
            return None

        return self._parse_variant(data)

    def _parse_variant(self, data: dict) -> QogitaProduct:
        """Parse variant API response into QogitaProduct.

        Real response structure (2026-03):
            gtin, name, slug, label, category{name,slug,hscode},
            brand{name,slug}, dimensions{width,height,depth,mass},
            images[], price, priceCurrency, inventory, delay,
            popularity, rating, ratingCount, qid, fid, unit, origin,
            shippingFromCountries[], sellerCount, isWatchlisted,
            priceUpdatedAt, isInStock
        """
        product = QogitaProduct(
            gtin=data.get("gtin", ""),
            name=data.get("name", "") or data.get("title", ""),
            qid=data.get("qid", ""),
            slug=data.get("slug", ""),
            fid=data.get("fid", ""),
        )

        # Brand
        brand = data.get("brand", {})
        if isinstance(brand, dict):
            product.brand = brand.get("name", "")
            product.brand_slug = brand.get("slug", "")
        elif isinstance(brand, str):
            product.brand = brand

        # Category
        cat = data.get("category", {})
        if isinstance(cat, dict):
            product.category = cat.get("name", "")
            product.hs_code = cat.get("hscode", "")
            path = cat.get("path", [])
            if path:
                product.category_path = [p.get("name", "") for p in path if isinstance(p, dict)]
        elif isinstance(cat, str):
            product.category = cat

        # Images
        images = data.get("images", [])
        if images:
            product.images = [
                img.get("url", "") if isinstance(img, dict) else str(img)
                for img in images[:5]
            ]

        # Price
        price = data.get("price")
        if price is not None:
            try:
                product.lowest_price = float(price)
            except (TypeError, ValueError):
                pass

        product.currency = data.get("priceCurrency", "EUR")

        # Inventory (real field name)
        inventory = data.get("inventory") or data.get("availableQuantity") or 0
        try:
            product.total_available_qty = int(inventory)
        except (TypeError, ValueError):
            pass

        # Seller count
        product.offer_count = data.get("sellerCount", 0) or (1 if product.lowest_price else 0)

        return product

    # ── Catalog browsing ─────────────────────────────────────────────

    def get_brands(self, page: int = 1, size: int = 50) -> list[dict]:
        """List available brands."""
        data = self._get("/brands/", params={"page": page, "size": size})
        if not data:
            return []
        return data.get("results", data) if isinstance(data, dict) else data

    def get_categories(self) -> list[dict]:
        """List available categories."""
        data = self._get("/categories/", params={"size": 100})
        if not data:
            return []
        return data.get("results", data) if isinstance(data, dict) else data

    def get_watchlist(self, page: int = 1, size: int = 50,
                      is_available: bool | None = None) -> list[dict]:
        """Get watchlisted items."""
        params = {"page": page, "size": size}
        if is_available is not None:
            params["is_available"] = str(is_available).lower()
        data = self._get("/watchlist/items/", params=params)
        if not data:
            return []
        return data.get("results", data) if isinstance(data, dict) else data

    def download_catalog_url(self, format: str = "xlsx") -> str | None:
        """Get download URL for the filtered catalog."""
        data = self._get("/variants/search/download/", params={"format": format})
        if data and isinstance(data, dict):
            return data.get("url") or data.get("download_url")
        return None


# ── Convenience function ─────────────────────────────────────────────

def lookup_ean(gtin: str) -> QogitaProduct | None:
    """Quick lookup: create client and search by EAN.

    Returns QogitaProduct or None if not found / credentials missing.
    """
    try:
        client = QogitaClient()
        return client.get_variant(gtin)
    except (FileNotFoundError, ValueError) as e:
        print(f"    [qogita] {e}")
        return None
    except Exception as e:
        print(f"    [qogita] Error: {e}")
        return None
