"""Shared Amazon SP-API utilities: auth, retry, reports framework."""
import requests
import time
import gzip
import csv
import io
import json
from datetime import datetime, timedelta
from . import config


# ── Auth ─────────────────────────────────────────────────────────────

_token = None
_token_time = 0


def _refresh_token():
    global _token, _token_time
    r = requests.post("https://api.amazon.com/auth/o2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": config.AMZ_CREDS.get("refresh_token", ""),
        "client_id": config.AMZ_CREDS.get("client_id", ""),
        "client_secret": config.AMZ_CREDS.get("client_secret", ""),
    })
    _token = r.json()["access_token"]
    _token_time = time.time()
    return _token


def headers():
    global _token, _token_time
    if not _token or time.time() - _token_time > 3000:
        _refresh_token()
    return {"x-amz-access-token": _token, "Content-Type": "application/json"}


# ── HTTP Helpers ─────────────────────────────────────────────────────

def api_get(path, params=None, retries=8):
    """GET with retry and backoff."""
    url = f"{config.AMZ_API_BASE}{path}"
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=headers(), params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            wait = 10 * (attempt + 1)
            print(f"    [{type(e).__name__}] retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(5 * (2 ** attempt), 60)
            print(f"    [429] rate limited, waiting {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            _refresh_token()
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [{resp.status_code}] server error, retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        return resp.json()
    print(f"    [WARN] All {retries} attempts failed for {path}")
    return {}


def api_post(path, body=None, params=None, retries=8):
    """POST with retry and longer backoff for rate limits."""
    url = f"{config.AMZ_API_BASE}{path}"
    last_error = None
    for attempt in range(retries):
        try:
            resp = requests.post(url, headers=headers(), json=body or {}, params=params, timeout=30)
        except (requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            wait = 10 * (attempt + 1)
            print(f"    [{type(e).__name__}] POST retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = min(15 * (2 ** attempt), 120)
            print(f"    [429] rate limited on POST, waiting {wait}s ({attempt+1}/{retries})")
            last_error = f"429 QuotaExceeded"
            time.sleep(wait)
            continue
        if resp.status_code == 403:
            _refresh_token()
            time.sleep(3)
            continue
        if resp.status_code >= 500:
            wait = 5 * (attempt + 1)
            print(f"    [{resp.status_code}] server error on POST, retry in {wait}s ({attempt+1}/{retries})")
            time.sleep(wait)
            continue
        return resp.json()
    print(f"    [WARN] All {retries} POST attempts failed for {path}"
          f"{f' (last error: {last_error})' if last_error else ''}")
    return {}


# ── Reports Framework ────────────────────────────────────────────────

ALL_EU_MARKETPLACE_IDS = list(config.MARKETPLACE_TO_PLATFORM.keys())


def create_report(report_type, marketplace_ids=None, start_date=None, end_date=None,
                  report_options=None):
    """Create a report request. Returns reportId."""
    if marketplace_ids is None:
        marketplace_ids = ALL_EU_MARKETPLACE_IDS

    body = {
        "reportType": report_type,
        "marketplaceIds": marketplace_ids,
    }
    if start_date:
        body["dataStartTime"] = (start_date if isinstance(start_date, str)
                                 else start_date.strftime("%Y-%m-%dT00:00:00Z"))
    if end_date:
        body["dataEndTime"] = (end_date if isinstance(end_date, str)
                               else end_date.strftime("%Y-%m-%dT23:59:59Z"))
    if report_options:
        body["reportOptions"] = report_options

    data = api_post("/reports/2021-06-30/reports", body)
    report_id = data.get("reportId")
    if not report_id:
        print(f"    [WARN] Failed to create report {report_type}: {data}")
    return report_id


def poll_report(report_id, timeout_minutes=30):
    """Poll until report is ready. Returns report document ID or None."""
    start = time.time()
    while time.time() - start < timeout_minutes * 60:
        data = api_get(f"/reports/2021-06-30/reports/{report_id}")
        status = data.get("processingStatus", "")
        if status == "DONE":
            return data.get("reportDocumentId")
        elif status in ("CANCELLED", "FATAL"):
            print(f"    [WARN] Report {report_id} status: {status}")
            return None
        time.sleep(15)
    print(f"    [WARN] Report {report_id} timed out after {timeout_minutes} min")
    return None


def download_report(document_id):
    """Download report document. Returns raw text content."""
    data = api_get(f"/reports/2021-06-30/documents/{document_id}")
    url = data.get("url")
    if not url:
        return ""

    compression = data.get("compressionAlgorithm")
    resp = requests.get(url, timeout=120)
    content = resp.content
    if compression == "GZIP":
        content = gzip.decompress(content)
    return content.decode("utf-8-sig")


def download_report_tsv(document_id):
    """Download report as parsed TSV rows (list of dicts)."""
    text = download_report(document_id)
    if not text:
        return []
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")
    return list(reader)


def download_report_json(document_id):
    """Download report as parsed JSON."""
    text = download_report(document_id)
    if not text:
        return {}
    return json.loads(text)


def fetch_report_tsv(report_type, marketplace_ids=None, start_date=None, end_date=None,
                     report_options=None):
    """High-level: create + poll + download TSV report."""
    report_id = create_report(report_type, marketplace_ids, start_date, end_date, report_options)
    if not report_id:
        return []

    print(f"    Report {report_type} created: {report_id}, polling...")
    doc_id = poll_report(report_id)
    if not doc_id:
        return []

    rows = download_report_tsv(doc_id)
    print(f"    Got {len(rows)} rows from {report_type}")
    return rows


def fetch_report_json(report_type, marketplace_ids=None, start_date=None, end_date=None,
                      report_options=None):
    """High-level: create + poll + download JSON report."""
    report_id = create_report(report_type, marketplace_ids, start_date, end_date, report_options)
    if not report_id:
        return {}

    print(f"    Report {report_type} created: {report_id}, polling...")
    doc_id = poll_report(report_id)
    if not doc_id:
        return {}

    data = download_report_json(doc_id)
    print(f"    Report {report_type} downloaded")
    return data
