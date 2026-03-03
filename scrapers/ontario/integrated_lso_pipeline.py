#!/usr/bin/env python3
"""
Integrated LSO pipeline:
1. Reads cities from cities.csv (column: City)
2. For each city: query Azure Search API, filter Status contains 'Private Practice'
3. Collect MemberNumbers + City/Province/Country metadata
4. Scrape detailed profile pages for each MemberNumber (Playwright)
5. Export one combined CSV (date-stamped) WITHOUT the public ProfileURL
6. Adds a 'Notes' column for errors / warnings for each record
7. Logs detailed errors to a separate pipeline log file
"""

import os
import sys
import time
import asyncio
import logging
from datetime import date, datetime
import requests
import pandas as pd
import re
import html
from typing import Dict

# ---------- CONFIG ----------
CITIES_CSV = "cities.csv"            # input CSV with single column 'City'
OUTPUT_FILENAME_TEMPLATE = "final_results_all_cities_{date}.csv"  # date will be replaced
LOG_FILENAME_TEMPLATE = "pipeline_log_{timestamp}.txt"
API_KEY_ENV = "LSO_API_KEY"          # optional env var name for API key
DEFAULT_API_KEY = "212D535962D4563E62F8EC5D6E1C71CA"  # fallback
AZURE_SEARCH_URL = "https://lawsocietyontario.search.windows.net/indexes/lsolpindexprd/docs/search?api-version=2017-11-11"
LICENSE_TYPE_DEFAULT = "L1"
BATCH_SIZE = 1000
DELAY_BETWEEN_REQUESTS = 1.0
REQUEST_TIMEOUT = 60
PLAYWRIGHT_HEADLESS = True

# ---------- Logging setup ----------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_filename = LOG_FILENAME_TEMPLATE.format(timestamp=timestamp)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ---------- Helpers ----------
def get_api_key() -> str:
    return os.environ.get(API_KEY_ENV, DEFAULT_API_KEY)

def clean_text(s):
    if s is None:
        return ""
    s = html.unescape(str(s))
    s = s.replace("\u00a0", " ").replace("Â", "")
    s = re.sub(r"[\u200b\u200c\u200d]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_email(text):
    if not text:
        return ""
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else ""

# ---------- Stage 1: API fetch & filter ----------
def fetch_city_members(city_token: str, license_type: str = LICENSE_TYPE_DEFAULT, batch_size: int = BATCH_SIZE) -> pd.DataFrame:
    api_key = get_api_key()
    headers = {
        "Content-Type": "application/json",
        "api-key": api_key,
        "Accept": "application/json"
    }
    all_rows = []
    skip = 0
    total = None
    logger.info(f"Fetching members for city: {city_token}")

    while True:
        payload = {
            "search": "*",
            "count": True,
            "top": batch_size,
            "skip": skip,
            "filter": f"memberlicencetype/any(m: m eq '{license_type}') and membercitynormalized/any(m: m eq '{city_token}')",
            "orderby": "memberlastname,memberfirstname,membermiddlename",
            "queryType": "full"
        }

        attempts = 0
        success = False
        while attempts < 3 and not success:
            attempts += 1
            try:
                r = requests.post(AZURE_SEARCH_URL, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
                if r.status_code == 200:
                    data = r.json()
                    success = True
                else:
                    logger.warning(f"API request failed for {city_token} (status {r.status_code}). Attempt {attempts}/3")
                    time.sleep(1 + attempts)
            except Exception as e:
                logger.exception(f"Exception when querying API for {city_token} (attempt {attempts}/3): {e}")
                time.sleep(1 + attempts)

        if not success:
            logger.error(f"Failed to fetch data for city {city_token} after 3 attempts. Skipping city.")
            break

        if total is None:
            total = data.get("@odata.count") or data.get("odata.count")
            logger.info(f"Total profiles returned by API for {city_token}: {total}")

        batch = data.get("value", [])
        logger.info(f"Fetched {len(batch)} profiles (skip={skip}) for {city_token}")

        if not batch:
            break

        for item in batch:
            # Flatten Status (sometimes a list)
            status_val = item.get("memberwebstatus")
            if isinstance(status_val, list):
                status_val = ", ".join([str(x) for x in status_val if x])
            elif status_val is None:
                status_val = ""
            else:
                status_val = str(status_val)

            all_rows.append({
                "MemberNumber": item.get("membernumber"),
                "FullName": item.get("memberfullname"),
                "Status": status_val.strip(),
                "City": item.get("membercity"),
                "Province": item.get("memberprovincetext"),
                "Country": item.get("membercountrytext")
            })

        skip += batch_size
        if total and skip >= total:
            break

    df = pd.DataFrame(all_rows)
    if not df.empty:
        unique_statuses = df["Status"].dropna().unique().tolist()
        logger.info(f"Unique Status values for {city_token} (first 10): {unique_statuses[:10]}")

        status_series = df["Status"].astype(str).fillna("").str.strip().str.lower()
        df_filtered = df[status_series.str.contains("private practice", na=False)].copy()

        logger.info(f"City {city_token}: {len(df_filtered)} members with 'Private Practice' in Status")
        return df_filtered
    else:
        return pd.DataFrame(columns=["MemberNumber", "FullName", "Status", "City", "Province", "Country"])

# ---------- Stage 2: Scrape detailed profile ----------
async def scrape_profile(playwright, page, member_number: str) -> Dict:
    BASE_URL = "https://lso.ca/public-resources/finding-a-lawyer-or-paralegal/directory-search/member?MemberNumber={}"
    url = BASE_URL.format(member_number)

    data = {
        "MemberNumber": member_number,
        "FullName": "",
        "AssumedName": "",
        "LawSocietyNumber": "",
        "AreasOfLaw_LegalServices": "",
        "BusinessName": "",
        "BusinessAddress": "",
        "Phone": "",
        "EmailAddress": "",
        "RegulatoryHistory": "",
        "Notes": ""
    }

    try:
        attempts = 0
        loaded = False
        while attempts < 2 and not loaded:
            attempts += 1
            try:
                await page.goto(url, timeout=30000)
                await page.wait_for_timeout(1000)
                loaded = True
            except Exception as e:
                logger.warning(f"Error loading {url} (attempt {attempts}/2): {e}")
                if attempts < 2:
                    await asyncio.sleep(1)

        if not loaded:
            data["Notes"] = f"Failed to load profile page after retries."
            return data

        h2 = await page.query_selector("h2.member-info-title")
        if h2:
            data["FullName"] = clean_text(await h2.inner_text())

        wrappers = await page.query_selector_all("div.member-info-wrapper")
        for w in wrappers:
            label_tag = await w.query_selector(".member-info-label")
            value_tag = await w.query_selector(".member-info-value")
            if not label_tag or not value_tag:
                continue
            label = clean_text(await label_tag.inner_text())
            val_text = clean_text(await value_tag.inner_text())

            if "Assumed Name" in label:
                data["AssumedName"] = val_text
            elif "Law Society Number" in label:
                data["LawSocietyNumber"] = val_text
            elif "Area(s) of Law" in label:
                data["AreasOfLaw_LegalServices"] = re.sub(r"\s*\|\s*", ", ", val_text)
            elif "Business Name" in label:
                data["BusinessName"] = val_text
            elif "Business Address" in label:
                data["BusinessAddress"] = val_text
            elif "Phone" in label:
                data["Phone"] = val_text
            elif "Email" in label:
                data["EmailAddress"] = extract_email(val_text)

        special_cases = await page.query_selector_all("div.member-special-cases")
        for s in special_cases:
            label_tag = await s.query_selector(".member-info-label")
            value_tag = await s.query_selector(".member-info-value")
            if not label_tag or not value_tag:
                continue
            label = clean_text(await label_tag.inner_text())
            if "Regulatory History" in label:
                data["RegulatoryHistory"] = clean_text(await value_tag.inner_text())
                break

        return data

    except Exception as e:
        logger.exception(f"Unhandled exception while scraping MemberNumber {member_number}: {e}")
        data["Notes"] = f"Unhandled exception: {e}"
        return data

# ---------- Main pipeline ----------
async def main():
    if not os.path.exists(CITIES_CSV):
        logger.error(f"{CITIES_CSV} not found. Please create a CSV with a 'City' column.")
        sys.exit(1)

    df_cities = pd.read_csv(CITIES_CSV, dtype=str)
    if "City" not in df_cities.columns:
        logger.error("cities.csv must contain a header column named 'City'")
        sys.exit(1)

    cities = [clean_text(c) for c in df_cities["City"].dropna().unique().tolist()]
    if not cities:
        logger.error("No cities found in cities.csv")
        sys.exit(1)

    logger.info(f"Cities to process: {cities}")

    all_members = []
    for city in cities:
        df_members = fetch_city_members(city_token=city)
        if df_members.empty:
            logger.info(f"No 'In Private Practice' members found for {city}. Skipping.")
            continue

        for _, row in df_members.iterrows():
            mn = row.get("MemberNumber")
            if not mn:
                continue
            all_members.append({
                "MemberNumber": str(mn).strip(),
                "City": clean_text(row.get("City") or city),
                "Province": clean_text(row.get("Province") or ""),
                "Country": clean_text(row.get("Country") or ""),
                "API_FullName": clean_text(row.get("FullName") or "")
            })

    if not all_members:
        logger.info("No members to scrape after filtering all cities. Exiting.")
        sys.exit(0)

    seen = {}
    unique_members = []
    for rec in all_members:
        mn = rec["MemberNumber"]
        if mn not in seen:
            seen[mn] = True
            unique_members.append(rec)
    logger.info(f"Total unique members to scrape: {len(unique_members)}")

    from playwright.async_api import async_playwright
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=PLAYWRIGHT_HEADLESS)
        page = await browser.new_page()

        for idx, rec in enumerate(unique_members, start=1):
            mn = rec["MemberNumber"]
            logger.info(f"[{idx}/{len(unique_members)}] Scraping MemberNumber {mn} (City: {rec['City']})")
            scraped = await scrape_profile(p, page, mn)
            scraped["City"] = rec["City"]
            scraped["Province"] = rec["Province"]
            scraped["Country"] = rec["Country"]

            if not scraped.get("FullName") and rec.get("API_FullName"):
                scraped["FullName"] = rec["API_FullName"]

            scraped["MemberNumber"] = mn
            results.append(scraped)

            await asyncio.sleep(DELAY_BETWEEN_REQUESTS)

        await browser.close()

    df_out = pd.DataFrame(results)
    final_columns = [
        "MemberNumber",
        "FullName",
        "AssumedName",
        "LawSocietyNumber",
        "AreasOfLaw_LegalServices",
        "BusinessName",
        "BusinessAddress",
        "Phone",
        "EmailAddress",
        "RegulatoryHistory",
        "City",
        "Province",
        "Country",
        "Notes"
    ]
    for col in final_columns:
        if col not in df_out.columns:
            df_out[col] = ""

    df_out = df_out[final_columns]
    df_out = df_out.sort_values(by=["City", "FullName"]).reset_index(drop=True)

    today = date.today().isoformat()
    output_filename = OUTPUT_FILENAME_TEMPLATE.format(date=today)
    df_out.to_csv(output_filename, index=False, encoding="utf-8")
    logger.info(f"[DONE] Saved {len(df_out)} records to {output_filename}")
    logger.info(f"Detailed log written to {log_filename}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
    except Exception as e:
        logger.exception(f"Fatal error in pipeline: {e}")
