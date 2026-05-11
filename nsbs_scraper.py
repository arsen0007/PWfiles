"""
Nova Scotia Barristers' Society - Member Directory Scraper
==========================================================
Phase 1 : Playwright browser-fetch, paginates through 'Nova Scotia' filter
Phase 2 : Playwright profile page scraper

Usage
-----
    python nsbs_scraper.py            # both phases
    python nsbs_scraper.py --phase1   # collect IDs only
    python nsbs_scraper.py --phase2   # scrape profiles only (needs nsbs_ids_staging.json)
"""

import asyncio
import argparse
import csv
import json
import os
import random
import re
import sys

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ═══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

SEARCH_URL    = "https://members.nsbs.org/NSBSMember/NSBSWEB/Lawyer_Search/Search_Page.aspx"
PROFILE_BASE  = "https://members.nsbs.org/NSBSMember/NSBSWEB/Lawyer_Search/Results_Page2.aspx?ID="

STAGING_FILE       = "nsbs_ids_staging.json"
OUTPUT_FILE        = "nsbs_lawyers.csv"

DELAY_MIN          = 2.0
DELAY_MAX          = 4.5

SUBMIT_BUTTON_ID   = "#ctl01_TemplateBody_WebPartManager1_gwpciNewQueryMenuCommon_ciNewQueryMenuCommon_ResultsGrid_Sheet0_SubmitButton"

# ═══════════════════════════════════════════════════════════════════════════════
# PERSISTENCE HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _atomic_save(path: str, data: dict):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f)
    os.replace(tmp, path)

def _load_staging() -> dict:
    if not os.path.exists(STAGING_FILE):
        return {}
    with open(STAGING_FILE, "r", encoding="utf-8") as f:
        return json.load(f).get("ids", {})

def _save_staging(ids: dict):
    _atomic_save(STAGING_FILE, {"ids": ids})

def _random_delay(lo: float, hi: float) -> float:
    return random.uniform(lo, hi)


# ═══════════════════════════════════════════════════════════════════════════════
# BROWSER SESSION MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════════

async def _make_context(pw):
    browser = await pw.chromium.launch(headless=True, args=["--disable-blink-features=AutomationControlled"])
    context = await browser.new_context(
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        locale="en-CA",
        timezone_id="America/Halifax",
        viewport={"width": 1366, "height": 768},
    )
    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return browser, context


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — PAGINATION SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

async def run_phase1():
    print("\n════════════════════════════════════════════════════════════")
    print("  PHASE 1 — ID Collection via Nova Scotia Filter")
    print("════════════════════════════════════════════════════════════\n")

    ids = _load_staging()
    if ids:
        print(f"  Resuming — {len(ids)} IDs currently collected.\n")

    async with async_playwright() as pw:
        browser, context = await _make_context(pw)
        page = await context.new_page()

        print("  Navigating to search page...")
        for attempt in range(3):
            try:
                await page.goto(SEARCH_URL, wait_until="networkidle", timeout=60000)
                break
            except Exception as e:
                print(f"  [nav error] {e}. Retrying...")
                await asyncio.sleep(3)

        print("  Selecting 'Nova Scotia' filter...")
        selects = await page.locator("select").all()
        found_ns = False
        for s in selects:
            options = await s.inner_text()
            if "Nova Scotia" in options:
                await s.select_option(label="Nova Scotia", force=True)
                found_ns = True
                break

        if not found_ns:
            print("  ERROR: Could not find the Province/State dropdown. Exiting.")
            await browser.close()
            return

        print("  Submitting search...")
        async with page.expect_response(lambda r: "Search_Page.aspx" in r.url, timeout=30000):
            await page.locator(SUBMIT_BUTTON_ID).click()

        await page.wait_for_load_state("networkidle")
        await asyncio.sleep(2)

        current_page = 1
        total_pages = 1

        while True:
            html = await page.content()
            
            # Extract total pages on the first pass
            if total_pages == 1:
                match = re.search(r'of\s+(\d+)</span>', html)
                if match:
                    total_pages = int(match.group(1))
                    print(f"  Detected {total_pages} total pages.\n")

            # Extract IDs from the current page for 'Practising Lawyer' only
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            rows = soup.find_all("tr", class_=["rgRow", "rgAltRow"])
            
            unique_uids = []
            for row in rows:
                cols = row.find_all("td")
                if len(cols) >= 7:
                    member_type = cols[5].get_text(strip=True)
                    if "practising lawyer" in member_type.lower():
                        link = cols[0].find("a", href=True)
                        if link:
                            uid_match = re.search(r'ID=(\d+)', link['href'])
                            if uid_match:
                                unique_uids.append(uid_match.group(1))

            unique_uids = list(set(unique_uids))
            
            new = 0
            for uid in unique_uids:
                if uid not in ids:
                    ids[uid] = {}
                    new += 1
                    
            print(f"  [Page {current_page:>3}/{total_pages}] Extracted {len(unique_uids):>2} 'Practising Lawyer' IDs  (+{new} new)")
            _save_staging(ids)
            
            if current_page >= total_pages:
                break
                
            current_page += 1
            
            # Locate the "Next Page" button
            next_btn = page.locator("input.rgPageNext").first
            if await next_btn.count() == 0:
                print("  No 'Next Page' button found. Stopping.")
                break
                
            # Random delay before clicking next to mimic human behaviour
            await asyncio.sleep(_random_delay(DELAY_MIN, DELAY_MAX))
            
            # Click and wait for postback
            try:
                async with page.expect_response(lambda r: "Search_Page.aspx" in r.url, timeout=30000):
                    await next_btn.click()
                await page.wait_for_load_state("networkidle")
                await asyncio.sleep(1)
            except Exception as e:
                print(f"  [pagination error] {e}. Retrying current page...")
                await asyncio.sleep(5)
                current_page -= 1 # retry

        await browser.close()

    print(f"\n  ✓ Phase 1 complete — {len(ids)} unique members found in staging.")


# ═══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — PROFILE SCRAPER
# ═══════════════════════════════════════════════════════════════════════════════

CSV_FIELDS = [
    "Name", "Status", "Member #", "Call to Bar Date",
    "Firm / Organization", "Address", "City", "Phone", "Email", "Website", "Area of Practice"
]

def parse_profile(html: str) -> dict:
    from bs4 import BeautifulSoup
    import re
    soup = BeautifulSoup(html, "html.parser")
    
    text_nodes = [s.strip() for s in soup.stripped_strings if len(s.strip()) > 0]
    full_text = " ".join(text_nodes)

    name = ""
    status = ""
    member_no = ""
    call_date = ""
    phone = ""
    email = ""
    website = ""
    address = ""
    city = ""
    firm = ""

    # Name
    m_name = re.search(r'Membership Information (.*?) Membership Info', full_text)
    if m_name: 
        name = m_name.group(1).strip()
        name = re.sub(r'^Loading Name\s*', '', name)
        name = re.sub(r'\s*Loading$', '', name)
    
    # Status
    m_status = re.search(r'Member Type:\s*(.*?)\s*(?:Member #|Call to the Bar|Primary Address|Disciplinary Info)', full_text)
    if m_status: status = m_status.group(1).strip()
        
    # Member # 
    m_mem = re.search(r'Member #:\s*([\d-]+)', full_text)
    if m_mem: member_no = m_mem.group(1).strip()
        
    # Call to Bar Date 
    m_call = re.search(r'Call to the Bar Date:\s*([A-Za-z]{3}\s*\d{1,2},\s*\d{4})', full_text)
    if m_call: call_date = m_call.group(1).strip()
        
    # Phone
    m_phone = re.search(r'Phone #:\s*([\d\-\(\)\s]+?)\s*(?:Fax|Email|Website|Secondary|Note)', full_text)
    if m_phone: phone = m_phone.group(1).strip()
        
    # Email
    m_email = re.search(r'Email:\s*([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})', full_text)
    if m_email: email = m_email.group(1).strip()
        
    # Website
    m_web = re.search(r'Website:\s*(http[^\s]+)', full_text)
    if m_web: website = m_web.group(1).strip()
        
    # Address Block
    m_addr = re.search(r'Primary Address(.*?)(?:Phone #|Fax|Email|Website|Secondary Address)', full_text)
    if m_addr:
        addr_text = m_addr.group(1).strip()
        if addr_text and "There are no records" not in addr_text:
            addr_start = -1
            addr_end = -1
            for i, t in enumerate(text_nodes):
                if "Primary Address" in t: addr_start = i
                if "Phone #:" in t: addr_end = i; break
                if "Fax:" in t and addr_end == -1: addr_end = i
                if "Email:" in t and addr_end == -1: addr_end = i
                if "Secondary Address" in t and addr_end == -1: addr_end = i
            
            if addr_start != -1 and addr_end != -1 and addr_end > addr_start + 1:
                addr_nodes = text_nodes[addr_start+1:addr_end]
                # Firm is often the first line if it doesn't start with a number
                if len(addr_nodes) >= 3 and not re.match(r'^\d', addr_nodes[0]):
                    firm = addr_nodes[0]
                    address_lines = addr_nodes[1:]
                else:
                    address_lines = addr_nodes
                    
                address = ", ".join(address_lines)
                if address_lines:
                    last_line = address_lines[-1]
                    parts = last_line.split(',')
                    if len(parts) > 1:
                        city = parts[0].strip()
                    elif " " in last_line:
                        city = last_line.split(" ")[0].strip()

    return {
        "Name": name,
        "Status": status,
        "Member #": member_no,
        "Call to Bar Date": call_date,
        "Firm / Organization": firm,
        "Address": address,
        "City": city,
        "Phone": phone,
        "Email": email,
        "Website": website,
        "Area of Practice": "" 
    }

async def run_phase2(staging: dict):
    scraped = set()
    if os.path.exists(OUTPUT_FILE):
        with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if "_rg" in row:
                    scraped.add(row["_rg"])

    remaining = {rg: rec for rg, rec in staging.items() if rg not in scraped}
    total = len(staging)
    done_count = len(scraped)

    print(f"\n══════════════════════════════════════════════")
    print(f"  PHASE 2 — Scraping {len(remaining)} profiles")
    print(f"══════════════════════════════════════════════\n")

    if not remaining: return

    file_exists = os.path.exists(OUTPUT_FILE) and os.stat(OUTPUT_FILE).st_size > 0
    all_fields = CSV_FIELDS + ["_rg"]

    async with async_playwright() as pw:
        browser, context = await _make_context(pw)
        page = await context.new_page()

        with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=all_fields)
            if not file_exists:
                writer.writeheader()

            for idx, (rg, _raw) in enumerate(remaining.items(), 1):
                url = f"{PROFILE_BASE}{rg}"
                print(f"  [{done_count + idx:>4}/{total}]", end=" ", flush=True)

                html = None
                for attempt in range(3):
                    try:
                        await page.goto(url, wait_until="networkidle", timeout=30000)
                        
                        # Smart wait loop: wait until "Loading Name" is gone
                        for _ in range(15):
                            html = await page.content()
                            if "Loading Name" not in html and "Loading Membership Info" not in html:
                                break
                            await asyncio.sleep(1)
                            
                        html = await page.content()
                        break
                    except Exception as e:
                        print(f"[err: {e}]", end=" ", flush=True)
                        await asyncio.sleep(4)

                if not html:
                    print(f"FAILED — {rg}")
                    continue

                try:
                    row = parse_profile(html)
                    row["_rg"] = rg
                    writer.writerow(row)
                    csvfile.flush()
                    print(f"✓  {row['Name']}")
                except Exception as e:
                    print(f"PARSE ERROR: {e}")

                await asyncio.sleep(1.0)

        await browser.close()
    print(f"\n  ✓ Phase 2 complete. Saved to: {OUTPUT_FILE}")

# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

async def main_async(args):
    run_p1 = args.phase1 or (not args.phase1 and not args.phase2)
    run_p2 = args.phase2 or (not args.phase1 and not args.phase2)

    if run_p1:
        await run_phase1()

    if run_p2:
        ids = _load_staging()
        if not ids:
            print(f"\n  ERROR: No IDs found in {STAGING_FILE}. Run Phase 1 first.\n")
            sys.exit(1)
        await run_phase2(ids)


def main():
    parser = argparse.ArgumentParser(description="NSBS Lawyer Scraper")
    parser.add_argument("--phase1", action="store_true", help="Phase 1 only")
    parser.add_argument("--phase2", action="store_true", help="Phase 2 only")
    args = parser.parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
