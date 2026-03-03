# details_final.py
import asyncio
from playwright.async_api import async_playwright
import pandas as pd
import re
import html
import time

# ---------- CONFIG ----------
INPUT_CSV = "input.csv"     
OUTPUT_CSV = "output.csv"
DELAY_BETWEEN_REQUESTS = 1  # seconds
BASE_URL = "https://lso.ca/public-resources/finding-a-lawyer-or-paralegal/directory-search/member?MemberNumber={}"

# ---------- HELPERS ----------
def clean_text(s):
    if s is None:
        return ""
    s = html.unescape(s)
    s = s.replace("\u00a0", " ").replace("Â", "")
    s = re.sub(r"[\u200b\u200c\u200d]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def extract_email(text):
    if not text:
        return ""
    match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    return match.group(0) if match else ""

async def scrape_profile(page, member_number):
    url = BASE_URL.format(member_number)
    await page.goto(url)
    await page.wait_for_timeout(1000)  # wait for JS to load

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
        "ProfileURL": url,
        "Success": True,
    }

    # Full name
    h2 = await page.query_selector("h2.member-info-title")
    if h2:
        data["FullName"] = clean_text(await h2.inner_text())

    # Member info wrappers
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

    # Regulatory history
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

async def main():
    df_in = pd.read_csv(INPUT_CSV, dtype=str)
    if "MemberNumber" not in df_in.columns:
        raise SystemExit("input.csv must contain a 'MemberNumber' column")
    member_numbers = df_in["MemberNumber"].dropna().astype(str).tolist()

    results = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()

        for i, mn in enumerate(member_numbers, start=1):
            print(f"[{i}/{len(member_numbers)}] Scraping {mn} ...", end=" ")
            try:
                rec = await scrape_profile(page, mn)
                print("done")
                results.append(rec)
            except Exception as e:
                print(f"failed: {e}")
            time.sleep(DELAY_BETWEEN_REQUESTS)

        await browser.close()

    # Save to CSV
    df_out = pd.DataFrame(results)
    cols = [
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
        "ProfileURL",
    ]
    df_out = df_out[cols]
    df_out.to_csv(OUTPUT_CSV, index=False)
    print(f"[DONE] Saved {len(df_out)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    asyncio.run(main())
