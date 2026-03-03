from playwright.sync_api import sync_playwright
import time, random
import pandas as pd

SEARCH_URL = "https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&action=searchTop"

def scrape_city(city, max_profiles=5):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=400)
        page = browser.new_page()
        lawyers = []

        for idx in range(max_profiles):
            page.goto(SEARCH_URL)

            # Select city and search
            page.select_option("select[name='city_nm']", city)
            page.click("a:has-text('Search')")
            page.wait_for_selector("div.font-size-plus")

            # Re-fetch names
            names = page.query_selector_all("div.font-size-plus")
            if idx >= len(names):
                break

            name_tag = names[idx]
            name = name_tag.inner_text().strip()
            print(f"➡️ Opening profile {idx+1}: {name}")

            # Click profile and wait for profile heading instead of navigation
            link = name_tag.evaluate_handle("el => el.closest('a')")
            link.click()
            page.wait_for_selector("div.content-heading", timeout=15000)

            # --- Extract fields ---
            def safe_extract(label):
                try:
                    tag = page.locator(f"text={label}")
                    if tag.count():
                        return tag.locator("xpath=..").locator("td").nth(1).inner_text().strip()
                    return None
                except:
                    return None

            email_tag = page.query_selector("a[href^='mailto']")
            phone_val = safe_extract("Office")
            status_val = safe_extract("Practising Status")
            enrol_val = safe_extract("Enrolment Date")
            discipline_val = safe_extract("Discipline History") or "None"

            practice_block = page.query_selector("div.content-subheading")

            lawyer = {
                "Name": name,
                "Email": email_tag.inner_text().strip() if email_tag else None,
                "Phone": phone_val,
                "Practising Status": status_val,
                "Enrolment Date": enrol_val,
                "Practice Name": practice_block.inner_text().strip() if practice_block else None,
                "Address": practice_block.evaluate("el => el.parentElement.innerText") if practice_block else None,
                "Discipline History": discipline_val
            }

            lawyers.append(lawyer)
            time.sleep(random.uniform(1.5, 3.0))

        browser.close()
        return lawyers


if __name__ == "__main__":
    city = "Calgary"
    data = scrape_city(city, max_profiles=5)

    # Save to Excel
    df = pd.DataFrame(data)
    df.to_excel("alberta_lawyers_fixed.xlsx", index=False)

    print("\n✅ Done. Saved to alberta_lawyers_fixed.xlsx")
