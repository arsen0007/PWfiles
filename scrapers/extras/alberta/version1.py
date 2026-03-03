import logging
import argparse
import time
import random
import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError

# --- Setup Professional Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
SEARCH_URL = "https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&action=searchTop"
CITY_DROPDOWN = "select[name='city_nm']"
SEARCH_BUTTON = "a:has-text('Search')"
PROFILE_LINK_SELECTOR = "a:has(div.font-size-plus)"
PROFILE_HEADING = "div.content-heading"


def scrape_profile_page(page):
    """Scrapes the data from the currently loaded profile page."""
    try:
        page.wait_for_selector(PROFILE_HEADING, timeout=15000)
    except TimeoutError:
        logging.warning("Timed out waiting for profile page to load. Skipping.")
        return None

    def safe_extract(label):
        try:
            row_locator = page.locator(f"tr:has-text('{label}')")
            if row_locator.count() > 0:
                return row_locator.locator("td").nth(1).inner_text(timeout=2000).strip()
            return None
        except Exception:
            return None

    name = page.locator(PROFILE_HEADING).inner_text().strip()
    email_locator = page.locator("a[href^='mailto']")
    email_val = email_locator.inner_text().strip() if email_locator.count() > 0 else None
    phone_val = safe_extract("Office")
    status_val = safe_extract("Practising Status")
    enrol_val = safe_extract("Enrolment Date")
    discipline_val = safe_extract("Discipline History") or "None"

    # --- FIX: Precise address extraction for cleaner data ---
    practice_locator = page.locator("div.content-subheading:has-text('Practice Name')")
    practice_name_val = None
    address_val = None
    if practice_locator.count() > 0:
        practice_name_val = practice_locator.inner_text().strip()
        # Use JS evaluation to get only the text nodes of the parent, excluding the heading
        address_val = practice_locator.locator("xpath=..").evaluate("""
            element => Array.from(element.childNodes)
                .filter(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim() !== '')
                .map(node => node.textContent.trim())
                .join('\\n')
        """)

    logging.info(f"Successfully scraped data for: {name}")
    return {
        "Name": name, "Email": email_val, "Phone": phone_val, "Practising Status": status_val,
        "Enrolment Date": enrol_val, "Practice Name": practice_name_val, "Address": address_val,
        "Discipline History": discipline_val
    }


def main(city, max_profiles):
    """Main function to orchestrate the scraping process."""
    logging.info(f"Starting scrape for city='{city}' with max_profiles={max_profiles}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=100)
        page = browser.new_page()
        scraped_data = []

        for i in range(max_profiles):
            logging.info(f"--- Starting scrape for profile number {i+1} ---")
            try:
                page.goto(SEARCH_URL, timeout=60000)
                page.select_option(CITY_DROPDOWN, city)
                page.click(SEARCH_BUTTON)
                page.wait_for_selector(PROFILE_LINK_SELECTOR, timeout=15000)

                profile_links = page.locator(PROFILE_LINK_SELECTOR)
                if i >= profile_links.count():
                    logging.info("No more profiles to scrape. Stopping.")
                    break

                profile_to_click = profile_links.nth(i)
                name_for_log = profile_to_click.inner_text()
                logging.info(f"Clicking on: {name_for_log.strip()}")
                profile_to_click.click()

                lawyer_data = scrape_profile_page(page)
                if lawyer_data:
                    scraped_data.append(lawyer_data)

            except TimeoutError:
                logging.error(f"A timeout occurred during iteration {i+1}. Skipping profile.")
                page.screenshot(path=f'debug_timeout_profile_{i+1}.png')
                continue
            except Exception as e:
                logging.error(f"An unexpected error occurred during iteration {i+1}: {e}")
                continue

            time.sleep(random.uniform(1.0, 2.5))

        browser.close()
        return scraped_data


if __name__ == "__main__":
    # --- FIX: Added command-line argument parsing for flexibility ---
    parser = argparse.ArgumentParser(description="Scrape lawyer data from the LSA directory.")
    parser.add_argument("city", type=str, help="The city to search for (e.g., 'Calgary', 'Edmonton').")
    parser.add_argument("--max", type=int, default=10, help="The maximum number of profiles to scrape.")
    args = parser.parse_args()

    data = main(args.city, args.max)

    if data:
        df = pd.DataFrame(data)
        output_file = f"alberta_lawyers_{args.city.lower()}.xlsx"
        df.to_excel(output_file, index=False)
        logging.info(f"🎉 Success! Saved {len(data)} profiles to {output_file}")
    else:
        logging.warning("Scraping finished, but no data was collected.")