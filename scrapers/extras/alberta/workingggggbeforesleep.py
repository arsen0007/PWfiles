import logging
import argparse
import time
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
PAGINATION_SELECTOR = "div.dataTables_paginate" # Used to confirm final page has loaded


def scrape_profile_page(page):
    """
    Scrapes the data from the currently loaded profile page.
    """
    try:
        page.wait_for_selector(PROFILE_HEADING, timeout=15000)
    except TimeoutError:
        logging.warning("Timed out waiting for profile page to load. Skipping.")
        return None

    name = page.locator(PROFILE_HEADING).inner_text().strip()
    email_locator = page.locator("a[href^='mailto']")
    email_val = email_locator.inner_text().strip() if email_locator.count() > 0 else None

    # --- FIX 1: Handle multiple phone numbers by taking the first one ---
    phone_val = None
    try:
        label_locator = page.locator('td.form-label:has-text("Office")')
        if label_locator.count() > 0:
            # Use .first to avoid strict mode violation if multiple numbers exist
            phone_val = label_locator.locator("xpath=./following-sibling::td").first.inner_text(timeout=2000).strip()
    except TimeoutError:
        logging.warning(f"Could not extract phone number for {name}.")

    status_val, enrol_val = None, None
    status_table = page.locator("table:has-text('Practising Status')")
    if status_table.count() > 0:
        try:
            data_cells = status_table.locator("td.table-result")
            if data_cells.count() >= 3:
                status_val = data_cells.nth(1).inner_text(timeout=2000).strip()
                enrol_val = data_cells.nth(2).inner_text(timeout=2000).strip()
        except TimeoutError:
            logging.warning(f"Could not extract Status/Enrolment for {name}.")

    practice_name_val, address_val = None, None
    location_header = page.locator("td.table-result-header:has-text('Practice Location')")
    if location_header.count() > 0:
        try:
            content_cell = location_header.locator("xpath=../following-sibling::tr[1]/td")
            practice_name_locator = content_cell.locator("div.content-subheading")
            if practice_name_locator.count() > 0:
                practice_name_val = practice_name_locator.inner_text(timeout=2000).strip()
                address_container = practice_name_locator.locator("xpath=..")
                address_val = address_container.evaluate("""
                    element => Array.from(element.childNodes)
                        .filter(node => node.nodeType === Node.TEXT_NODE && node.textContent.trim() !== '')
                        .map(node => node.textContent.trim())
                        .join(', ')
                """)
        except TimeoutError:
            logging.warning(f"Could not extract Practice Name/Address for {name}.")

    def safe_extract_discipline(label):
        try:
            row_locator = page.locator(f"tr:has-text('{label}')")
            if row_locator.count() > 0:
                return row_locator.locator("td").nth(1).inner_text(timeout=2000).strip()
            return None
        except Exception: return None
    discipline_val = safe_extract_discipline("Discipline History") or "None"

    logging.info(f"Successfully scraped data for: {name}")
    return {
        "Name": name, "Email": email_val, "Phone": phone_val, "Practising Status": status_val,
        "Enrolment Date": enrol_val, "Practice Name": practice_name_val, "Address": address_val,
        "Discipline History": discipline_val
    }


def main(city, max_profiles, profiles_per_page=10):
    """
    Main function with new logic to handle pagination correctly.
    """
    logging.info(f"Starting scrape for city='{city}' with max_profiles={max_profiles}")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False, slow_mo=50)
        page = browser.new_page()
        scraped_data = []

        for i in range(max_profiles):
            current_profile_num = i + 1
            # --- FIX 2: Calculate target page and index for pagination ---
            target_page = i // profiles_per_page
            index_on_page = i % profiles_per_page

            logging.info(f"--- Preparing to scrape profile {current_profile_num} (Page {target_page + 1}, Item {index_on_page + 1}) ---")
            try:
                # 1. Start a fresh search
                page.goto(SEARCH_URL, timeout=60000)
                page.select_option(CITY_DROPDOWN, city)
                page.click(SEARCH_BUTTON)
                page.wait_for_selector(PAGINATION_SELECTOR, timeout=90000)

                # 2. Navigate to the correct page by clicking "Next"
                for page_click in range(target_page):
                    logging.info(f"Navigating to page {page_click + 2}...")
                    next_button = page.locator("a.paginate_button.next")
                    if "disabled" in (next_button.get_attribute("class") or ""):
                        raise Exception("Cannot find 'Next' button to click to the target page.")
                    next_button.click()
                    # Wait for page to settle after click
                    page.wait_for_selector(f"a.paginate_button.current:has-text('{page_click + 2}')")

                # 3. Click the correct profile on the target page
                profile_links = page.locator(PROFILE_LINK_SELECTOR)
                if index_on_page >= profile_links.count():
                    logging.warning(f"No more profiles found on page {target_page + 1}. Stopping.")
                    break
                
                profile_to_click = profile_links.nth(index_on_page)
                name_for_log = profile_to_click.inner_text(timeout=5000)
                logging.info(f"Clicking on: {name_for_log.strip()}")
                profile_to_click.click()

                # 4. Scrape the profile details
                lawyer_data = scrape_profile_page(page)
                if lawyer_data:
                    scraped_data.append(lawyer_data)
                
                time.sleep(1.0)

            except Exception as e:
                logging.error(f"An unexpected error occurred scraping profile {current_profile_num}: {e}", exc_info=False)
                page.screenshot(path=f'debug_error_profile_{current_profile_num}.png')
                if "Target page might have been removed" in str(e): # Break if Playwright context is lost
                    logging.critical("Critical error, likely due to page navigation failure. Stopping script.")
                    break
                continue
        
        browser.close()
    return scraped_data


if __name__ == "__main__":
    try:
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
    except Exception as e:
        logging.critical(f"A critical error occurred at the script level: {e}", exc_info=True)