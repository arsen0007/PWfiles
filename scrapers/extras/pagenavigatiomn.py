import logging
import argparse
import pandas as pd
import re
from playwright.sync_api import sync_playwright, TimeoutError

# --- Setup Professional Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Constants ---
SEARCH_URL = "https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&action=searchTop"
CITY_DROPDOWN = "select[name='city_nm']"
SEARCH_BUTTON = "a:has-text('Search')"
PROFILE_LINK_SELECTOR = "a:has(div.font-size-plus)"
PROFILE_HEADING = "div.content-heading"
PAGINATION_SELECTOR = "div.dataTables_paginate"


def get_total_profiles(page) -> int | None:
    """
    Finds and parses the total number of entries from the results text.
    Example text: "Showing 1 to 10 of 5,679 entries" -> returns 5679
    """
    try:
        # The ID is the most reliable selector
        info_locator = page.locator("#member-directory_info")
        info_text = info_locator.inner_text(timeout=5000)
        
        # Use a regular expression to find the number after "of "
        match = re.search(r'of ([\d,]+)', info_text)
        
        if match:
            number_str = match.group(1)  # Extracts '5,679'
            return int(number_str.replace(',', '')) # Returns 5679
            
        logging.warning("Could not find the total profile count pattern in the info text.")
        return None
    except Exception as e:
        logging.error(f"Could not extract total profile count from page: {e}")
        return None


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

    phone_val = None
    try:
        label_locator = page.locator('td.form-label:has-text("Office")')
        if label_locator.count() > 0:
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


def main(city, max_profiles):
    """
    Main function refactored to be page-centric and use direct
    page number navigation for improved performance.
    """
    logging.info(f"Starting scrape for city='{city}' with max_profiles={max_profiles}")
    scraped_data = []

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.route("**/*", lambda route: route.abort() if route.request.resource_type in ["image", "stylesheet", "font"] else route.continue_())

        try:
            logging.info("Performing initial search...")
            page.goto(SEARCH_URL, timeout=60000)
            page.select_option(CITY_DROPDOWN, city)
            page.click(SEARCH_BUTTON)
            page.wait_for_selector(PAGINATION_SELECTOR, timeout=90000)

            # --- NEW LOGIC: DETECT TOTAL PROFILES ---
            if max_profiles == -1:
                logging.info("Scrape all mode enabled. Detecting total number of profiles...")
                total_profiles_on_site = get_total_profiles(page)
                if total_profiles_on_site:
                    max_profiles = total_profiles_on_site
                    logging.info(f"Total profiles detected: {max_profiles}. Adjusting scrape limit.")
                else:
                    logging.critical("Could not determine total profiles. Aborting scrape.")
                    browser.close()
                    return # Exit the function
            
            current_page = 1
            while len(scraped_data) < max_profiles:
                logging.info(f"--- Scraping Page {current_page} ---")
                page.wait_for_selector(PROFILE_LINK_SELECTOR, timeout=30000)
                profile_links = page.locator(PROFILE_LINK_SELECTOR).all()

                if not profile_links:
                    logging.warning(f"No profiles found on page {current_page}. Stopping.")
                    break

                for link in profile_links:
                    if len(scraped_data) >= max_profiles:
                        break

                    name_for_log = link.inner_text().strip()
                    logging.info(f"Scraping profile: {name_for_log}")
                    
                    try:
                        link.click()
                        lawyer_data = scrape_profile_page(page)
                        if lawyer_data:
                            scraped_data.append(lawyer_data)
                        
                        page.go_back(wait_until="domcontentloaded")
                        page.wait_for_selector(PAGINATION_SELECTOR)
                    except Exception as e:
                        logging.error(f"Failed to process {name_for_log}: {e}")
                        page.goto(page.url)
                        page.wait_for_selector(PAGINATION_SELECTOR)

                if len(scraped_data) >= max_profiles:
                    logging.info("Max profiles limit reached.")
                    break

                try:
                    next_page_num = current_page + 1
                    logging.info(f"Attempting to navigate to page {next_page_num}...")
                    
                    next_page_button = page.locator(f"a.paginate_button:text-is('{next_page_num}')")
                    
                    if next_page_button.count() == 0:
                        logging.info("No more page number links found. Ending scrape.")
                        break

                    next_page_button.click()
                    current_page += 1
                except Exception as e:
                    logging.warning(f"Could not navigate to next page. Ending scrape. Reason: {e}")
                    break
        
        except Exception as e:
            logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)

        finally:
            browser.close()
            logging.info("--- Scrape finished or interrupted. Saving collected data. ---")
            if scraped_data:
                df = pd.DataFrame(scraped_data)
                output_file = f"alberta_lawyers_{city.lower()}.xlsx"
                df.to_excel(output_file, index=False)
                logging.info(f"🎉 Success! Saved {len(scraped_data)} profiles to {output_file}")
            else:
                logging.warning("No data was collected to save.")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Scrape lawyer data from the LSA directory.")
        parser.add_argument("city", type=str, help="The city to search for (e.g., 'Calgary', 'Edmonton').")
        parser.add_argument("--max", type=int, default=10, help="The maximum number of profiles to scrape. Use -1 to scrape all available profiles.")
        args = parser.parse_args()
        
        main(args.city, args.max)

    except Exception as e:
        logging.critical(f"A critical error occurred at the script level: {e}", exc_info=True)