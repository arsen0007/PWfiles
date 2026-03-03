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


def get_total_entries(page, city) -> int | None:
    """
    Finds and parses the total number of entries for a specific city.
    """
    try:
        logging.info(f"Detecting total entries for city: {city}...")
        page.goto(SEARCH_URL, timeout=30000)
        page.select_option(CITY_DROPDOWN, city)
        page.click(SEARCH_BUTTON)
        page.wait_for_selector(PAGINATION_SELECTOR, timeout=30000)
        
        info_locator = page.locator("#member-directory_info")
        info_text = info_locator.inner_text(timeout=5000)
        
        match = re.search(r'of ([\d,]+)', info_text)
        if match:
            number_str = match.group(1)
            total = int(number_str.replace(',', ''))
            logging.info(f"Total entries detected for {city}: {total}")
            return total
        return None
    except Exception as e:
        logging.error(f"Could not extract total profile count for {city}: {e}")
        return None

def prompt_and_parse_range(total_available: int) -> tuple[int | None, int | None]:
    """
    Prompts the user to enter a profile range and validates it.

    Args:
        total_available: The total number of profiles found on the site.

    Returns:
        A tuple containing the start and end profile numbers, or (None, None) if input is invalid.
    """
    prompt_message = (
        f"\n✅ Found {total_available} total profiles. "
        f"Enter the range to scrape (e.g., '70-200', or '50' for 50 to end).\n"
        f"Press Enter to scrape all: "
    )
    user_input = input(prompt_message).strip()

    if not user_input:
        logging.info("No range specified. Defaulting to scrape all profiles.")
        return 1, total_available

    try:
        if '-' in user_input:
            start_str, end_str = [s.strip() for s in user_input.split('-', 1)]
            start = int(start_str) if start_str else 1
            end = int(end_str) if end_str else total_available
        else:
            start = int(user_input)
            end = total_available

        if not (1 <= start <= end <= total_available):
            logging.error(f"Invalid range. Values must be between 1 and {total_available}. You entered: {start}-{end}")
            return None, None
            
        logging.info(f"User selected range: Scraping profiles from {start} to {end}")
        return start, end
    except ValueError:
        logging.error("Invalid input. Please enter numbers in the format 'start-end' or a single 'start' number.")
        return None, None

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

    logging.info(f"Successfully scraped data for: {name}")
    return {
        "Name": name, "Email": email_val, "Phone": phone_val, "Practising Status": status_val,
        "Enrolment Date": enrol_val, "Practice Name": practice_name_val, "Address": address_val
    }


def main(city, profiles_per_page=10, retries=2):
    """
    Main function to orchestrate the scraping process.
    """
    scraped_data = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # --- Determine total profiles and get user-defined range ---
        total_on_site = get_total_entries(page, city)
        if not total_on_site:
            logging.critical("Could not determine total profiles. Aborting.")
            browser.close()
            return

        start_profile, end_profile = prompt_and_parse_range(total_on_site)
        if start_profile is None:
            logging.critical("Invalid range provided. Aborting script.")
            browser.close()
            return
        
        profiles_in_range = end_profile - start_profile + 1
        logging.info(f"Starting scrape for city='{city}' with a target of {profiles_in_range} profiles (from {start_profile} to {end_profile}).")
        
        try:
            should_stop_scraping = False
            # --- Main scraping loop now iterates over the user-defined range ---
            for i in range(start_profile - 1, end_profile):
                for attempt in range(retries):
                    try:
                        current_item_in_range = i - (start_profile - 1) + 1
                        target_page = i // profiles_per_page
                        index_on_page = i % profiles_per_page
                        
                        logging.info(f"--- Preparing to scrape profile {i+1} (Item {current_item_in_range}/{profiles_in_range}) ---")
                        logging.info(f"Navigating to results page {target_page + 1} to find item {index_on_page + 1}")

                        page.goto(SEARCH_URL, timeout=60000)
                        page.select_option(CITY_DROPDOWN, city)
                        page.click(SEARCH_BUTTON)
                        page.wait_for_selector(PAGINATION_SELECTOR, timeout=90000)

                        # Navigate to the correct page
                        for page_click in range(target_page):
                            next_button = page.locator("a.paginate_button.next")
                            if not next_button.is_enabled():
                                raise Exception(f"Cannot navigate to page {target_page + 1}, 'Next' button is disabled.")
                            next_button.click()
                            page.wait_for_selector(f"a.paginate_button.current:has-text('{page_click + 2}')", timeout=15000)

                        profile_links = page.locator(PROFILE_LINK_SELECTOR)
                        if index_on_page >= profile_links.count():
                            logging.warning(f"No more profiles found on page {target_page + 1}. This may be the end.")
                            should_stop_scraping = True
                            break
                        
                        profile_to_click = profile_links.nth(index_on_page)
                        profile_to_click.click()

                        lawyer_data = scrape_profile_page(page)
                        if lawyer_data:
                            scraped_data.append(lawyer_data)
                        break 
                    
                    except Exception as e:
                        logging.error(f"Attempt {attempt + 1} failed for profile {i+1}: {e}")
                        if attempt >= retries - 1:
                            logging.error(f"All retries failed for profile {i+1}. Skipping.")
                
                if should_stop_scraping:
                    logging.info("Stopping scrape as no more profiles were found.")
                    break
        finally:
            browser.close()
            logging.info("--- Scrape finished or interrupted. Saving collected data. ---")
            if scraped_data:
                df = pd.DataFrame(scraped_data)
                output_file = f"alberta_lawyers_{city.lower()}_{start_profile}-{end_profile}.xlsx"
                df.to_excel(output_file, index=False)
                logging.info(f"🎉 Success! Saved {len(scraped_data)} profiles to {output_file}")
            else:
                logging.warning("No data was collected to save.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape lawyer data from the LSA directory.")
    parser.add_argument("city", type=str, help="The city to search for (e.g., 'Calgary', 'Edmonton').")
    args = parser.parse_args()
    
    main(args.city)