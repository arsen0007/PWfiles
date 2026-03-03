#!/usr/bin/env python3
import logging
import argparse
import pandas as pd
import re
import json
import os
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

# --- Checkpoint / Progress files ---
PROGRESS_FILE = "progress.json"
SKIP_CITIES = {"Calgary", "Edmonton"}  # user requested excluded cities

def load_progress():
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logging.warning(f"Could not read progress file: {e}. Starting fresh.")
    # default structure
    return {"completed": [], "cities": {}}  # cities: map city -> next_profile_to_scrape (1-based)

def save_progress(progress):
    try:
        with open(PROGRESS_FILE, "w", encoding="utf-8") as f:
            json.dump(progress, f, indent=2)
    except Exception as e:
        logging.error(f"Failed to save progress file: {e}")

def append_partial_csv(city, scraped_rows):
    """
    Append scraped_rows (list of dicts) to a partial CSV for the city.
    This is called frequently (after each profile) to minimize lost work.
    """
    partial_file = f"alberta_lawyers_{city.lower().replace(' ', '_')}_partial.csv"
    df = pd.DataFrame(scraped_rows)
    # If file exists, append without header; else write with header
    if os.path.exists(partial_file):
        df.to_csv(partial_file, mode="a", index=False, header=False)
    else:
        df.to_csv(partial_file, index=False)

def finalize_city_files(city, scraped_rows, start_profile, end_profile):
    """
    Save final excel and remove partial file. Use the same naming logic as before.
    """
    if not scraped_rows:
        logging.warning(f"No data collected for {city}; nothing to save.")
        return
    df = pd.DataFrame(scraped_rows)
    output_file = f"alberta_lawyers_{city.lower().replace(' ', '_')}_{start_profile}-{end_profile}.xlsx"
    df.to_excel(output_file, index=False)
    logging.info(f"🎉 Success! Saved {len(scraped_rows)} profiles to {output_file}")

    # remove partial file if exists
    partial_file = f"alberta_lawyers_{city.lower().replace(' ', '_')}_partial.csv"
    try:
        if os.path.exists(partial_file):
            os.remove(partial_file)
    except Exception as e:
        logging.warning(f"Could not remove partial file {partial_file}: {e}")

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
    Scrapes the data from the currently loaded profile page,
    capturing up to two valid email addresses in separate columns.
    """
    try:
        page.wait_for_selector(PROFILE_HEADING, timeout=15000)
    except TimeoutError:
        logging.warning("Timed out waiting for profile page to load. Skipping.")
        return None

    # --- Name ---
    name = page.locator(PROFILE_HEADING).inner_text().strip()

    # --- Email(s) ---
    email_1, email_2 = None, None
    try:
        email_locator = page.locator("a[href^='mailto']")
        valid_emails = []
        if email_locator.count() > 0:
            for i in range(email_locator.count()):
                # use nth to avoid strict-mode errors and to iterate multiples
                try:
                    text = email_locator.nth(i).inner_text().strip()
                except Exception:
                    # fallback: attribute extraction if inner_text fails
                    try:
                        text = email_locator.nth(i).get_attribute("href") or ""
                        # mailto:some@domain.com -> strip prefix
                        if text.lower().startswith("mailto:"):
                            text = text.split(":", 1)[1]
                    except Exception:
                        text = ""
                # simple email pattern validation
                if re.match(r"[^@]+@[^@]+\.[^@]+", text) and text not in valid_emails:
                    valid_emails.append(text)

        # assign up to two emails
        if len(valid_emails) >= 1:
            email_1 = valid_emails[0]
        if len(valid_emails) >= 2:
            email_2 = valid_emails[1]

    except Exception as e:
        logging.warning(f"Could not extract email(s) for {name}: {e}")

    # --- Phone ---
    phone_val = None
    try:
        label_locator = page.locator('td.form-label:has-text("Office")')
        if label_locator.count() > 0:
            phone_val = label_locator.locator(
                "xpath=./following-sibling::td"
            ).first.inner_text(timeout=2000).strip()
    except TimeoutError:
        logging.warning(f"Could not extract phone number for {name}.")

    # --- Practising Status / Enrolment ---
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

    # --- Practice Name & Address ---
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
        "Name": name,
        "Email": email_1,
        "Second Email": email_2,
        "Phone": phone_val,
        "Practising Status": status_val,
        "Enrolment Date": enrol_val,
        "Practice Name": practice_name_val,
        "Address": address_val
    }

def get_all_cities_from_dropdown(page) -> list:
    """
    Return a list of city option 'values' or visible texts (prefer values if present)
    and filter out blank/placeholder options and the SKIP_CITIES.
    """
    page.goto(SEARCH_URL, timeout=60000)
    options = page.locator(f"{CITY_DROPDOWN} option")
    city_items = []
    for i in range(options.count()):
        try:
            # prefer the option's value attribute; if empty, use visible text
            opt = options.nth(i)
            val = opt.get_attribute("value") or opt.inner_text().strip()
            text = opt.inner_text().strip()
            # skip placeholder blanks like '' or 'Select City'
            if not val or val.lower().startswith("select"):
                continue
            # use displayed text for human-readable name, but selection by value often works too
            # we'll return a tuple (value, text)
            city_items.append((val, text))
        except Exception:
            continue
    # Filter out skip list by text
    city_items = [c for c in city_items if c[1] not in SKIP_CITIES]
    return city_items

def scrape_city(page, city_value, city_text, resume_next_profile=None, profiles_per_page=10, retries=2, auto_resume=False, manual_range=True):
    """
    Scrape a single city. Supports resume_next_profile (1-based).
    manual_range: if True will prompt for range; if False, scrape from resume or start -> total.
    auto_resume: if True, do not prompt to resume; resume automatically if resume_next_profile present.
    """
    scraped_data = []
    partial_rows_to_write = []  # buffer rows to append to partial CSV frequently

    # Determine total profiles
    total_on_site = get_total_entries(page, city_value)
    if not total_on_site:
        logging.critical(f"Could not determine total profiles for {city_text}. Skipping city.")
        return scraped_data, None, None  # nothing saved

    # Decide start/end
    if resume_next_profile and 1 <= resume_next_profile <= total_on_site:
        if auto_resume:
            start_profile = resume_next_profile
            logging.info(f"Auto-resuming {city_text} from profile {start_profile} (per progress).")
        else:
            ans = input(f"Found previous progress for {city_text}. Resume from profile {resume_next_profile}? (Y/n): ").strip().lower()
            if ans in ("", "y", "yes"):
                start_profile = resume_next_profile
                logging.info(f"Resuming {city_text} from profile {start_profile}.")
            else:
                # ask for new range if manual_range true, else start from 1
                if manual_range:
                    sp, ep = prompt_and_parse_range(total_on_site)
                    if sp is None:
                        logging.critical("Invalid range provided. Aborting city.")
                        return [], None, None
                    start_profile, end_profile = sp, ep
                else:
                    start_profile = 1
    else:
        # no resume: either prompt for range (manual run) or scrape all (auto via --all)
        if manual_range:
            sp, ep = prompt_and_parse_range(total_on_site)
            if sp is None:
                logging.critical("Invalid range provided. Aborting city.")
                return [], None, None
            start_profile, end_profile = sp, ep
        else:
            start_profile, end_profile = 1, total_on_site

    # Ensure end_profile exists if not set above
    if 'end_profile' not in locals():
        end_profile = total_on_site

    profiles_in_range = end_profile - start_profile + 1
    logging.info(f"Starting scrape for city='{city_text}' with a target of {profiles_in_range} profiles (from {start_profile} to {end_profile}).")

    # main loop: i is 1-based profile number in overall list
    try:
        should_stop_scraping = False
        for profile_num in range(start_profile, end_profile + 1):
            # Update progress file BEFORE trying to avoid re-scraping on immediate crash? we'll update next_profile after success.
            for attempt in range(retries):
                try:
                    current_item_in_range = profile_num - start_profile + 1
                    zero_based_index = profile_num - 1
                    target_page = zero_based_index // profiles_per_page
                    index_on_page = zero_based_index % profiles_per_page

                    logging.info(f"--- Preparing to scrape profile {profile_num} (Item {current_item_in_range}/{profiles_in_range}) ---")
                    logging.info(f"Navigating to results page {target_page + 1} to find item {index_on_page + 1}")

                    page.goto(SEARCH_URL, timeout=60000)
                    page.select_option(CITY_DROPDOWN, city_value)
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
                        partial_rows_to_write.append(lawyer_data)
                        # write partial CSV (append this single new row)
                        append_partial_csv(city_text, [lawyer_data])
                        # update progress: set next_profile_to_scrape = profile_num + 1
                        progress = load_progress()
                        progress.setdefault("cities", {})[city_text] = profile_num + 1
                        save_progress(progress)
                    break  # success -> break retry loop

                except Exception as e:
                    logging.error(f"Attempt {attempt + 1} failed for profile {profile_num}: {e}")
                    if attempt >= retries - 1:
                        logging.error(f"All retries failed for profile {profile_num}. Skipping.")
                        # Mark the next profile as current + 1 so resume will pick up after this skipped one
                        progress = load_progress()
                        progress.setdefault("cities", {})[city_text] = profile_num + 1
                        save_progress(progress)
                # end retry
            if should_stop_scraping:
                logging.info("Stopping scrape as no more profiles were found.")
                break
    finally:
        # on exit of city scraping: either save final file or leave partial for resume
        if scraped_data:
            # finalize and save excel for this range
            finalize_city_files(city_text, scraped_data, start_profile, min(end_profile, start_profile + len(scraped_data) - 1))
        else:
            logging.warning(f"No data collected for {city_text} in this run.")

    # If we finished the entire requested range, mark city as completed
    if (not should_stop_scraping) and (end_profile == total_on_site or end_profile <= total_on_site):
        # If next_profile in progress is > end_profile, it means we've progressed past requested end
        progress = load_progress()
        next_profile = progress.get("cities", {}).get(city_text, 1)
        if next_profile > end_profile:
            progress.setdefault("completed", [])
            if city_text not in progress["completed"]:
                progress["completed"].append(city_text)
            # remove city resume entry
            progress.get("cities", {}).pop(city_text, None)
            save_progress(progress)
            logging.info(f"Marked city {city_text} as completed in progress file.")

    return scraped_data, start_profile, end_profile

def main(city_arg=None, profiles_per_page=10, retries=2, all_mode=False):
    """
    Orchestration of all-city or single-city runs.
    If all_mode is True -> scrape all cities (except SKIP_CITIES) automatically.
    If all_mode is False -> requires city_arg (string) to scrape single city (with prompt for range).
    """
    progress = load_progress()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        try:
            if all_mode:
                # fetch cities from dropdown
                city_items = get_all_cities_from_dropdown(page)  # list of (value, text)
                if not city_items:
                    logging.critical("No cities found in dropdown. Aborting.")
                    return
                for (city_value, city_text) in city_items:
                    # skip if in skip list
                    if city_text in SKIP_CITIES:
                        logging.info(f"Skipping {city_text} (explicit exclude).")
                        continue
                    # skip if completed
                    if city_text in progress.get("completed", []):
                        logging.info(f"Skipping {city_text} (already completed in progress file).")
                        continue
                    resume_next = progress.get("cities", {}).get(city_text)
                    logging.info(f"Beginning city: {city_text}")
                    # In all_mode we auto_resume and do not prompt for manual range; scrape full city
                    scraped_rows, start_p, end_p = scrape_city(
                        page, city_value, city_text, resume_next_profile=resume_next,
                        profiles_per_page=profiles_per_page, retries=retries,
                        auto_resume=True, manual_range=False
                    )
                    # refresh progress data after each city iteration
                    progress = load_progress()
                logging.info("All-city run finished (or reached previously completed cities).")
            else:
                if not city_arg:
                    logging.critical("No city provided and --all not specified. Aborting.")
                    return
                # run single city flow (manual range prompts)
                page.goto(SEARCH_URL, timeout=60000)  # initial load
                # We pass the city argument as selection value or visible text - try both
                # get resume if exists
                resume_next = progress.get("cities", {}).get(city_arg)
                scraped_rows, start_p, end_p = scrape_city(
                    page, city_arg, city_arg, resume_next_profile=resume_next,
                    profiles_per_page=profiles_per_page, retries=retries,
                    auto_resume=False, manual_range=True
                )
                # If entire requested range completed, progress marking handled inside scrape_city
        finally:
            browser.close()
            logging.info("Browser closed. Exiting main.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape lawyer data from the LSA directory.")
    parser.add_argument("city", nargs="?", default=None, help="The city to search for (e.g., 'Calgary', 'Edmonton'). Optional when using --all.")
    parser.add_argument("--all", action="store_true", help="Scrape all cities found in the dropdown (except Calgary & Edmonton).")
    args = parser.parse_args()

    if args.all:
        main(city_arg=None, all_mode=True)
    else:
        # require city positional argument
        if not args.city:
            logging.critical("Please provide a city (e.g. 'Red Deer') or use --all to scrape all cities.")
        else:
            main(city_arg=args.city, all_mode=False)
