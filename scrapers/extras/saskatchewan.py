import requests
import csv
import json
import string
import itertools
import time
import re
from bs4 import BeautifulSoup

# --- Configuration ---
BASE_URL = "https://lss.alinityapp.com/client/publicdirectory"
SEARCH_API_URL = "https://lss.alinityapp.com/Client/PublicDirectory/Registrants"
# We are now correctly targeting the HTML page, NOT the inaccessible API
DETAILS_API_URL_TEMPLATE = "https://lss.alinityapp.com/Client/PublicDirectory/Registrant/{}"
OUTPUT_CSV_FILE = "saskatchewan_lawyers_final_detailed.csv"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Referer': BASE_URL
}

def get_lawyer_details(session, registrant_id):
    """
    Makes a GET request to the HTML details page and uses a robust regex to
    extract the embedded JSON data.
    """
    if not registrant_id:
        return {}

    details_url = DETAILS_API_URL_TEMPLATE.format(registrant_id)
    try:
        response = session.get(details_url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        html_content = response.text

        # This is a more robust regex to find the JSON data. It looks for the specific
        # function call we know contains the data.
        match = re.search(r'fwkParseStringTemplate\s*\([^,]+,\s*(\{.*\})\s*\);', html_content)

        if not match:
            print(f"  - Could not find the hidden JSON data on the page for ID {registrant_id}.")
            return {}
        
        json_string = match.group(1)
        details_data = json.loads(json_string)

        # Extract the fields you requested from the now-accessible JSON
        email = details_data.get("ea", "")
        admit_date = details_data.get("ir", "")
        reg_type = details_data.get("pr", "")
        
        # The address data is stored as a string of HTML, so we parse it
        employment_html = details_data.get("e", "")
        full_address = ""
        
        if employment_html:
            soup = BeautifulSoup(employment_html, 'html.parser')
            cells = soup.find_all('td')
            if len(cells) >= 2:
                # We extract all text pieces and join them with a space
                full_address = ' '.join(cells[1].stripped_strings)

        return {
            "Email": email,
            "AdmitDate": admit_date,
            "RegistrationType": reg_type,
            "FullAddress": full_address
        }

    except requests.exceptions.RequestException as e:
        print(f"  - Could not fetch details page for ID {registrant_id}. Error: {e}")
    except (json.JSONDecodeError, AttributeError) as e:
        print(f"  - Failed to parse details from HTML for ID {registrant_id}. Error: {e}")
    
    return {}

def scrape_for_combo(session, query_combo, csv_writer, processed_fingerprints):
    """
    Sends a POST request to search and then GETs the HTML details page for each result.
    """
    print(f"\n----- Searching for last names starting with: '{query_combo.upper()}' -----")

    post_headers = HEADERS.copy()
    post_headers['Content-Type'] = 'application/x-www-form-urlencoded; charset=UTF-8'
    post_headers['X-Requested-With'] = 'XMLHttpRequest'
    
    query_params = { "Parameter": [ {"ID": "TextOptionA", "Value": query_combo, "ValueLabel": "[not entered]"}, {"ID": "TextOptionD", "Value": "", "ValueLabel": "[not entered]"}, {"ID": "TextOptionB", "Value": "", "ValueLabel": "[not entered]"}, {"ID": "GenderPronoun", "Value": "-", "ValueLabel": "[not entered]"}, {"ID": "TextOptionC", "Value": "", "ValueLabel": "[not entered]"}, {"ID": "TextOptionE", "Value": "", "ValueLabel": "[not entered]"}, {"ID": "SpecializationSID", "Value": "-", "ValueLabel": "[not entered]"}, {"ID": "ServiceModels", "Value": "-", "ValueLabel": "[not entered]"}, {"ID": "PricingModels", "Value": "-", "ValueLabel": "[not entered]"}, {"ID": "ExperienceRange", "Value": "-", "ValueLabel": "[not entered]"} ] }
    payload = { 'queryParameters': json.dumps(query_params), 'querySID': '1000601' }

    try:
        response = session.post(SEARCH_API_URL, headers=post_headers, data=payload, timeout=30)
        response.raise_for_status()
        data = response.json()

        records = data.get("Records", [])
        if not records:
            print(f"No results found for '{query_combo}'. Skipping.")
            return
        
        print(f"Found {len(records)} records for '{query_combo}'. Fetching details...")

        for record in records:
            name = record.get("rl", "")
            firm = record.get("pe", "")
            fingerprint = f"{name}|{firm}"

            if fingerprint not in processed_fingerprints:
                processed_fingerprints.add(fingerprint)
                print(f"  - Processing: {name}")
                
                basic_info = { "Name": name, "Firm": firm, "Phone": record.get("ph", "") }
                
                registrant_id = record.get("rg")
                detailed_info = get_lawyer_details(session, registrant_id)
                time.sleep(0.5)
                
                full_record = {**basic_info, **detailed_info}
                csv_writer.writerow(full_record)

    except requests.exceptions.RequestException as e:
        print(f"  ERROR: A network error occurred for '{query_combo}': {e}")
    except Exception as e:
        print(f"  ERROR: A general error occurred for '{query_combo}': {e}")

def main():
    print("--- Starting Law Society of Saskatchewan Scraper (Final Corrected Version) ---")
    
    session = requests.Session()
    print("Initializing session...")
    try:
        session.get(BASE_URL, headers=HEADERS, timeout=30)
        print("Session established successfully.")
    except requests.exceptions.RequestException as e:
        print(f"Could not establish a session. Exiting. Error: {e}")
        return

    alphabet = string.ascii_lowercase
    search_combos = [''.join(p) for p in itertools.product(alphabet, repeat=2)]
    
    processed_fingerprints = set()
    
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["Name", "Firm", "Phone", "Email", "AdmitDate", "RegistrationType", "FullAddress"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        total_combos = len(search_combos)
        for i, combo in enumerate(search_combos):
            print(f"\nProcessing combination {i+1} of {total_combos}")
            scrape_for_combo(session, combo, writer, processed_fingerprints)
            time.sleep(1)
            
    print(f"\n--- SCRAPING COMPLETE! ---")
    print(f"All data has been saved to '{OUTPUT_CSV_FILE}'")
    print(f"Found a total of {len(processed_fingerprints)} unique lawyers.")

if __name__ == "__main__":
    main()

