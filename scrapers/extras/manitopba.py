import requests
import csv
from bs4 import BeautifulSoup
import math
import time
import string
import itertools

# --- Configuration ---
BASE_URL = "https://portal.lawsociety.mb.ca/lookup/action.php"
# The server is hardcoded to return 15 results per page.
RESULTS_PER_PAGE = 15
# Output file name
OUTPUT_CSV_FILE = "manitoba_lawyers_accurate.csv"
# Headers to make our script look like a real browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://lawsociety.mb.ca/'
}

def parse_lawyer_data(row):
    """
    Parses a single table row (<tr>) and extracts lawyer information.
    """
    cells = row.find_all('td')
    if len(cells) < 4: return None
    
    contact_cell = cells[0]
    name = contact_cell.strong.get_text(strip=True) if contact_cell.strong else ''
    if not name: return None # Skip if there's no name

    email = contact_cell.a.get_text(strip=True) if contact_cell.a else ''
    phone, fax, address_parts = '', '', []
    
    lines = [line.strip() for line in contact_cell.get_text(separator='\n').split('\n') if line.strip()]

    for line in lines:
        if line == name or line == f"Email:{email}": continue
        if line.startswith('Phone:'): phone = line.replace('Phone:', '').strip()
        elif line.startswith('Fax:'): fax = line.replace('Fax:', '').strip()
        else: address_parts.append(line)
        
    address = ', '.join(address_parts)
    firm = cells[1].get_text(strip=True)
    status = cells[2].get_text(strip=True)
    
    return {"Name": name, "Firm": firm, "Status": status, "Phone": phone, "Fax": fax, "Email": email, "Address": address}

def scrape_for_query(query_combo, csv_writer, processed_fingerprints):
    """
    Scrapes all pages for a single two-letter query (e.g., 'ab') and writes new entries to the CSV.
    """
    print(f"\n----- Searching for: '{query_combo.upper()}' -----")
    try:
        # Step 1: Get total results for this query
        params = {'query': query_combo, 'sort': 'contact', 'dir': '1', 'page': 1, 'rp': RESULTS_PER_PAGE, '_': int(time.time() * 1000)}
        response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        total_results_tag = soup.find('span', id='rc')

        if not total_results_tag or not total_results_tag.get_text(strip=True).isdigit():
            print(f"No results found for '{query_combo}'. Skipping.")
            return

        total_results = int(total_results_tag.get_text(strip=True))
        if total_results == 0:
            print(f"No results found for '{query_combo}'. Skipping.")
            return
            
        total_pages = math.ceil(total_results / RESULTS_PER_PAGE)
        print(f"Found {total_results} lawyers. Scraping {total_pages} pages.")

        # Step 2: Loop through all pages for this query
        for page_num in range(1, total_pages + 1):
            print(f"Scraping '{query_combo.upper()}': Page {page_num} of {total_pages}...")
            params['page'] = page_num
            params['_'] = int(time.time() * 1000)
            
            page_response = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
            page_response.raise_for_status()
            page_soup = BeautifulSoup(page_response.text, 'html.parser')
            
            table_body = page_soup.find('table').find('tbody') if page_soup.find('table') else None
            if not table_body:
                print(f"  Warning: No table found on page {page_num} for '{query_combo}'.")
                continue

            rows = table_body.find_all('tr')
            for row in rows:
                lawyer_data = parse_lawyer_data(row)
                if lawyer_data:
                    # --- UPDATED DUPLICATE CHECK LOGIC ---
                    # Create a unique fingerprint from Name, Firm, and Email
                    fingerprint = f'{lawyer_data["Name"]}|{lawyer_data["Firm"]}|{lawyer_data["Email"]}'
                    
                    # Check the fingerprint instead of just the name
                    if fingerprint not in processed_fingerprints:
                        csv_writer.writerow(lawyer_data)
                        processed_fingerprints.add(fingerprint)
            
            time.sleep(1.5) # Be extra respectful of the server

    except requests.exceptions.RequestException as e:
        print(f"  ERROR: An error occurred while searching for '{query_combo}': {e}")
    except Exception as e:
        print(f"  ERROR: A general error occurred for '{query_combo}': {e}")


def main():
    """
    Main controller function to loop through all two-letter combinations.
    """
    print("--- Starting Manitoba Law Society Scraper (Accurate Version) ---")
    print("This process will take a long time. Please be patient.")
    
    # Generate all two-letter combinations from 'aa' to 'zz'
    alphabet = string.ascii_lowercase
    search_combos = [''.join(p) for p in itertools.product(alphabet, repeat=2)]
    
    # This set will now store unique fingerprints to prevent duplicates
    processed_fingerprints = set()
    
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ["Name", "Firm", "Status", "Phone", "Fax", "Email", "Address"]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        
        total_combos = len(search_combos)
        for i, combo in enumerate(search_combos):
            print(f"\nProcessing combination {i+1} of {total_combos}")
            scrape_for_query(combo, writer, processed_fingerprints)
            
    print(f"\n--- SCRAPING COMPLETE! ---")
    print(f"All data has been saved to '{OUTPUT_CSV_FILE}'")
    print(f"Found a total of {len(processed_fingerprints)} unique lawyers.")

if __name__ == "__main__":
    main()
