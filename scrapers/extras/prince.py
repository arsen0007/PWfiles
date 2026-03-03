import requests
import csv
import time
import json

# --- Configuration ---
# The correct URL for the API endpoint, as you discovered
API_URL = "https://lawsocietypei.ca/wp-admin/admin-ajax.php"
OUTPUT_CSV_FILE = "pei_lawyers_detailed.csv"
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Referer': 'https://lawsocietypei.ca/find-a-lawyer/'
}

def get_all_lawyers():
    """
    Performs the initial search to get the master list of all lawyers and their IDs.
    An empty search name returns all results.
    """
    print("Step 1: Fetching the master list of all lawyers...")
    
    # This payload tells the website's backend to run the 'lspei_search' function
    payload = {
        'action': 'lspei_search',
        'search[name]': ''
    }
    
    try:
        response = requests.post(API_URL, headers=HEADERS, data=payload, timeout=30)
        response.raise_for_status()
        lawyers = response.json()
        print(f"Success! Found {len(lawyers)} lawyers in the directory.")
        return lawyers
    except requests.exceptions.RequestException as e:
        print(f"Error fetching master list: {e}")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from the master list response.")
    return None

def get_lawyer_details(lawyer_id):
    """
    Fetches the detailed profile for a single lawyer using their unique ID.
    """
    if not lawyer_id:
        return None
        
    # This payload tells the backend to run the 'lspei_profile' function for a specific ID
    payload = {
        'action': 'lspei_profile',
        'profile': lawyer_id
    }
    
    try:
        response = requests.post(API_URL, headers=HEADERS, data=payload, timeout=30)
        response.raise_for_status()
        # The response is a list containing a single dictionary
        details_list = response.json()
        if details_list:
            return details_list[0]
    except requests.exceptions.RequestException as e:
        print(f"  - Could not fetch details for ID {lawyer_id}. Error: {e}")
    except (json.JSONDecodeError, IndexError):
        print(f"  - Error parsing details for ID {lawyer_id}.")
    return None

def main():
    """
    Main controller to run the scraper.
    """
    # Step 1: Get the complete list of lawyers
    master_list = get_all_lawyers()
    if not master_list:
        print("Could not retrieve the master list. Exiting.")
        return

    # Step 2: Prepare CSV and fetch details for each lawyer
    print("\nStep 2: Fetching detailed information for each lawyer...")
    with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            "FullName", "Firm", "Email", "Phone", "Fax",
            "FullAddress", "AdmitDate", "MembershipType"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        total_lawyers = len(master_list)
        for i, lawyer_summary in enumerate(master_list):
            lawyer_id = lawyer_summary.get("id")
            lawyer_name = lawyer_summary.get("fullname", f"ID {lawyer_id}")
            print(f"Processing {i+1} of {total_lawyers}: {lawyer_name}")

            details = get_lawyer_details(lawyer_id)
            if details:
                # Combine address fields into a single, clean string
                full_address = ", ".join(filter(None, [
                    details.get("address2_line1"),
                    details.get("address2_city"),
                    details.get("address2_stateorprovince"),
                    details.get("address2_postalcode")
                ]))
                
                writer.writerow({
                    "FullName": details.get("fullname"),
                    "Firm": details.get("lspei_companyname"),
                    "Email": details.get("emailaddress1"),
                    "Phone": details.get("lspei_businessphonenumber"),
                    "Fax": details.get("lspei_businessfaxnumber"),
                    "FullAddress": full_address,
                    "AdmitDate": details.get("lspei_baradmissiondate"),
                    "MembershipType": details.get("membership_type_name")
                })
            
            # Pause between detail requests to be respectful of the server
            time.sleep(0.5)

    print(f"\n--- SCRAPING COMPLETE! ---")
    print(f"All data has been saved to '{OUTPUT_CSV_FILE}'")

if __name__ == "__main__":
    main()

