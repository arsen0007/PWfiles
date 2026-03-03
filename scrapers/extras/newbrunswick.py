import requests
import json

TARGET_CITY = "Saint John"
MAIN_PAGE_URL = "https://lsnb.alinityapp.com/client/PublicDirectory"
API_URL = "https://lsnb.alinityapp.com/client/PublicDirectory/Registrants"

# We'll use a full set of headers to look like a browser
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.5',
    'Referer': MAIN_PAGE_URL,
    'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'X-Requested-With': 'XMLHttpRequest',
    'Origin': 'https://lsnb.alinityapp.com',
    'Connection': 'keep-alive',
}

# The search payload
PAYLOAD = {
    'queryParameters': f'{{"Parameter":[{{"ID":"TextOptionA","Value":""}},{{"ID":"TextOptionD","Value":"{TARGET_CITY}"}},{{"ID":"SpecializationSID","Value":"-"}},{{"ID":"TextOptionB","Value":""}},{{"ID":"TextOptionC","Value":""}},{{"ID":"GenderPronoun","Value":"-"}},{{"ID":"TextOptionE","Value":""}}]}}',
    'querySID': '1000602'
}

def test_api_method():
    print("--- Starting Final Direct API Test ---")
    
    # Using a Session object is key, as it stores cookies automatically
    with requests.Session() as session:
        try:
            # Step 1: "Warm-up" - Visit the main page to get session cookies
            print(f"1. Making a warm-up request to {MAIN_PAGE_URL} to establish a session...")
            warmup_response = session.get(MAIN_PAGE_URL, headers=HEADERS, timeout=15)
            warmup_response.raise_for_status()
            print(f"   -> Warm-up successful. Status: {warmup_response.status_code}")
            
            # The session object now has any cookies the site sent.
            print("   -> Cookies stored in session:", session.cookies.get_dict())

            # Step 2: Make the actual API POST request using the same session
            print(f"\n2. Making the POST request to the API at {API_URL}...")
            api_response = session.post(API_URL, headers=HEADERS, data=PAYLOAD, timeout=15)
            
            # Check the result
            print(f"   -> API response status code: {api_response.status_code}")
            api_response.raise_for_status() # This will raise an error if status is 4xx or 5xx
            
            # If we get here, it means we succeeded!
            print("\n--- SUCCESS! ---")
            print("Successfully bypassed security with the API method.")
            
            data = api_response.json()
            records = data.get("Records", [])
            print(f"Found {len(records)} lawyers in {TARGET_CITY}.")
            print("\n--- Sample Data ---")
            for record in records[:5]:
                print(f"  Name: {record.get('rl')}, Status: {record.get('prl')}")
            print("-------------------\n")

        except requests.exceptions.HTTPError as e:
            print("\n--- TEST FAILED ---")
            if e.response.status_code == 503:
                print("Result: Got a 503 Service Unavailable error again.")
                print("This confirms the server is blocking our API request even with a valid session.")
                print("Conclusion: The JavaScript challenge is mandatory, and browser automation is required.")
            else:
                print(f"An HTTP error occurred: {e}")
        except requests.exceptions.RequestException as e:
            print(f"\nA network error occurred: {e}")

# This block ensures the function is called when you run the script
if __name__ == '__main__':
    test_api_method()