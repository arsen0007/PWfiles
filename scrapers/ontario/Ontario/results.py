# lso_search_all_by_city.py
import requests
import json
import sys
import pandas as pd

API_KEY = "212D535962D4563E62F8EC5D6E1C71CA"
URL = "https://lawsocietyontario.search.windows.net/indexes/lsolpindexprd/docs/search?api-version=2017-11-11"

HEADERS = {
    "Content-Type": "application/json",
    "api-key": API_KEY,
    "Accept": "application/json"
}

def search_city_all(city_token="Windsor", license_type="L1", batch_size=1000):
    """Pull ALL results for a city with pagination."""
    all_rows = []
    skip = 0
    total = None

    while True:
        payload = {
            "search": "*",
            "count": True,
            "top": batch_size,
            "skip": skip,
            "filter": f"memberlicencetype/any(m: m eq '{license_type}') and membercitynormalized/any(m: m eq '{city_token}')",
            "orderby": "memberlastname,memberfirstname,membermiddlename",
            "queryType": "full",
            "searchFields": "memberfirstname,membermiddlename,memberlastname,memberfullname,membermailname,memberfirstnameclean,membermiddlenameclean,memberlastnameclean,membermailnameclean"
        }

        r = requests.post(URL, headers=HEADERS, json=payload, timeout=60)
        if r.status_code != 200:
            print("Request failed:", r.status_code, r.text[:1000])
            break

        data = r.json()

        if total is None:
            total = data.get("@odata.count") or data.get("odata.count")
            print(f"\nTotal profiles for {city_token}: {total}")

        batch = data.get("value", [])
        print(f"Fetched {len(batch)} profiles (skip={skip})")

        if not batch:
            break

        for item in batch:
            all_rows.append({
                "MemberNumber": item.get("membernumber"),
                "FullName": item.get("memberfullname"),
                "Status": item.get("memberwebstatus"),
                "City": item.get("membercity"),
                "Province": item.get("memberprovincetext"),
                "Country": item.get("membercountrytext")
            })

        skip += batch_size
        if total and skip >= total:
            break

    return pd.DataFrame(all_rows)

if __name__ == "__main__":
    city = "Windsor"
    if len(sys.argv) > 1:
        city = sys.argv[1]

    df = search_city_all(city_token=city, license_type="L1", batch_size=1000)
    if not df.empty:
        filename = f"lso_{city}_all_results.csv"
        df.to_csv(filename, index=False)
        print(f"\n✅ Saved {len(df)} profiles to {filename}")
    else:
        print("\n⚠️ No results found.")
