import streamlit as st
import pandas as pd
import io
import sys
import asyncio
from playwright.sync_api import sync_playwright

# ✅ Windows fix for Playwright + asyncio
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

SEARCH_URL = "https://lsa.memberpro.net/main/body.cfm?menu=directory&submenu=directoryPractisingMember&action=searchTop"

# ----------------------
# Function 1: Scrape City
# ----------------------
def scrape_city(city):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(SEARCH_URL, timeout=60000)

        # Select city
        page.select_option("select[name='city_nm']", city)

        # Click search
        page.click("a:has-text('Search')")
        page.wait_for_selector("table", timeout=20000)

        rows = page.locator("table tr").all()[1:]  # skip header row
        data = []
        for row in rows:
            cols = row.locator("td").all()
            if len(cols) >= 2:
                name = cols[0].inner_text().strip()
                status = cols[1].inner_text().strip()
                data.append({"Name": name, "Status": status})

        browser.close()
        return pd.DataFrame(data)

# ----------------------
# Function 2: Scrape Profiles by Name
# ----------------------
def scrape_profiles(df_names):
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        results = []

        for _, row in df_names.iterrows():
            name = row["Name"]
            st.write(f"➡️ Scraping profile for: {name}")

            page.goto(SEARCH_URL, timeout=60000)

            # Search by name
            page.fill("input[name='member_name']", name)
            page.click("a:has-text('Search')")
            try:
                page.wait_for_selector("table", timeout=15000)
            except:
                st.warning(f"No results for {name}")
                continue

            # Open first result
            try:
                page.click("table tr:nth-child(2) a")
                page.wait_for_selector("div.content-heading", timeout=10000)
            except:
                st.warning(f"Could not open profile for {name}")
                continue

            # Extract heading (lawyer full name at top)
            full_name = page.locator("div.content-heading").inner_text().strip()

            # Extract profile details table
            rows = page.locator("table tr").all()
            profile_data = {"Name": full_name}
            for r in rows:
                cells = r.locator("td").all()
                if len(cells) == 2:
                    label = cells[0].inner_text().strip().replace(":", "")
                    value = cells[1].inner_text().strip()
                    profile_data[label] = value

            # Normalise final output
            result = {
                "Name": profile_data.get("Name", full_name),
                "Email": profile_data.get("Email", ""),
                "Phone": profile_data.get("Phone", ""),
                "Practising Status": profile_data.get("Practising Status", ""),
                "Enrolment Date": profile_data.get("Enrolment Date", ""),
                "Practice Name": profile_data.get("Practice Name", ""),
                "Practice Location": profile_data.get("Practice Location", ""),
                "Discipline History": profile_data.get("Discipline History", ""),
            }

            results.append(result)

        browser.close()
        return pd.DataFrame(results)

# ----------------------
# Streamlit App
# ----------------------
st.title("Alberta Lawyers Scraper")

# Step 1: Scrape by City
st.header("Step 1: Scrape City List")
city = st.text_input("Enter city name (e.g., Calgary, Edmonton)")

if st.button("Scrape City"):
    df_city = scrape_city(city)
    st.dataframe(df_city)

    # ✅ Download Excel
    output = io.BytesIO()
    df_city.to_excel(output, index=False, engine="openpyxl")
    st.download_button(
        label="Download City Excel",
        data=output.getvalue(),
        file_name=f"{city}_lawyers.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# Step 2: Scrape Profiles
st.header("Step 2: Scrape Profiles")
uploaded_file = st.file_uploader("Upload Excel with Names", type=["xlsx"])

if uploaded_file is not None:
    df_names = pd.read_excel(uploaded_file)
    if st.button("Scrape Profiles"):
        df_profiles = scrape_profiles(df_names)
        st.dataframe(df_profiles)

        # ✅ Download Excel
        output = io.BytesIO()
        df_profiles.to_excel(output, index=False, engine="openpyxl")
        st.download_button(
            label="Download Profiles Excel",
            data=output.getvalue(),
            file_name="lawyer_profiles.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
