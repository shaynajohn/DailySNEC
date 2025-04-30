import sys
import asyncio
from datetime import date

import polars as pl
from pymongo import MongoClient
from playwright.async_api import async_playwright, TimeoutError

# ---------------------------
# Configuration & Constants
# ---------------------------

# Expected number of new case IDs to generate per county-year
BATCH_SIZE = 10

# Mapping from county code to county name
COUNTY_MAP = {
    "01": "Douglas",
    "02": "Lancaster",
    "59": "Sarpy"
}

# Global timeout for Playwright actions (milliseconds)
TIMEOUT_MS = 60_000  # 60 seconds

# MongoDB settings (to be provided via CLI)
USAGE_TEXT = (
    "Usage: {script} <mongodb_conn> <url>\n"
    "  <mongodb_conn>: MongoDB URI, e.g. mongodb://localhost:27017\n"
    "  <url>: URL of the court-case search page"
)

# Aggregation pipeline to find the highest case number per (year, county)
AGG_PIPELINE = [
    {
        "$group": {
            "_id": {"CaseYear": "$CaseYear", "County": "$County"},
            "MaxCaseNumber": {"$max": {"$toInt": "$CaseNumber"}}
        }
    }
]


def usage():
    """
    Print usage instructions and exit.
    """
    script = sys.argv[0]
    print(USAGE_TEXT.format(script=script))
    sys.exit(1)


# ---------------------------
# Command-line argument parsing
# ---------------------------
if len(sys.argv) != 3:
    usage()

MONGO_URI = sys.argv[1]
CASE_URL = sys.argv[2]

# ---------------------------
# Helper Functions
# ---------------------------

def parse_case_info(case_str: str, county_map: dict = COUNTY_MAP) -> dict:
    """
    Parse a case identifier string into its components.

    Example input: "D 01 JV 25 0000123"
    Returns: {
        "CaseYear": 2025,
        "County": "Douglas",
        "CaseNumber": "0000123"
    }
    """
    parts = case_str.split()
    # parts[1] is county code, parts[3] is two-digit year, parts[4] is case number
    county_code = parts[1]
    year_suffix = parts[3]
    case_number = parts[4]

    # Convert '25' -> 2025
    year = 2000 + int(year_suffix)
    county = county_map.get(county_code, "Unknown")

    return {
        "CaseYear": year,
        "County": county,
        "CaseNumber": case_number
    }


def get_new_batch() -> pl.DataFrame:
    """
    Generate the next batch of case IDs to scrape based on stored data.

    1. Query MongoDB for the current max case number per (year, county).
    2. Build the next BATCH_SIZE IDs for each group.
    3. Parse each into structured columns using Polars.
    4. Add metadata columns (TimeScraped, Docket, DateOfBirth).
    """
    # Connect to MongoDB and aggregate
    client = MongoClient(MONGO_URI)
    db = client["JVCases"]
    collection = db["Cases"]
    checkpoints = list(collection.aggregate(AGG_PIPELINE))

    # Build reverse map from county name back to code
    inv_county_map = {v: k for k, v in COUNTY_MAP.items()}

    # Generate next IDs
    raw_ids = []
    for ckpt in checkpoints:
        year_suffix = str(ckpt["_id"]["CaseYear"] - 2000)
        county_code = inv_county_map.get(ckpt["_id"]["County"], "00")
        # next sequential case numbers, zero-padded to 7 digits
        start_num = int(ckpt["MaxCaseNumber"]) + 1
        for offset in range(BATCH_SIZE):
            num_str = str(start_num + offset).zfill(7)
            raw_ids.append(f"D {county_code} JV {year_suffix} {num_str}")

    # Create DataFrame and parse into columns
    df = pl.DataFrame({"CaseID": raw_ids})
    df = df.with_columns(
        pl.col("CaseID").map_elements(parse_case_info).alias("parsed"),
        pl.lit(date.today()).cast(pl.Datetime).alias("TimeScraped"),
        pl.lit(None).alias("Docket"),
        pl.lit(None).alias("DateOfBirth")
    ).unnest("parsed")

    return df


async def scrape_case(cases: list[dict], url: str = CASE_URL) -> None:
    """
    Use Playwright to scrape each case’s docket page and store results in MongoDB.

    For each case dict:
      - Navigate to the search URL
      - Fill form fields: court type, county, case type, year, ID
      - Submit and wait for network idle
      - If "Case Summary" found in HTML, insert into MongoDB
      - Otherwise, log "Not Available"
      - Handle timeouts by logging "ERROR"
    """
    client = MongoClient(MONGO_URI)
    db = client["JVCases"]
    collection = db["Cases"]

    async with async_playwright() as pw:
        # Launch headless browser
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Set default timeouts
        page.set_default_timeout(TIMEOUT_MS)
        page.set_default_navigation_timeout(TIMEOUT_MS)

        for case in cases:
            try:
                # Open the search page
                await page.goto(url)

                # Fill in form inputs
                await page.select_option("#court_type", "D")
                await page.select_option("#county_num", str(case["County"]))
                await page.select_option("#case_type", "JV")
                # two-digit year suffix
                year_suffix = str(case["CaseYear"] - 2000)
                await page.fill("#case_year", year_suffix)
                await page.fill("#case_id", str(case["CaseNumber"]))

                # Submit search
                await page.click("#search")
                # Wait until network idle (max 2 connections for >= 500ms)
                await page.wait_for_load_state("networkidle")

                html = await page.content()
                if "Case Summary" in html:
                    # Save docket HTML and insert into MongoDB
                    case_record = case.copy()
                    case_record["Docket"] = html
                    collection.insert_one(case_record)
                else:
                    print("Not Available for case:", case)

            except TimeoutError:
                print("ERROR scraping case:", case)

        # Clean up browser resources
        await context.close()
        await browser.close()


if __name__ == "__main__":
    # Generate new cases to scrape
    df_new = get_new_batch()
    # Run the async scraping routine
    asyncio.run(scrape_case(df_new.to_dicts()))
