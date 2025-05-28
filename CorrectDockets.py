import sys
import asyncio
from pymongo import MongoClient
from playwright.async_api import async_playwright, TimeoutError
from util import get_bounced_cases

# ---------------------------
# Configuration & Constants
# ---------------------------

# Global timeout for Playwright actions (milliseconds)
TIMEOUT_MS = 60_000  # 60 seconds

# MongoDB settings (to be provided via CLI)
USAGE_TEXT = (
    "Usage: {script} <mongodb_conn> <url>\n"
    "  <mongodb_conn>: MongoDB URI, e.g. mongodb://localhost:27017\n"
    "  <url>: URL of the court-case search page"
)

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
    df_new = get_bounced_cases(MONGO_URI)
    # Run the async scraping routine
    asyncio.run(scrape_case(df_new.to_dicts()))
