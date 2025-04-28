#!/usr/bin/env python3
"""
Simplified scraper: takes URL as command-line argument, selects Lancaster county and year 2025, performs search, then exits.
"""
import sys
import asyncio
from playwright.async_api import async_playwright

# Ensure URL is provided as an argument
def usage():
    print(f"Usage: {sys.argv[0]} <url>")
    sys.exit(1)

if len(sys.argv) != 2:
    usage()

url = sys.argv[1]
county = "Lancaster"
year = "2025"

async def run():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            page = await browser.new_page()
            await page.goto(url)

            # Select county
            await page.locator("div#county_num_chosen").click()
            await page.locator("div#county_num_chosen").get_by_text(county).click()

            # Select all court types
            await page.locator("div#court_type_chosen").click()
            await page.locator("div#court_type_chosen").get_by_text("All Court Types").click()

            # Select juvenile cases
            await page.locator("div#case_type_chosen").click()
            await page.locator("div#case_type_chosen").get_by_text("Juvenile").click()

            # Select all judges
            await page.locator("div#judge_chosen").click()
            await page.locator("div#judge_chosen").get_by_text("All Judges").click()

            # Select all attorneys
            await page.locator("div#attorney_name_chosen").click()
            await page.locator("div#attorney_name_chosen").get_by_text("All Attorneys").click()

            # Select specified year
            await page.select_option("select#year", value=[year])

            # Sort by CaseNum descending
            await page.select_option("select#sort", value=["casenum"])
            await page.locator("label", has_text="Descending").click()

            # Perform search
            await page.get_by_role("button", name="search").click()

             ## Check if any results are returned, if not pass
            no_results = await page.locator("div#info.alert.alert-info").is_visible()

            if no_results:
                print("Failed")
            else:
                content = await page.content()
                print(content)

        finally:
            await browser.close()

if __name__ == "__main__":
    asyncio.run(run())
