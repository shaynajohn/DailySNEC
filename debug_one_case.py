"""
Temporary one-case debug scraper.

Does NOT connect to MongoDB and does NOT insert or delete anything.
Uses the same Playwright form-filling logic as UpdateDockets.py.

Usage:
  python debug_one_case.py <court_search_url>
  python debug_one_case.py <court_search_url> "D 01 JV 25 0001547"
  python debug_one_case.py <court_search_url> "D 01 JV 25 0001546" "D 01 JV 25 0001547" "D 01 JV 26 0000001"

Pass --try-county-code to also retry the same case using county code
("01") instead of county name ("Douglas").
"""
import sys
import asyncio
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from util import COUNTY_MAP, parse_case_info

TIMEOUT_MS = 60_000
DEFAULT_CASE_ID = "D 01 JV 25 0001547"

USAGE_TEXT = (
    "Usage: {script} <url> [case_id ...]\n"
    "  <url>: URL of the court-case search page\n"
    "  [case_id ...]: optional CaseID(s), e.g. 'D 01 JV 25 0001547'\n"
    "  optional flag: --try-county-code"
)

PAGE_MARKERS = [
    "Case Summary",
    "No matching",
    "not found",
    "No cases",
    "Cloudflare",
    "cf-challenge",
    "Just a moment",
    "Access Denied",
    "captcha",
    "CAPTCHA",
    "login",
    "sign in",
    "blocked",
]


def usage():
    print(USAGE_TEXT.format(script=sys.argv[0]))
    sys.exit(1)


def parse_args(argv: list[str]) -> tuple[str, list[str], bool]:
    if len(argv) < 2:
        usage()

    try_county_code = "--try-county-code" in argv
    positional = [arg for arg in argv[1:] if arg != "--try-county-code"]

    if not positional:
        usage()

    url = positional[0]
    case_ids = positional[1:] or [DEFAULT_CASE_ID]
    return url, case_ids, try_county_code


def build_case(case_id: str) -> dict:
    parsed = parse_case_info(case_id)
    parts = case_id.split()
    county_code = parts[1] if len(parts) >= 2 else "??"
    return {
        "CaseID": case_id,
        "CaseYear": parsed["CaseYear"],
        "County": parsed["County"],
        "CaseNumber": parsed["CaseNumber"],
        "CountyCode": county_code,
    }


def snippet(html: str, limit: int = 2000) -> str:
    return html[:limit]


def marker_report(html: str) -> str:
    lines = []
    lower = html.lower()
    for marker in PAGE_MARKERS:
        found = marker.lower() in lower if marker != "Case Summary" else marker in html
        lines.append(f"    {marker!r}: {found}")
    return "\n".join(lines)


async def option_list(page, selector: str) -> list[dict]:
    return await page.eval_on_selector_all(
        selector,
        """els => els.map(e => ({
            value: e.value,
            label: (e.textContent || "").trim(),
            selected: e.selected
        }))""",
    )


async def selected_option(page, selector: str) -> dict:
    return await page.eval_on_selector(
        selector,
        """el => {
            const opt = el.options[el.selectedIndex];
            if (!opt) return {value: el.value, label: null, selectedIndex: el.selectedIndex};
            return {
                value: opt.value,
                label: (opt.textContent || "").trim(),
                selectedIndex: el.selectedIndex
            };
        }""",
    )


async def search_once(page, url: str, case: dict, county_value: str) -> None:
    print("\n" + "=" * 72)
    print(f"CASE ID: {case['CaseID']}")
    print(f"County name in dataframe: {case['County']}")
    print(f"County code from CaseID: {case['CountyCode']}")
    print(f"Value passed to #county_num: {county_value!r}")
    print("=" * 72)

    await page.goto(url)
    print(f"Search page URL: {page.url}")
    print(f"Search page title: {await page.title()}")

    try:
        county_options = await option_list(page, "#county_num")
        court_options = await option_list(page, "#court_type")
        print(f"#county_num options ({len(county_options)}):")
        for opt in county_options:
            print(f"    value={opt['value']!r} label={opt['label']!r}")
        print(f"#court_type options ({len(court_options)}):")
        for opt in court_options:
            print(f"    value={opt['value']!r} label={opt['label']!r}")
    except Exception as exc:
        print(f"Could not read select options: {type(exc).__name__}: {exc}")

    await page.select_option("#court_type", "D")
    await page.select_option("#county_num", county_value)
    await page.select_option("#case_type", "JV")
    year_suffix = str(case["CaseYear"] - 2000)
    await page.fill("#case_year", year_suffix)
    await page.fill("#case_id", str(case["CaseNumber"]))

    selected_county = await selected_option(page, "#county_num")
    print("Selected #county_num BEFORE submit:", selected_county)
    print(f"Filled #case_year={year_suffix!r} #case_id={str(case['CaseNumber'])!r}")

    await page.click("#search")
    await page.wait_for_load_state("networkidle")

    html = await page.content()
    print(f"URL after search: {page.url}")
    print(f"Page title after search: {await page.title()}")
    print(f"HTML length: {len(html)}")
    print(f"'Case Summary' in HTML: {'Case Summary' in html}")
    print("Useful page markers:")
    print(marker_report(html))
    print("First ~2000 characters of HTML:")
    print(snippet(html))
    print("=" * 72)


async def main(url: str, case_ids: list[str], try_county_code: bool) -> None:
    print("DEBUG MODE: no MongoDB reads or writes.")
    print(f"COUNTY_MAP: {COUNTY_MAP}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()
        page.set_default_timeout(TIMEOUT_MS)
        page.set_default_navigation_timeout(TIMEOUT_MS)

        for case_id in case_ids:
            case = build_case(case_id)
            try:
                await search_once(page, url, case, str(case["County"]))
                if try_county_code:
                    print("\nRetrying the same case with county CODE instead of name...")
                    await search_once(page, url, case, str(case["CountyCode"]))
            except PlaywrightTimeoutError:
                print(f"TIMEOUT while scraping {case_id}")
            except Exception as exc:
                print(f"ERROR while scraping {case_id}: {type(exc).__name__}: {exc}")

        await context.close()
        await browser.close()


if __name__ == "__main__":
    case_url, ids, try_code = parse_args(sys.argv)
    asyncio.run(main(case_url, ids, try_code))
