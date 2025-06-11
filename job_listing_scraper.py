import requests
from bs4 import BeautifulSoup
import json
import re
import os
import logging # Import logging
import time
import asyncio # Import asyncio for potential future async support
from playwright.async_api import async_playwright # Import Playwright for advanced scraping
from urllib.parse import urljoin
import requests.compat

# Import the function from your company_data_retriever.py
from company_data_retriever import retrieve_companies_from_sheet

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- DEFINE THESE GLOBALLY (AT THE TOP LEVEL OF YOUR FILE) ---
job_keywords = ["engineer", "developer", "manager", "analyst", "designer",
                "specialist", "director", "lead", "architect", "associate",
                "intern", "principal", "consultant", "recruiter", "sales",
                "marketing", "product", "data scientist", "researcher",
                "software", "hardware", "ux", "ui", "operations", "finance",
                "hr", "support", "account", "expert", "senior", "junior",
                "staff", "staffing", "technician", "business", "legal", "counsel"]

excluded_keywords = ["cookie", "privacy", "help", "careers", "about", "blog",
                     "login", "sign", "policy", "terms", "faq", "jobsjobs",
                     "person_outline", "work_outline", "search", "results",
                     "dashboard", "preferences", "categories", "alerts",
                     "eec", "eeo", "how we hire", "know your rights", "equal opportunity",
                     "contact"]
# --- END GLOBAL DEFINITIONS ---


# Global set to store found job listing IDs across all scrapes
global_found_listings_ids = set()

# Global list of selectors for Playwright
css_selectors_playwright = [
    # Specific selectors based on detailed analysis
    'li.rc-accordion-item h3 a', # Apple
    'li.inner-grid h3.text-size-4 a', # Airbnb
    'a.bx--card-group__card', # IBM
    'div.position-title', # Netflix (title only)
    'h2.MZGzlrn8gfgSs8TZHhv2', # Microsoft (title only)
    'div#js-job-search-results h2', # ServiceNow
    'li.lLd3Je a[jsname="hSRGPd"]', # Google (full card link)
    'li.search-result.job-listing h4.job-listing__title a.job-listing__link', # Walmart
    'li.open-positions__listing a.open-positions__listing-link', # Dropbox
    'a.career-role-card', # Webflow
    'a[data-qa="job-card-title"]', # Adobe (common for some enterprise sites)
    'div.job-info a.job-title', # Amazon (common, verify)

    # General selectors that often work on dynamic sites too
    'a.job-link',
    'a.jobTitle-link',
    'a[data-ph-at-id="job-list-item-title"]',
    'a[data-automation-id="jobTitle"]',
    'li.job-result-card a',
    'div.job-listing__item a',
    'h3 a',
    'h2 a',
    '.job-card a',
    '.job-item a',
    '.opening-title', # For titles that might not be links
    'a[href*="/job/"]', 'a[href*="/jobs/"]', 'a[href*="/careers/"]',
    'a[data-qa="job-link"]',
    'a[href*="boards.greenhouse.io/"], a[href*="jobs.lever.co/"], a[href*="myworkdayjobs.com/"]',
]


# --- Global Set for Deduplication across all scraped listings ---
# This will be initialized in main and passed or accessed by functions.
global_found_listings_ids = set()

# --- Companies that require Playwright for JavaScript rendering ---
# Add or remove companies from this list based on scraping success.
# If a company fails with requests, add it here.
companies_requiring_playwright = [
    "Google", "Microsoft", "Netflix", "Airbnb", "IBM", "ServiceNow",
    "Adobe", "Apple", "Amazon", "Dropbox", "Webflow", "Canva", "GoodRx",
    "Walmart" 
]

def scrape_company_jobs(company_url, company_name):
    """
    Scrapes job listings from a given company's career page URL using requests (for static content).
    This function should be used for sites that render content directly in HTML (not heavily JS-loaded).
    """
    try:
        logging.info(f"  Attempting to scrape (requests): {company_url}")

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36'
        }

        response = requests.get(company_url, timeout=15, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        listings = []
        # CSS selectors for static sites. Focus on common patterns.
        # Avoid Playwright-specific selectors here, or ensure they also work statically.
        css_selectors_static = [
            'a.job-link', # General link with job-link class
            'a.jobTitle-link', # Common Workday selector
            'a[data-ph-at-id="job-list-item-title"]', # Workday common attribute
            'a[data-automation-id="jobTitle"]', # Another Workday common attribute
            'li.job-result-card a', # Common card pattern with link
            'div.job-listing__item a', # Div containing a link
            'h3 a', # h3 with nested link
            'h2 a', # h2 with nested link
            '.job-card a', # Job card containing a link
            '.job-item a', # Job item containing a link
            '.opening-title a', # Specific title with a link
            'a[href*="/job/"]', 'a[href*="/jobs/"]', 'a[href*="/careers/"]', # Links containing job/jobs/careers
            'a[data-qa="job-link"]', # Data attribute link
            # For Greenhouse/Lever/Workday, these might work if they redirect directly:
            'a[href*="boards.greenhouse.io/"], a[href*="jobs.lever.co/"], a[href*="myworkdayjobs.com/"]',
            # Example for companies that might work statically (confirm if dynamic or static):
            # 'li.rc-accordion-item h3 a', # Apple (might be static, but Playwright handles it better)
            # 'h3.QJPWVE', # Google (definitely dynamic, remove if still here for requests)
        ]

        potential_job_elements = []
        for selector in css_selectors_static:
            try:
                # Use find_all here as select might return duplicates if selectors overlap
                potential_job_elements.extend(soup.select(selector))
            except Exception as e:
                logging.debug(f"  Selector '{selector}' failed for {company_name} (requests): {e}")
                pass

        # Use global_found_listings_ids for deduplication
        current_processed_urls = set() # For deduplication within this run

        for element in potential_job_elements:
            title = element.get_text(strip=True)
            href = element.get('href')

            if not title or not href:
                continue

            # Standardizing title and URL
            title = re.sub(r'(work_outlineJobs|person_outline|JobsJobs|helpHelpopen_in_new|open_in_new)', '', title, flags=re.IGNORECASE).strip()
            title_parts = title.split()
            if len(title_parts) >= 2 and title_parts[0].lower() == title_parts[1].lower():
                title = ' '.join(title_parts[1:])
            title = title.strip(':- ').replace('  ', ' ')

            if not href.startswith('http'):
                href = urljoin(company_url, href)

            # Deduplicate globally
            job_id = f"{title}-{href}" # Simple unique ID for now
            if job_id in global_found_listings_ids or href in current_processed_urls:
                continue

            title_lower = title.lower()
            if not any(keyword in title_lower for keyword in job_keywords):
                continue

            if any(ex_keyword in title_lower for ex_keyword in excluded_keywords):
                continue

            # Basic URL filtering
            if len(href.strip('/').split('/')) < 4 and not any(k in href for k in ['job', 'career', 'opening', 'position', 'viewjob']):
                continue

            listings.append({"title": title, "url": href})
            global_found_listings_ids.add(job_id) # Add to global set
            current_processed_urls.add(href)

        logging.info(f"  Found {len(listings)} potential listings for {company_name} (requests)")
        return {"url": company_url, "listings": listings}

    except requests.exceptions.RequestException as e:
        logging.error(f"  Requests error for {company_name} ({company_url}): {e}")
        return {"url": company_url, "listings": [], "error": str(e)}
    except Exception as e:
        logging.error(f"  An unexpected error occurred for {company_name} ({company_url}): {e}", exc_info=True)
        return {"url": company_url, "listings": [], "error": str(e)}
    

# Your scrape_company_jobs_with_playwright function starts here
async def scrape_company_jobs_with_playwright(company_url, company_name):
    """
    Scrapes job listings from a given company's career page URL using Playwright (for JS-loaded content).
    Each company now gets its own dedicated block for URL navigation, waiting, and extraction.
    """
    browser = None
    try:
        logging.info(f"  Attempting to scrape (Playwright): {company_url}")
        async with async_playwright() as p:
            # Set headless=False for debugging, True for production
            browser = await p.chromium.launch(headless=False, slow_mo=500)
            page = await browser.new_page()

            listings = []
            current_processed_urls = set() # For deduplication within this run

            # --- Company-Specific Logic Dispatch ---

            if company_name == "Google":
                logging.info(f"    Playwright: Starting Google-specific scraping for {company_url}")
                page_count = 0
                max_pages = 45 # Safety limit
                google_next_button_selector = 'a[aria-label="Go to next page"]'
                google_job_item_selector = 'li.lLd3Je'
                google_title_selector = 'h3.QJPWVe'
                google_link_selector = 'a[jsname="hSRGPd"]'

                # Google: Initial navigation and wait for specific job items
                await page.goto(company_url, wait_until='domcontentloaded', timeout=60000)
                try:
                    logging.info(f"    Playwright: Waiting for Google job items '{google_job_item_selector}' (timeout: 30.0s)...")
                    await page.wait_for_selector(google_job_item_selector, timeout=30000)
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    logging.info("      Playwright: Google initial job list loaded.")
                except Exception as e:
                    logging.warning(f"      Playwright: Timeout waiting for Google initial job list: {e}. Proceeding but may miss initial jobs.")

                while page_count < max_pages:
                    page_count += 1
                    logging.info(f"    Playwright: Scraping Google page {page_count}...")

                    # Re-get content and re-parse soup for the current page inside the loop
                    content = await page.content()
                    soup = BeautifulSoup(content, 'html.parser')

                    job_list_items = soup.select(google_job_item_selector)
                    if not job_list_items and page_count > 1:
                        logging.info("    Playwright: No more job listings found on current Google page. Exiting pagination.")
                        break

                    current_page_new_listings = 0
                    for element in job_list_items:
                        title_element = element.select_one(google_title_selector)
                        link_element = element.select_one(google_link_selector)

                        title = title_element.get_text(strip=True) if title_element else None
                        link = link_element.get('href') if link_element else None

                        if not title or not link:
                            continue

                        if not link.startswith('http'):
                            link = urljoin(company_url, link)

                        job_id = f"{title}-{link}"
                        if job_id in global_found_listings_ids or link in current_processed_urls:
                            continue

                        title_lower = title.lower()
                        if not any(keyword in title_lower for keyword in job_keywords):
                            continue
                        if any(ex_keyword in title_lower for ex_keyword in excluded_keywords):
                            continue
                        if len(link.strip('/').split('/')) < 4 and not any(k in link for k in ['job', 'career', 'opening', 'position', 'viewjob', 'listing']):
                            continue

                        listings.append({"title": title, "url": link})
                        global_found_listings_ids.add(job_id)
                        current_processed_urls.add(link)
                        current_page_new_listings += 1

                    logging.info(f"    Playwright: Found {current_page_new_listings} new listings on Google page {page_count}.")

                    try:
                        await page.wait_for_selector(google_next_button_selector, state='visible', timeout=5000)
                        next_button = await page.query_selector(google_next_button_selector)

                        if next_button and await next_button.is_enabled():
                            logging.info(f"    Playwright: Clicking 'Next Page' button for Google page {page_count}...")
                            await next_button.click()
                            await page.wait_for_load_state('domcontentloaded', timeout=15000)
                            await page.wait_for_timeout(500)
                        else:
                            logging.info("    Playwright: 'Next Page' button not found or not enabled. End of Google results.")
                            break
                    except Exception as e:
                        logging.info(f"    Playwright: Error finding/clicking 'Next Page' button for Google: {e}. Assuming end of results.")
                        break

                logging.info(f"  Found {len(listings)} total potential listings for {company_name} (Playwright)")
                return {"url": company_url, "listings": listings, "error": None}

            elif company_name == "ServiceNow":
                logging.info(f"    Playwright: Starting ServiceNow-specific scraping for {company_url}")
                service_now_main_container_selector = 'div#js-job-search-results'
                # --- UPDATED SELECTOR HERE ---
                service_now_job_item_selector = 'div.card.card-job' # Corrected selector based on your feedback
                service_now_title_selector = 'h2.card-title' # Assuming this class is correct for the h2 within the job card
                service_now_link_selector = 'a.js-view-job' # Assuming this class is correct for the link within the job card

                # ServiceNow: Initial navigation and wait for main job container
                await page.goto(company_url, wait_until='domcontentloaded', timeout=30000)
                try:
                    logging.info(f"    Playwright: Waiting for ServiceNow main container '{service_now_main_container_selector}' (timeout: 45.0s)...")
                    await page.wait_for_selector(service_now_main_container_selector, timeout=45000) # Increased timeout
                    await page.wait_for_load_state('networkidle', timeout=45000) # Keep for now, but note it might time out
                    logging.info(f"      Playwright: ServiceNow main container '{service_now_main_container_selector}' found, network idle state reached.")
                except Exception as e:
                    logging.warning(f"      Playwright: Timeout waiting for ServiceNow job elements or network idle: {e}. Attempting to proceed without full load confirmation.")

                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                # Extract jobs from the ServiceNow page
                job_items = soup.select(service_now_job_item_selector) # Select all individual job containers
                if not job_items:
                    logging.warning(f"    Playwright: No job items found using selector '{service_now_job_item_selector}' for ServiceNow. Please double-check this selector and the inner title/link selectors!") # Added a more emphatic warning
                else:
                    logging.info(f"    Playwright: Found {len(job_items)} job item containers using selector '{service_now_job_item_selector}'.")


                for item in job_items:
                    title_element = item.select_one(service_now_title_selector) # Find title within the item
                    link_element = item.select_one(service_now_link_selector) # Find link within the item

                    title = title_element.get_text(strip=True) if title_element else None
                    link = link_element.get('href') if link_element else None

                    if not title or not link:
                        # Log if a title or link is missing within a found job item
                        logging.debug(f"      Playwright: Skipping job item due to missing title or link. Item HTML (title selector '{service_now_title_selector}', link selector '{service_now_link_selector}'): {item.prettify()}")
                        continue

                    if not link.startswith('http'):
                        link = urljoin(company_url, link)

                    job_id = f"{title}-{link}" if link else title
                    if job_id in global_found_listings_ids or (link and link in current_processed_urls):
                        continue

                    # --- TEMPORARILY COMMENT OUT THIS BLOCK ---
                    # title_lower = title.lower()
                    # if not any(keyword in title_lower for keyword in job_keywords):
                    #     continue
                    # if any(ex_keyword in title_lower for ex_keyword in excluded_keywords):
                    #     continue

                    # if link and len(link.strip('/').split('/')) < 4 and not any(k in link for k in ['job', 'career', 'opening', 'position', 'viewjob', 'listing']):
                    #     logging.debug(f"      Playwright: Skipping job item due to short/non-job related link: {link}")
                    #     continue
                    # elif not link and len(title.split()) < 3:
                    #     logging.debug(f"      Playwright: Skipping job item due to no link and short title: {title}")
                    #     continue
                    # --- END TEMPORARY COMMENT OUT ---

                    listings.append({"title": title, "url": link})
                    global_found_listings_ids.add(job_id)
                    if link:
                        current_processed_urls.add(link)

                logging.info(f"  Found {len(listings)} total potential listings for {company_name} (Playwright)")
                return {"url": company_url, "listings": listings, "error": None}

            else: # Generic Playwright CSS selectors processing for all other companies
                logging.info(f"    Playwright: Starting generic scraping for {company_url}")
                # For generic companies, we fall back to a simple goto and wait for body (or rely on networkidle)
                await page.goto(company_url, wait_until='domcontentloaded', timeout=60000)
                try:
                    # Generic wait for body and network idle
                    logging.info(f"    Playwright: Waiting for generic page load (body, timeout: 20.0s)...")
                    await page.wait_for_selector('body', timeout=20000)
                    await page.wait_for_load_state('networkidle', timeout=30000)
                    logging.info(f"      Playwright: Generic page loaded, network idle state reached.")
                except Exception as e:
                    logging.warning(f"      Playwright: Timeout waiting for generic page load for {company_name}: {e}. Proceeding.")

                content = await page.content()
                soup = BeautifulSoup(content, 'html.parser')

                for selector in css_selectors_playwright:
                    try:
                        elements = soup.select(selector)
                        for element in elements:
                            link = None
                            title = None

                            # --- Specific extraction logic for Playwright elements within generic search ---
                            if company_name == "Microsoft" and "MZGzlrn8gfgSs8TZHhv2" in element.get('class', []):
                                title = element.get_text(strip=True)
                                link = None
                            elif company_name == "Netflix" and "position-title" in element.get('class', []):
                                title = element.get_text(strip=True)
                                link = None
                            elif element.name == 'a':
                                link = element.get('href')
                                title = element.get_text(strip=True)
                            else:
                                nested_link = element.find('a', href=True)
                                if nested_link:
                                    link = nested_link.get('href')
                                    title = nested_link.get_text(strip=True)
                                else:
                                    title = element.get_text(strip=True)
                                    link = None

                            if title:
                                title = re.sub(r'(work_outlineJobs|person_outline|JobsJobs|helpHelpopen_in_new|open_in_new)', '', title, flags=re.IGNORECASE).strip()
                                title_parts = title.split()
                                if len(title_parts) >= 2 and title_parts[0].lower() == title_parts[1].lower():
                                    title = ' '.join(title_parts[1:])
                                title = title.strip(':- ').replace('  ', ' ')

                                if link and not link.startswith('http'):
                                    link = urljoin(company_url, link)

                                job_id = f"{title}-{link}" if link else title
                                if job_id in global_found_listings_ids or (link and link in current_processed_urls):
                                    continue

                                title_lower = title.lower()
                                if not any(keyword in title_lower for keyword in job_keywords):
                                    continue
                                if any(ex_keyword in title_lower for ex_keyword in excluded_keywords):
                                    continue

                                if link and len(link.strip('/').split('/')) < 4 and not any(k in link for k in ['job', 'career', 'opening', 'position', 'viewjob', 'listing']):
                                    continue
                                elif not link and len(title.split()) < 3:
                                    continue

                                listings.append({"title": title, "url": link})
                                global_found_listings_ids.add(job_id)
                                if link:
                                    current_processed_urls.add(link)
                    except Exception as e:
                        logging.debug(f"  Selector '{selector}' failed for {company_name} (Playwright): {e}")
                        pass

                logging.info(f"  Found {len(listings)} potential listings for {company_name} (Playwright)")
                return {"url": company_url, "listings": listings, "error": None}

    except Exception as e:
        logging.error(f"  Error scraping with Playwright for {company_name} ({company_url}): {e}", exc_info=True)
        return {"url": company_url, "listings": [], "error": str(e)}
    finally:
        if browser:
            logging.info(f"    Playwright: Closing browser for {company_name}.")
            await browser.close()

# --- Main execution block ---
async def main():
    # --- Configuration for Google Sheet (from company_data_retriever.py's testing block) ---
    # Adjust this path if your key file is in a different location.
    LOCAL_KEY_FILE_PATH = os.path.expanduser('~/Projects/my-job-scraper-agent-key.json')
    YOUR_COMPANY_SHEET_ID = "14XRmAeAyyPvJFg6ePhz8Koad3dokBC8q86wBxqtcd4Q"

    sa_info = None
    if not os.path.exists(LOCAL_KEY_FILE_PATH):
        logging.error(f"Service account key file not found at: {LOCAL_KEY_FILE_PATH}")
        logging.info("Please update LOCAL_KEY_FILE_PATH for local testing.")
        # Attempt to use a common path if it's expected to be in the same dir
        alt_path = 'job-scraper-424619-354a7c13cb24.json'
        if os.path.exists(alt_path):
            logging.info(f"Found key file at '{alt_path}'. Attempting to use this.")
            LOCAL_KEY_FILE_PATH = alt_path
            try:
                with open(LOCAL_KEY_FILE_PATH, 'r') as f:
                    sa_info = json.load(f)
            except json.JSONDecodeError as e:
                logging.error(f"Error decoding service account JSON file at '{LOCAL_KEY_FILE_PATH}': {e}")
                exit()
            except Exception as e:
                logging.error(f"An unexpected error occurred while loading service account key from '{LOCAL_KEY_FILE_PATH}': {e}", exc_info=True)
                exit()
        else:
            logging.error("No valid service account key file found. Exiting.")
            exit()
    else:
        try:
            with open(LOCAL_KEY_FILE_PATH, 'r') as f:
                sa_info = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding service account JSON file at '{LOCAL_KEY_FILE_PATH}': {e}")
            exit()
        except Exception as e:
            logging.error(f"An unexpected error occurred while loading service account key from '{LOCAL_KEY_FILE_PATH}': {e}", exc_info=True)
            exit()


    logging.info(f"Attempting to retrieve companies from Google Sheet ID: {YOUR_COMPANY_SHEET_ID}")
    companies_data = retrieve_companies_from_sheet(YOUR_COMPANY_SHEET_ID, sa_info)

    if not companies_data:
        logging.error("No companies retrieved from Google Sheet. Cannot proceed with scraping.")
        exit()
    else:
        logging.info(f"Successfully loaded {len(companies_data)} companies from Google Sheet.")

    scraped_data = []
    logging.info("Starting job scraping process...\n")

    # Access the global set for deduplication
    global global_found_listings_ids
    global_found_listings_ids = set() # Reset for each run if needed
    #debug_companies = ["ServiceNow"] # Only scrape Google and ServiceNow for now - REMOVE THIS LINE IN PRODUCTION
    for company in companies_data:
        # --- FIX STARTS HERE ---
        # Use the keys that retrieve_companies_from_sheet actually provides
        company_name = company.get('name', 'Unknown Company').strip()
        career_page_url = company.get('direct_career_url', '').strip()
        # --- FIX ENDS HERE ---
     

        if not career_page_url:
            logging.warning(f"--- Skipping {company_name}: No Careers Page URL found in Google Sheet. ---")
            continue

        logging.info(f"    DEBUG: Current company_name being processed: '{company_name}' ")

        if company_name in companies_requiring_playwright:
            result = await scrape_company_jobs_with_playwright(career_page_url, company_name)
        else:
            result = scrape_company_jobs(career_page_url, company_name)

        scraped_data.append({
            "company_name": company_name,
            "career_page_url": career_page_url,
            "scraped_data": result # This now contains 'url' and 'listings'
        })
        logging.info("-" * (len(company_name) + 16) + "\n")

    output_filename = 'scraped_job_listings.json'
    try:
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(scraped_data, f, indent=4, ensure_ascii=False)
        logging.info(f"Scraping complete. Results saved to '{output_filename}'")
    except Exception as e:
        logging.error(f"Error saving results to JSON: {e}")

# --- Run the async main function ---
if __name__ == "__main__":
    asyncio.run(main())