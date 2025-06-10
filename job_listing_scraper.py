import requests
from bs4 import BeautifulSoup
import json
import re
import os
import logging # Import logging

# Import the function from your company_data_retriever.py
from company_data_retriever import retrieve_companies_from_sheet

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def scrape_company_jobs(company_url, company_name):
    """
    Scrapes job listings from a given company's career page URL.
    This version uses more refined CSS selectors and keyword filtering.
    """
    try:
        logging.info(f"  Attempting to scrape: {company_url}")
        response = requests.get(company_url, timeout=15) # Increased timeout for robustness
        response.raise_for_status() # Raise an HTTPError for bad responses (4xx or 5xx)
        soup = BeautifulSoup(response.text, 'html.parser')

        listings = []

        # --- Define robust CSS selectors based on common patterns ---
        # This is the CRITICAL part you'll need to adapt for each problematic company.
        # Use your browser's Developer Tools to inspect the HTML of job titles/links.
        
        # This is a broad set of common selectors. You will likely need to *refine* this
        # or add company-specific selectors after inspecting their career pages.
        css_selectors = [
            '.job-title', '.job-listing', '.job-card', '.position-name',
            '.opening-title', 'h2.job-posting', 'h3.title', 'a.job-link',
            'div[data-qa="job-card"] a', # Common for some ATS or modern sites
            'a[href*="/job/"]', 'a[href*="/jobs/"]', 'a[href*="/careers/"]', # Links containing common job paths
            'li.job-result-card a', # Common for list-based results
            'a[data-qa="job-link"]', # Another common pattern
            'a[href*="boards.greenhouse.io/"], a[href*="jobs.lever.co/"], a[href*="myworkdayjobs.com/"]' # Common ATS links
        ]
        
        potential_job_elements = []
        for selector in css_selectors:
            try:
                potential_job_elements.extend(soup.select(selector))
            except Exception as e:
                pass # Silently ignore if a selector doesn't match

        # --- Define keywords for filtering ---
        job_keywords = [
            "manager", "analyst", "designer", "specialist",
            "director", "lead", "architect", "associate", "principal",
            "consultant", "product",
            "software", "hardware", "ux", "ui",
            "operations", "executive", "support", "account", "expert", "senior",
            "staff", "staffing", "director", "senior director", "vice president", "vp"
        ]

        # Keywords to exclude (case-insensitive)
        excluded_keywords = [
            "cookie", "privacy", "help", "careers", "about", "blog", "login",
            "sign", "policy", "terms", "faq", "jobsjobs", "person_outline", "work_outline",
            "search", "results", "dashboard", "preferences", "categories", "alerts",
            "eec", "eeo", "how we hire", "know your rights", "equal opportunity"
        ]

        processed_urls = set() # Use a set to store unique URLs to avoid duplicates

        for element in potential_job_elements:
            title = element.get_text(strip=True)
            href = element.get('href')

            if not title or not href:
                continue # Skip if no title or URL

            # Clean the title: remove common non-job text like icon names or duplicate words
            title = re.sub(r'(work_outlineJobs|person_outline|JobsJobs|helpHelpopen_in_new|open_in_new)', '', title, flags=re.IGNORECASE).strip()
            title_parts = title.split()
            if len(title_parts) >= 2 and title_parts[0].lower() == title_parts[1].lower():
                title = ' '.join(title_parts[1:])
            title = title.strip(':- ').replace('  ', ' ') # More aggressive cleaning

            # Make sure the URL is absolute
            if not href.startswith('http'):
                href = requests.compat.urljoin(company_url, href)

            # --- Filtering Logic ---
            if href in processed_urls:
                continue

            title_lower = title.lower()
            if not any(keyword in title_lower for keyword in job_keywords):
                continue

            if any(ex_keyword in title_lower for ex_keyword in excluded_keywords):
                continue
            
            if len(href.strip('/').split('/')) < 4 and not any(k in href for k in ['job', 'career', 'opening', 'position']):
                continue

            listings.append({"title": title, "url": href})
            processed_urls.add(href) # Add to set of processed URLs

        logging.info(f"  Found {len(listings)} potential listings for {company_name}")
        return {"url": company_url, "listings": listings}

    except requests.exceptions.RequestException as e:
        logging.error(f"  Error scraping {company_name} ({company_url}): {e}")
        return {"url": company_url, "listings": [], "error": str(e)}
    except Exception as e:
        logging.error(f"  An unexpected error occurred for {company_name} ({company_url}): {e}", exc_info=True)
        return {"url": company_url, "listings": [], "error": str(e)}

if __name__ == "__main__":
    # --- Configuration for Google Sheet (from company_data_retriever.py's testing block) ---
    LOCAL_KEY_FILE_PATH = os.path.expanduser('~/Projects/my-job-scraper-agent-key.json') 
    YOUR_COMPANY_SHEET_ID = "14XRmAeAyyPvJFg6ePhz8Koad3dokBC8q86wBxqtcd4Q" 

    sa_info = None
    if not os.path.exists(LOCAL_KEY_FILE_PATH):
        logging.error(f"Service account key file not found at: {LOCAL_KEY_FILE_PATH}")
        logging.info("Please update LOCAL_KEY_FILE_PATH in job_listing_scraper.py (or company_data_retriever.py) for local testing.")
        exit()
    else:
        try:
            with open(LOCAL_KEY_FILE_PATH, 'r') as f:
                sa_info = json.load(f)
        except json.JSONDecodeError as e:
            logging.error(f"Error decoding service account JSON file: {e}")
            exit()
        except Exception as e:
            logging.error(f"An unexpected error occurred while loading service account key: {e}", exc_info=True)
            exit()

    # --- Retrieve companies from Google Sheet ---
    logging.info(f"Attempting to retrieve companies from Google Sheet ID: {YOUR_COMPANY_SHEET_ID}")
    companies_data = retrieve_companies_from_sheet(YOUR_COMPANY_SHEET_ID, sa_info)

    if not companies_data:
        logging.error("No companies retrieved from Google Sheet. Cannot proceed with scraping.")
        exit()
    else:
        logging.info(f"Successfully loaded {len(companies_data)} companies from Google Sheet.")
        
    scraped_data = []
    logging.info("Starting job scraping process...\n")
    for company in companies_data:
        company_name = company.get('name', 'Unknown Company')
        career_page_url = company.get('direct_career_url')

        if not career_page_url:
            logging.warning(f"--- Skipping {company_name}: No direct_career_url found in Google Sheet. ---")
            continue

        logging.info(f"--- Scraping {company_name} ---")
        result = scrape_company_jobs(career_page_url, company_name)
        scraped_data.append({
            "company_name": company_name,
            "career_page_url": career_page_url,
            "scraped_data": result
        })
        logging.info("-" * (len(company_name) + 16) + "\n") # Separator for readability

    # Save the scraped data
    output_filename = 'scraped_job_listings.json'
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(scraped_data, f, indent=4, ensure_ascii=False)

    logging.info(f"Scraping complete. Results saved to '{output_filename}'")