import json
import os
import logging
from typing import List, Dict, Optional

# Import your custom modules
import company_data_retriever
import career_site_discoverer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_orchestration(
    sheet_id: str,
    service_account_info: dict,
    output_file: str = "discovered_career_pages.json"
) -> List[Dict]:
    """
    Orchestrates the process of retrieving company data and discovering their career pages.

    Args:
        sheet_id (str): The ID of the Google Sheet containing company names and URLs.
        service_account_info (dict): The service account credentials for Google Sheets access.
        output_file (str): The filename to save the discovered career pages.

    Returns:
        List[Dict]: A list of dictionaries, each containing 'company_name',
                    'career_page_url', and 'status'.
    """
    logging.info("Starting orchestration process...")
    discovered_pages = []

    # 1. Retrieve companies from Google Sheet
    logging.info("Retrieving companies from Google Sheet...")
    companies_from_sheet = company_data_retriever.retrieve_companies_from_sheet(
        sheet_id, service_account_info
    )

    if not companies_from_sheet:
        logging.error("No companies retrieved from the Google Sheet. Orchestration aborted.")
        return []

    logging.info(f"Successfully retrieved {len(companies_from_sheet)} companies. Discovering career pages...")

    # 2. Discover career pages for each company
    for company_data in companies_from_sheet:
        company_name = company_data.get('name')
        direct_career_url = company_data.get('direct_career_url') # This will be None if not provided

        if not company_name:
            logging.warning(f"Skipping entry with missing company name: {company_data}")
            continue

        try:
            career_url = career_site_discoverer.find_career_page_url(
                company_name,
                direct_url=direct_career_url
            )

            if career_url:
                logging.info(f"Discovered career page for {company_name}: {career_url}")
                discovered_pages.append({
                    'company_name': company_name,
                    'career_page_url': career_url,
                    'status': 'SUCCESS'
                })
            else:
                logging.warning(f"Could not discover career page for {company_name}.")
                discovered_pages.append({
                    'company_name': company_name,
                    'career_page_url': None,
                    'status': 'FAILED_DISCOVERY'
                })
        except Exception as e:
            logging.error(f"Error discovering career page for {company_name}: {e}", exc_info=True)
            discovered_pages.append({
                'company_name': company_name,
                'career_page_url': None,
                'status': f'ERROR_DISCOVERY: {str(e)}'
            })

    # 3. Save the results to a JSON file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(discovered_pages, f, ensure_ascii=False, indent=4)
        logging.info(f"Discovered career pages saved to {output_file}")
    except Exception as e:
        logging.error(f"Error saving discovered pages to {output_file}: {e}", exc_info=True)

    logging.info("Orchestration process finished.")
    return discovered_pages

if __name__ == "__main__":
    # --- Local Testing Setup for Orchestrator ---
    LOCAL_KEY_FILE_PATH = os.path.expanduser('~/Projects/my-job-scraper-agent-key.json') # Or your actual secure path

    # This should be the ID of the Google Sheet you just updated with the new column.
    YOUR_COMPANY_SHEET_ID = "14XRmAeAyyPvJFg6ePhz8Koad3dokBC8q86wBxqtcd4Q" # <--- REPLACE THIS WITH YOUR REAL SHEET ID

    if not os.path.exists(LOCAL_KEY_FILE_PATH):
        logging.error(f"Service account key file not found at: {LOCAL_KEY_FILE_PATH}")
        logging.info("Please update LOCAL_KEY_FILE_PATH in orchestrator.py for local testing.")
    elif YOUR_COMPANY_SHEET_ID == "YOUR_COMPANIES_GOOGLE_SHEET_ID_HERE":
        logging.warning("Please replace 'YOUR_COMPANIES_GOOGLE_SHEET_ID_HERE' with your actual Google Sheet ID in orchestrator.py for local testing.")
    else:
        try:
            logging.info("Loading service account info for orchestration.")
            with open(LOCAL_KEY_FILE_PATH, 'r') as f:
                sa_info = json.load(f)

            # Run the orchestration process
            final_results = run_orchestration(YOUR_COMPANY_SHEET_ID, sa_info)

            logging.info(f"\n--- Orchestration Summary ---")
            for entry in final_results:
                logging.info(f"Company: {entry['company_name']}, URL: {entry['career_page_url'] if entry['career_page_url'] else 'N/A'}, Status: {entry['status']}")

        except Exception as e:
            logging.error(f"Error during orchestrator local testing: {e}", exc_info=True)
    # --- End Local Testing Setup ---