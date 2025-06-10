import gspread
from google.oauth2 import service_account
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def retrieve_companies_from_sheet(sheet_id: str, service_account_info: dict) -> list[dict]:
    """
    Retrieves a list of company names and their direct career URLs from a Google Sheet.
    Assumes Company Name is in Column A and Direct Career URL is in Column B.

    Args:
        sheet_id (str): The ID of the Google Sheet containing company names and URLs.
        service_account_info (dict): A dictionary containing the service account credentials.

    Returns:
        list: A list of dictionaries, where each dictionary represents a company
              and has keys 'name' and 'direct_career_url'.
              Returns an empty list if an error occurs or no valid data is found.
    """
    companies_data = []
    try:
        creds = service_account.Credentials.from_service_account_info(
            service_account_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets.readonly']
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_key(sheet_id)
        worksheet = spreadsheet.sheet1 # Assuming data is on the first sheet

        all_data = worksheet.get_all_values()

        # Assuming the first row is headers, we'll skip it for data processing
        # If your sheet does NOT have headers, remove or adjust this slicing.
        data_rows = all_data[1:] # Skip header row

        for i, row in enumerate(data_rows):
            if len(row) >= 2 and row[0].strip():
                company_name = row[0].strip()
                direct_url = row[1].strip() if len(row[1].strip()) > 0 else None # Store empty strings as None

                companies_data.append({
                    'name': company_name,
                    'direct_career_url': direct_url
                })
            else:
                logging.warning(f"Skipping row {i+2} (including header) due to insufficient data or empty company name: {row}")

        logging.info(f"Successfully retrieved {len(companies_data)} valid company entries from sheet ID: {sheet_id}")
        return companies_data

    except gspread.exceptions.SpreadsheetNotFound:
        logging.error(f"Spreadsheet with ID '{sheet_id}' not found or service account lacks permission.")
        return []
    except gspread.exceptions.NoValidUrlKeyFound:
        logging.error(f"Invalid Google Sheet ID format: '{sheet_id}'.")
        return []
    except Exception as e:
        logging.error(f"An unexpected error occurred while retrieving companies: {e}", exc_info=True)
        return []

if __name__ == "__main__":
    # --- Local Testing Setup (DO NOT commit your actual key here) ---
    import json
    import os

    LOCAL_KEY_FILE_PATH = os.path.expanduser('~/Projects/my-job-scraper-agent-key.json') # Or your actual secure path

    # This should be the ID of the Google Sheet you just updated with the new column.
    YOUR_COMPANY_SHEET_ID = "14XRmAeAyyPvJFg6ePhz8Koad3dokBC8q86wBxqtcd4Q" 

    if not os.path.exists(LOCAL_KEY_FILE_PATH):
        logging.error(f"Service account key file not found at: {LOCAL_KEY_FILE_PATH}")
        logging.info("Please update LOCAL_KEY_FILE_PATH in company_data_retriever.py for local testing.")
    else:
        try:
            with open(LOCAL_KEY_FILE_PATH, 'r') as f:
                sa_info = json.load(f)

            if YOUR_COMPANY_SHEET_ID == "YOUR_COMPANIES_GOOGLE_SHEET_ID_HERE": # <--- This string is the placeholder
                logging.warning("Please replace 'YOUR_COMPANIES_GOOGLE_SHEET_ID_HERE' with your actual Google Sheet ID for local testing.")
            else:
                logging.info(f"Attempting to retrieve companies from Sheet ID: {YOUR_COMPANY_SHEET_ID}")
                companies = retrieve_companies_from_sheet(YOUR_COMPANY_SHEET_ID, sa_info)
                if companies:
                    logging.info(f"Companies retrieved (first 3): {companies[:3]}")
                    for company in companies:
                        logging.info(f"  Name: {company['name']}, Direct URL: {company['direct_career_url']}")
                else:
                    logging.info("No companies retrieved or an error occurred. Check logs above.")
        except Exception as e:
            logging.error(f"Error during local testing setup: {e}", exc_info=True)
    # --- End Local Testing Setup ---