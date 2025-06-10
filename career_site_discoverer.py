import requests
from bs4 import BeautifulSoup
import time
import logging
from urllib.parse import urlparse, urljoin

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def is_valid_url(url: str) -> bool:
    """Checks if a string is a syntactically valid URL."""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except ValueError:
        return False

def get_base_domain(url: str) -> str:
    """Extracts the base domain from a URL (e.g., example.com from www.sub.example.com)."""
    try:
        netloc = urlparse(url).netloc
        # Remove common prefixes like 'www.'
        if netloc.startswith('www.'):
            netloc = netloc[4:]
        return netloc
    except Exception:
        return ""

def find_career_page_url(company_name: str, direct_url: str | None = None) -> str | None:
    # ... (docstring and initial logging.info remain the same) ...
    logging.info(f"Attempting to find career page for company: {company_name}")

    # DEBUG: Check the direct_url and its validity
    if direct_url:
        is_valid = is_valid_url(direct_url)
        print(f"DEBUG: Company: {company_name}, Provided URL: '{direct_url}', is_valid_url result: {is_valid}")

    # Check if a direct URL is provided and valid
    if direct_url and is_valid_url(direct_url): # This line remains the same
        logging.info(f"Using provided direct URL for {company_name}: {direct_url}")
        return direct_url

    # Common search queries to find career pages
    search_queries = [
        f"{company_name} careers",
        f"{company_name} jobs",
        f"{company_name} job openings",
        f"{company_name} recruiting",
        f"{company_name} career opportunities"
    ]

    # Use Google search (or a search engine API if configured)
    # For this example, we'll simulate a basic web search with DuckDuckGo for simplicity.
    # A real production system might use Google Custom Search API or a dedicated search API.
    search_base_url = "https://duckduckgo.com/html/?q="

    # Heuristic for likely company domains
    # This is a very simple guess; real implementation might use more advanced techniques
    # e.g., company_name.com, company-name.com, companyname.com
    company_slug = company_name.lower().replace(" ", "")
    likely_domains = [f"{company_slug}.com", f"{company_slug}.org", f"{company_slug}.net"]
    # Add more variations if needed

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    found_url = None

    for query in search_queries:
        full_search_url = f"{search_base_url}{requests.utils.quote(query)}"
        logging.debug(f"Searching with query: {full_search_url}")

        try:
            response = requests.get(full_search_url, headers=headers, timeout=10)
            response.raise_for_status() # Raise HTTPError for bad responses (4xx or 5xx)
            soup = BeautifulSoup(response.text, 'html.parser')

            # DuckDuckGo's HTML structure for search results (can change!)
            # Look for links within elements that contain search results
            # Common selectors: .result__a, .web-result__url, etc.
            links = soup.select('a.result__a') # Common selector for DDG result links

            for link in links:
                href = link.get('href')
                if not href or not is_valid_url(href):
                    continue

                # Attempt to resolve relative URLs
                if not href.startswith(('http://', 'https://')):
                    # This part might need refinement based on how DDG renders relative links
                    # For now, we'll assume most are absolute for simplicity.
                    pass # If DDG gives relative links, this needs a base URL

                # Check if the URL is likely a career page and from the correct domain
                href_lower = href.lower()
                if ('careers' in href_lower or 'jobs' in href_lower or 'employment' in href_lower or 'hiring' in href_lower):
                    # Prioritize links that are likely from the company's own domain
                    base_href_domain = get_base_domain(href)
                    is_likely_company_domain = False
                    for dom in likely_domains:
                        if dom in base_href_domain:
                            is_likely_company_domain = True
                            break
                    # Also check if the company name itself is in the URL or domain
                    if company_name.lower().replace(" ", "") in base_href_domain or company_name.lower().replace(" ", "") in href_lower:
                        is_likely_company_domain = True

                    if is_likely_company_domain:
                        found_url = href
                        logging.info(f"Found potential career URL for {company_name}: {found_url}")
                        return found_url # Return the first good one

        except requests.exceptions.RequestException as e:
            logging.warning(f"Network error during search for {company_name} (query: {query}): {e}")
        except Exception as e:
            logging.error(f"Unexpected error during search for {company_name} (query: {query}): {e}", exc_info=True)
        finally:
            time.sleep(1) # Be polite to the search engine

    logging.warning(f"Could not find a reliable career URL for {company_name} after all attempts.")
    return None

if __name__ == "__main__":
    # --- Local Testing Setup ---
    # Format: {"name": "Company Name", "direct_career_url": "https://direct.url/" or None}
    test_companies_with_urls = [
        {"name": "Google", "direct_career_url": "https://careers.google.com/jobs/results/"},
        {"name": "Microsoft", "direct_career_url": "https://careers.microsoft.com/"},
        {"name": "Apple", "direct_career_url": "https://jobs.apple.com/"}, # Now with direct URL
        {"name": "Tesla", "direct_career_url": "https://www.tesla.com/careers"}, # Now with direct URL
        {"name": "Amazon", "direct_career_url": "https://www.amazon.jobs/en/"}, # Now with direct URL
        {"name": "NonExistentCompany12345", "direct_career_url": None}, # Still no direct URL, will fall back to search
        {"name": "Netflix", "direct_career_url": "https://jobs.netflix.com/"},
        {"name": "Coinbase", "direct_career_url": None} # Example where search will still be used
    ]

    print("\n--- Testing Career Site Discovery ---")
    for company_data in test_companies_with_urls:
        company_name = company_data['name']
        direct_url_provided = company_data['direct_career_url']

        # Call the function with both arguments
        url = find_career_page_url(company_name, direct_url=direct_url_provided)
        if url:
            print(f"  {company_name}: {url}")
        else:
            print(f"  {company_name}: No URL found")
        print("-" * 30) # Separator for readability
    # --- End Local Testing Setup ---