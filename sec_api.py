# sec_api.py
import logging
import re
import requests

def fetch_filing_metadata(symbol, start_year, end_year, form_groups):
    """
    Fetches a list of filing metadata from the NASDAQ API for a given symbol and period.
    """
    filings_meta_to_process = []
    headers = {'User-Agent': 'Mozilla/5.0'}
    logging.info("Fetching filing list from NASDAQ API...")

    for year in range(start_year, end_year - 1, -1):
        for form_group in form_groups:
            try:
                api_url = f"https://api.nasdaq.com/api/company/{symbol}/sec-filings"
                params = {'limit': 100, 'formGroup': form_group, 'year': year}
                response = requests.get(api_url, headers=headers, params=params, timeout=20)
                response.raise_for_status()
                filings = response.json().get('data', {}).get('rows', [])
                logging.info(f"Found {len(filings)} filings for '{form_group}' in {year}.")
                for f in filings:
                    # Extract the ref from the URL for a more unique filename
                    ref_match = re.search(r'ref=(\d+)', f.get('view', {}).get('htmlLink', ''))
                    filings_meta_to_process.append({
                        'url': f.get('view', {}).get('htmlLink'),
                        'form_type': f.get('formType'),
                        'date_filed': f.get('filed', '').split('#')[0],
                        'symbol': symbol.upper(),
                        'ref': ref_match.group(1) if ref_match else 'no_ref'
                    })
            except Exception as e:
                logging.error(f"Error fetching from NASDAQ API for {year} {form_group}: {e}")
    
    return filings_meta_to_process