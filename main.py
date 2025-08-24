# main.py
import logging
import pandas as pd
import asyncio
import json
from concurrent.futures import ProcessPoolExecutor
from functools import partial

# Import functions from our new modules
from sec_api import fetch_filing_metadata
from downloader import download_all_filings
from parser import process_single_filing

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def scrape_sec_filings(symbol, start_year, end_year, form_groups, filing_urls, save_dir):
    """
    Main orchestration function to scrape SEC filings.
    """
    print("-" * 50)
    logging.info(f"Starting scrape for {symbol.upper()} | {start_year}-{end_year}")
    print("-" * 50)

    if filing_urls:
        logging.info(f"Processing {len(filing_urls)} provided filing URLs.")
        filings_meta_to_process = []
        for f in filing_urls:
            f['symbol'] = symbol
            filings_meta_to_process.append(f)
    else:
        filings_meta_to_process = fetch_filing_metadata(symbol, start_year, end_year, form_groups)

    if not filings_meta_to_process:
        logging.warning("No filings found to process. Exiting.")
        return None

    # STAGE 1: Asynchronous Download
    logging.info(f"\n--- STAGE 1: Starting Asynchronous Download of {len(filings_meta_to_process)} filings ---")
    downloaded_files_info = asyncio.run(download_all_filings(filings_meta_to_process, save_dir))
    
    if not downloaded_files_info:
        logging.warning("No files were downloaded or found locally. Cannot proceed.")
        return None

    # STAGE 2: Parallel Processing
    logging.info(f"\n--- STAGE 2: Starting Parallel Processing of {len(downloaded_files_info)} files ---")
    
    # Load financial terms and prepare the worker function
    with open('financial_statement_terms.json', 'r', encoding='utf-8') as f:
        financial_statement_terms = json.load(f)
    
    # Use functools.partial to pass the terms dictionary to the worker
    worker_func = partial(process_single_filing, terms_dict=financial_statement_terms)

    all_data_points = []
    filing_reports = []
    total_missing_units = 0

    with ProcessPoolExecutor(max_workers=None) as executor:
        results = list(executor.map(worker_func, downloaded_files_info))

    for result_data, report in results:
        if result_data: all_data_points.extend(result_data)
        filing_reports.append(report)
       #total_missing_units += missing_units
    
    # STAGE 3: Reporting
    print("\n" + "="*70)
    logging.info("Scraping and Processing Complete.")
    
    successful_filings = sum(1 for r in filing_reports if all(s != 'Missing' for s in r['statements'].values()))
    accuracy = (successful_filings / len(downloaded_files_info)) * 100
    logging.info(f"Overall Accuracy: {successful_filings}/{len(downloaded_files_info)} filings ({accuracy:.2f}%) successfully scraped.")

    # STAGE 4: DataFrame Creation
    if not all_data_points:
        logging.warning("No financial data was extracted.")
        return None

    df = pd.DataFrame(all_data_points)
    df = df.rename(columns={'period_end_date': 'filing_period_end_date', 'units': 'unit'})
    final_cols = ['symbol', 'form_type', 'date_filed', 'filing_period_end_date', 'fiscal_period', 'table_description', 'table_number', 'href', 'category', 'metric', 'value', 'unit']
    df = df.reindex(columns=[col for col in final_cols if col in df.columns])

    logging.info(f"Total data points extracted: {len(df)}")
    return df

def main():
    """Main function to configure and initiate scraping."""
    symbol = 'SNOW'
    start_year = 2025 
    end_year = 2019
    form_groups = ['Quarterly Reports'] #'Annual Reports',  
    filing_urls_to_scrape = [] 

    final_df = scrape_sec_filings(
        symbol=symbol, 
        start_year=start_year, 
        end_year=end_year, 
        form_groups=form_groups,
        filing_urls=filing_urls_to_scrape,
        save_dir=f"sec_filings_{symbol.upper()}"
    )

    if final_df is not None and not final_df.empty:
        output_filename = f"{symbol.upper()}_financial_data_parallel.csv"
        final_df.to_csv(output_filename, index=False, encoding='utf-8')
        logging.info(f"Successfully exported data to {output_filename}")
        print(f"\nSample of the final DataFrame:\n{final_df.head()}")
    else:
        logging.warning("No data was scraped, CSV file not created.")

if __name__ == '__main__':
    main()