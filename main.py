# main.py
import logging
import logging.handlers
import pandas as pd
import asyncio
import json
from concurrent.futures import ProcessPoolExecutor
from functools import partial
import multiprocessing

# Import functions from our new modules
from sec_api import fetch_filing_metadata
from downloader import download_all_filings
from parser import process_single_filing

def listener_configurer():
    """Configures logging for the listener process."""
    root = logging.getLogger()
    h = logging.StreamHandler()
    f = logging.Formatter('%(asctime)s - %(processName)-12s - %(levelname)-8s - %(message)s')
    h.setFormatter(f)
    root.addHandler(h)

def listener_process(queue, configurer):
    """Listens for logging messages on a queue and handles them."""
    configurer()
    logger = logging.getLogger()
    logger.info("Log listener started.")
    while True:
        try:
            record = queue.get()
            if record is None:  # We send this as a sentinel to tell the listener to quit.
                break
            logger.handle(record)
        except Exception:
            import sys, traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)

def scrape_sec_filings(symbol, start_year, end_year, form_groups, filing_urls, save_dir, log_queue):
    """
    Main orchestration function to scrape SEC filings.
    """
    logger = logging.getLogger()
    
    logger.info("-" * 50)
    logger.info(f"Starting scrape for {symbol.upper()} | {start_year}-{end_year}")
    logger.info("-" * 50)

    if filing_urls:
        filings_meta_to_process = [{'symbol': symbol, **f} for f in filing_urls]
    else:
        filings_meta_to_process = fetch_filing_metadata(symbol, start_year, end_year, form_groups)

    if not filings_meta_to_process:
        logger.warning("No filings found to process. Exiting.")
        return None

    # STAGE 1: Asynchronous Download
    logger.info(f"\n--- STAGE 1: Starting Asynchronous Download of {len(filings_meta_to_process)} filings ---")
    downloaded_files_info = asyncio.run(download_all_filings(filings_meta_to_process, save_dir))
    
    if not downloaded_files_info:
        logger.warning("No files were downloaded or found locally. Cannot proceed.")
        return None

    # STAGE 2: Parallel Processing
    logger.info(f"\n--- STAGE 2: Starting Parallel Processing of {len(downloaded_files_info)} files ---")
    
    with open('financial_statement_terms.json', 'r', encoding='utf-8') as f:
        financial_statement_terms = json.load(f)
    
    worker_func = partial(process_single_filing, terms_dict=financial_statement_terms, queue=log_queue)

    all_data_points = []
    filing_reports = []

    with ProcessPoolExecutor(max_workers=None) as executor:
        results = list(executor.map(worker_func, downloaded_files_info))

    for result_data, report in results:
        if result_data: all_data_points.extend(result_data)
        filing_reports.append(report)
    
    # STAGE 3: Reporting
    logger.info("\n" + "="*70)
    logger.info("Scraping and Processing Complete.")
    
    # *** NEW DETAILED SUMMARY LOGGING ***
    logger.info("--- Detailed Filing Summary ---")
    for report in filing_reports:
        filepath = report.get('filepath', 'Unknown File')
        statements = report.get('statements', {})
        found = [s_type for s_type, status in statements.items() if status != 'Missing']
        missing = [s_type for s_type, status in statements.items() if status == 'Missing']
        
        summary_message = f"File: {filepath} | Found {len(found)}/3 statements."
        if missing:
            summary_message += f" (Missing: {', '.join(missing)})"
        
        logger.info(summary_message)
    logger.info("-----------------------------")

    successful_filings = sum(1 for r in filing_reports if all(s != 'Missing' for s in r['statements'].values()))
    accuracy = (successful_filings / len(downloaded_files_info)) * 100 if downloaded_files_info else 0
    logger.info(f"Overall Accuracy: {successful_filings}/{len(downloaded_files_info)} filings ({accuracy:.2f}%) successfully scraped.")

    # STAGE 4: DataFrame Creation
    if not all_data_points:
        logger.warning("No financial data was extracted.")
        return None

    df = pd.DataFrame(all_data_points)
    df = df.rename(columns={'period_end_date': 'filing_period_end_date', 'units': 'unit'})
    final_cols = ['symbol', 'form_type', 'date_filed', 'filing_period_end_date', 'fiscal_period', 'table_description', 'table_number', 'href', 'category', 'metric', 'value', 'unit']
    df = df.reindex(columns=[col for col in final_cols if col in df.columns])

    logger.info(f"Total data points extracted: {len(df)}")
    return df

def main():
    """Main function to configure and initiate scraping."""
    symbol = 'SNOW'
    start_year = 2025 
    end_year = 2024
    form_groups = ['Quarterly Reports'] #'Annual Reports',  
    filing_urls_to_scrape = [] 

    # --- Use a Manager to create a shareable queue ---
    with multiprocessing.Manager() as manager:
        log_queue = manager.Queue(-1)
        
        listener = multiprocessing.Process(target=listener_process, args=(log_queue, listener_configurer))
        listener.start()

        # Configure the main process logger
        h = logging.handlers.QueueHandler(log_queue)
        root = logging.getLogger()
        root.addHandler(h)
        root.setLevel(logging.INFO)
        
        final_df = scrape_sec_filings(
            symbol=symbol, 
            start_year=start_year, 
            end_year=end_year, 
            form_groups=form_groups,
            filing_urls=filing_urls_to_scrape,
            save_dir=f"sec_filings_{symbol.upper()}",
            log_queue=log_queue
        )

        if final_df is not None and not final_df.empty:
            output_filename = f"{symbol.upper()}_financial_data_parallel.csv"
            final_df.to_csv(output_filename, index=False, encoding='utf-8')
            root.info(f"Successfully exported data to {output_filename}")
            print(f"\nSample of the final DataFrame:\n{final_df.head()}")
        else:
            root.warning("No data was scraped, CSV file not created.")
            
        # --- SHUTDOWN LOGGING ---
        log_queue.put_nowait(None)
        listener.join()

if __name__ == '__main__':
    main()