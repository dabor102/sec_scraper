# downloader.py
import asyncio
import aiohttp
import os
import logging

async def fetch_and_save(session, filing_meta, save_dir):
    """Coroutine to fetch one URL and save its content to a file."""
    url = filing_meta.get('url')
    if not url:
        logging.warning(f"Skipping a filing due to missing 'url' in metadata: {filing_meta}")
        return None, None

    date_filed = filing_meta.get('date_filed', 'unknown_date').split(' ')[0]
    safe_date = date_filed.replace('/', '-')
    form_type = filing_meta.get('form_type', 'unknown_form').replace('/', '_')
    symbol = filing_meta.get('symbol', 'UNKNOWN_SYMBOL')
    
    safe_filename = f"{symbol}_{form_type}_{safe_date}_{filing_meta.get('ref', 'no_ref')}.html"
    filepath = os.path.join(save_dir, safe_filename)
    
    filing_meta['local_filepath'] = filepath # Add filepath to meta for later use

    if os.path.exists(filepath):
        logging.info(f"File already exists, skipping download: {safe_filename}")
        return filepath, filing_meta

    try:
        async with session.get(url, timeout=30) as response:
            response.raise_for_status()
            content = await response.text()
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            logging.info(f"Successfully downloaded: {safe_filename}")
            return filepath, filing_meta
    except Exception as e:
        logging.error(f"Failed to download or save {url}: {e}")
        return None, None

async def download_all_filings(filings_meta, save_dir):
    """Manages the concurrent download of all filings."""
    if not os.path.exists(save_dir):
        os.makedirs(save_dir)
        logging.info(f"Created save directory: {save_dir}")
        
    headers = {'User-Agent': 'Mozilla/5.0'}
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch_and_save(session, meta, save_dir) for meta in filings_meta]
        results = await asyncio.gather(*tasks)
    
    successful_downloads = [res for res in results if res[0] is not None]
    logging.info(f"Successfully downloaded/verified {len(successful_downloads)} of {len(filings_meta)} total filings.")
    return successful_downloads