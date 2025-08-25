# parser.py

import logging
import logging.handlers
import re
import os
from datetime import datetime
from bs4 import BeautifulSoup, Tag
# Import both the validator and the classifier from the llm_analyzer
from llm_analyzer import classify_toc_items, validate_financial_toc

# ==============================================================================
# SECTION 1: CORE PARSING UTILITIES
# (These functions handle low-level tasks like parsing values, units, and headers)
# ==============================================================================

def parse_financial_value(value_str):
    """
    Parses a string to extract a financial value, handling commas, parentheses for negatives, and dashes.
    """
    if not value_str:
        return None
    cleaned_str = value_str.strip().replace('$', '').replace(',', '')
    if cleaned_str in ['—', '-']:
        return 0.0
    if not cleaned_str:
        return None
    is_negative = cleaned_str.startswith('(') and cleaned_str.endswith(')')
    if is_negative:
        cleaned_str = '-' + re.sub(r'[^\d.]', '', cleaned_str)
    try:
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None

def find_table_units(table):
    """
    Searches for financial units (e.g., 'in millions') associated with a table by checking
    nearby text and the table's initial rows.
    """
    unit_pattern = re.compile(r'\((?:in\s+)?(?:millions|thousands|billions)[^)]*\)', re.IGNORECASE)
    # Check the first few rows of the table
    for row in table.find_all('tr', limit=5):
        match = unit_pattern.search(row.get_text(" ", strip=True))
        if match:
            return match.group(0)
    # Check the tags immediately preceding the table
    for prev_tag in table.find_previous_siblings(limit=15):
        if prev_tag.name in ['p', 'div', 'span', 'b', 'strong']:
            match = unit_pattern.search(prev_tag.get_text(" ", strip=True))
            if match:
                return match.group(0)
    return None

def parse_table_headers(table):
    """
    Finds the header row in a table and returns a list of all fiscal years found.
    This is critical for correctly mapping values to their respective periods.
    """
    logger = logging.getLogger()
    header_rows = table.find_all('tr', limit=10)
    date_pattern = re.compile(r'\b((?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2},\s+20\d{2})\b|\b(20\d{2})\b', re.IGNORECASE)
    
    for row in header_rows:
        header_text = row.get_text(" ", strip=True)
        matches = date_pattern.findall(header_text)
        
        years_found = []
        for full_date, year_only in matches:
            if full_date:
                try:
                    # Extract just the year from a full date string like "Dec. 31, 2024"
                    years_found.append(re.search(r'(20\d{2})', full_date).group(1))
                except (AttributeError, IndexError):
                    continue
            elif year_only:
                years_found.append(year_only)

        if years_found:
            logger.info(f"Header parse found raw years: {years_found} in text: '{header_text[:150]}...'")
            # Sort years descending to handle typical financial report layouts
            return sorted(years_found, reverse=True)
            
    logger.warning("Could not find any valid fiscal periods in table headers.")
    return []


def scrape_data_from_tables(tables, context, all_data_points, table_map, toc_href=None):
    """
    The core data extraction function. Iterates through rows of provided tables and extracts
    financial metrics and their corresponding values for each fiscal period.
    """
    logger = logging.getLogger()
    for table in tables:
        table_info = table_map.get(table)
        if not table_info:
            continue

        fiscal_periods = parse_table_headers(table)
        num_periods = len(fiscal_periods)
        
        if num_periods == 0:
            logger.warning(f"Skipping table #{table_info.get('number', 'N/A')} because no fiscal periods were found.")
            continue

        units = find_table_units(table) or "N/A"
        current_category = "" # Used to group metrics under subheadings like "Current Assets"
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            metric_name = cells[0].get_text(" ", strip=True)
            if not metric_name or metric_name.isdigit():
                continue

            # Find all potential numeric values in the row
            full_row_text = " ".join([c.get_text(" ", strip=True) for c in cells[1:]])
            value_strings = re.findall(r'(\([\d,.-]+\)|—|[\d,.-]+)', full_row_text)

            if not value_strings:
                # If a row has a label but no values, it's likely a category header
                current_category = metric_name
                continue
            
            # Ensure the number of values found matches the number of fiscal periods from the header
            if len(value_strings) == num_periods:
                for i, period in enumerate(fiscal_periods):
                    if i < len(value_strings):
                        value = parse_financial_value(value_strings[i])
                        if value is not None:
                            all_data_points.append({
                                **context,
                                "metric": metric_name,
                                "value": value,
                                "units": units,
                                "fiscal_period": period,
                                "category": current_category,
                                "table_number": table_info['number'],
                                "href": toc_href if toc_href else f"#{table_info['id']}"
                            })
            elif value_strings:
                 logger.warning(f"Skipping row '{metric_name[:50]}...'. Found {len(value_strings)} values but expected {num_periods}.")


# ==============================================================================
# SECTION 2: TOC-GUIDED SCRAPING LOGIC (PRIMARY PATH)
# ==============================================================================

def find_all_toc_tables(soup):
    """
    Finds all potential Table of Contents tables in the document using multiple heuristics.
    This is crucial for annual reports that may have a general and a financial ToC.
    """
    toc_tables = []
    
    # Heuristic 1: Find tables immediately following a "TABLE OF CONTENTS" or similar header.
    potential_headers = soup.find_all(lambda tag: tag.name in ['p', 'div', 'b'] and re.search(r'^\s*(TABLE\s+OF\s+CONTENTS|INDEX\s+TO\s+FINANCIAL\s+STATEMENTS)\s*$', tag.get_text(strip=True), re.IGNORECASE))
    for header in potential_headers:
        parent = header.find_parent(('div', 'p')) or header
        if 'center' in parent.get('align', '') or 'text-align:center' in parent.get('style', ''):
            potential_table = parent.find_next('table')
            if potential_table and len(potential_table.find_all('tr')) > 5:
                if potential_table not in toc_tables:
                    toc_tables.append(potential_table)
                    logging.info("Found ToC candidate via header search.")

    # Heuristic 2: Find tables with a high density of internal anchor links.
    for table in soup.find_all('table'):
        if table in toc_tables:
            continue
        rows = table.find_all('tr')
        if len(rows) < 10:
            continue
        link_count = sum(1 for r in rows[:20] if r.find('a', href=lambda h: h and h.startswith('#')))
        if link_count > 7:
            toc_tables.append(table)
            logging.info("Found ToC candidate via structural analysis (link density).")

    return toc_tables

def parse_toc_table_to_index(toc_table, soup):
    """
    Parses a single BeautifulSoup table object into a structured list of items,
    each containing its description and a reference to its anchor tag in the document.
    """
    index = []
    for row in toc_table.find_all('tr'):
        links = row.find_all('a', href=lambda href: href and href.startswith('#'))
        if not links:
            continue

        main_href = links[-1]['href']
        anchor_name = main_href.lstrip('#')
        anchor_tag = soup.find('a', {'name': anchor_name}) or soup.find(id=anchor_name)

        text = row.get_text(" ", strip=True).replace('\xa0', ' ')
        item_match = re.search(r'(ITEM\s+\d+[A-Z]?\.?)', text, re.IGNORECASE)
        desc = re.sub(r'^\s*ITEM\s+\d+[A-Z]?\.?\s*', '', text, flags=re.IGNORECASE).strip()
        desc = re.sub(r'[\s.]+\d+\s*$', '', desc).strip() # Remove trailing page numbers

        if desc and anchor_tag:
            index.append({
                'item_no': item_match.group(1).upper().strip() if item_match else "N/A",
                'item_description': desc,
                'anchor_href': main_href,
                'anchor_tag': anchor_tag
            })
    return index


def get_section_content_between_anchors(start_tag, end_tag):
    """
    "Slices" the HTML document by collecting all sibling tags between a start and end anchor.
    This isolates the content of a specific section (e.g., "Consolidated Balance Sheets").
    """
    content_tags = []
    current_tag = start_tag
    
    while current_tag:
        if current_tag == end_tag:
            break
        
        content_tags.append(current_tag)
        current_tag = current_tag.find_next_sibling()

    if not content_tags:
        logging.warning("Slicing function found no content between anchors.")
        return BeautifulSoup("", 'html.parser')

    return BeautifulSoup("".join(str(t) for t in content_tags), 'html.parser')

def process_guided_scrape(full_index, soup, base_context, terms_dict, all_data_points, status_report, table_map):
    """
    The main guided scraping workflow. It uses the LLM-classified ToC items to slice the document
    and scrape data only from the relevant sections.
    """
    logging.info("Attempting ToC-Guided scraping path...")
    # This LLM call maps specific ToC descriptions to our required statement types
    mapped_statements = classify_toc_items(full_index, list(terms_dict.keys()))

    if not mapped_statements:
        logging.warning("LLM could not map any ToC items to statements for this index.")
        return False

    statements_found_in_this_run = 0
    tag_to_index_pos = {item['anchor_tag']: i for i, item in enumerate(full_index)}

    for statement_type, toc_item in mapped_statements.items():
        start_tag = toc_item['anchor_tag']
        toc_anchor_href = toc_item['anchor_href']
        current_pos = tag_to_index_pos.get(start_tag)

        # Define the end of the section by finding the anchor of the *next* item in the ToC
        end_tag = None
        if current_pos is not None and current_pos + 1 < len(full_index):
            end_tag = full_index[current_pos + 1]['anchor_tag']

        logging.info(f"Slicing document for '{statement_type}' using anchors...")
        section_soup = get_section_content_between_anchors(start_tag, end_tag)

        tables_in_section = section_soup.find_all('table')
        if tables_in_section:
            context = {**base_context, "table_description": statement_type.replace('_', ' ').title()}
            initial_data_point_count = len(all_data_points)
            
            scrape_data_from_tables(tables_in_section, context, all_data_points, table_map, toc_href=toc_anchor_href)
            
            # Confirm that data was actually added before marking as successful
            if len(all_data_points) > initial_data_point_count:
                status_report['statements'][statement_type] = 'Found (ToC-Guided)'
                statements_found_in_this_run += 1
        else:
            logging.warning(f"Found section for '{statement_type}' but no tables within it.")
    
    return statements_found_in_this_run > 0

# ==============================================================================
# SECTION 3: FALLBACK SCRAPING LOGIC
# ==============================================================================

def find_and_scrape_financial_statements_fallback(soup, base_context, terms_dict, all_data_points, status_report, table_map):
    """
    A brute-force fallback method that is used only if the ToC-guided approach fails.
    It scores every table in the document against a dictionary of financial terms.
    """
    logger = logging.getLogger()
    logger.warning("Executing Fallback scraping path (global table scan).")
    
    def get_all_terms(data):
        if isinstance(data, list): return data
        terms = []
        if isinstance(data, dict):
            for v in data.values(): terms.extend(get_all_terms(v))
        return terms

    flat_terms = {s: get_all_terms(c) for s, c in terms_dict.items()}
    all_tables = soup.find_all('table')
    scored_tables = []

    for i, table in enumerate(all_tables):
        table_info = table_map.get(table, {"number": "N/A"})
        scores = {}
        text = table.get_text(" ", strip=True).lower()
        if '%' in text[:500]: # Exclude tables with percentages, likely notes
            continue
        for s_type, terms in flat_terms.items():
            scores[s_type] = sum(1 for term in terms if term in text)
        
        if any(s > 5 for s in scores.values()):
            logger.info(f"Table #{table_info['number']} scores: {scores}")
            scored_tables.append({'table_obj': table, 'scores': scores, 'number': table_info['number']})

    found_statements = {}
    for s_type in terms_dict.keys():
        best_candidate = max(
            (c for c in scored_tables if c['table_obj'] not in found_statements.values()),
            key=lambda x: x['scores'].get(s_type, 0),
            default=None
        )
        if best_candidate and best_candidate['scores'].get(s_type, 0) > 10:
            found_statements[s_type] = best_candidate['table_obj']
            logger.info(f"Selected Table #{best_candidate['number']} for {s_type} with score {best_candidate['scores'].get(s_type, 0)}")

    for s_type, table in found_statements.items():
        context = {**base_context, "table_description": s_type.replace('_', ' ').title()}
        scrape_data_from_tables([table], context, all_data_points, table_map)
        status_report['statements'][s_type] = 'Found (Fallback)'

# ==============================================================================
# SECTION 4: MAIN PROCESSING ROUTER
# ==============================================================================

def extract_fiscal_period(html_content):
    """
    Extracts the filing's period end date from the document's header text.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    patterns = [r"for\s+the\s+fiscal\s+year\s+ended", r"for\s+the\s+quarterly\s+period\s+ended"]
    date_pattern = re.compile(r'([a-zA-Z]+\s+\d{1,2}\s*,\s*\d{4})', re.IGNORECASE)
    # Limit search to the beginning of the document for efficiency
    text_blob = ' '.join(tag.get_text(" ", strip=True) for tag in soup.find_all(['p', 'div'], limit=1000))
    if re.search('|'.join(patterns), text_blob, re.IGNORECASE):
        date_match = date_pattern.search(text_blob)
        if date_match:
            try:
                date_str = re.sub(r'\s+,', ',', date_match.group(1))
                return datetime.strptime(date_str, "%B %d, %Y").date(), soup
            except ValueError:
                pass
    return None, soup

def worker_configurer(queue):
    """Configures logging for each parallel worker process."""
    h = logging.handlers.QueueHandler(queue)
    root = logging.getLogger()
    root.addHandler(h)
    root.setLevel(logging.INFO)
    
def process_single_filing(filing_info, terms_dict, queue):
    """
    The main function for a single worker process. It orchestrates the entire parsing
    workflow for one filing, from reading the file to executing the scraping logic.
    """
    worker_configurer(queue)
    logger = logging.getLogger()
    
    filepath, filing_meta = filing_info
    logger.info(f"Processing: {os.path.basename(filepath)}")
    status_report = {'filepath': os.path.basename(filepath), 'statements': {stype: 'Missing' for stype in terms_dict.keys()}}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()

        period_end_date, soup = extract_fiscal_period(html_content)
        if not period_end_date:
            logger.warning(f"-> Skipping {os.path.basename(filepath)}: Could not find period end date.")
            return [], status_report
        
        # Pre-process all tables to assign them unique IDs for tracking
        all_tables = soup.find_all('table')
        table_map = {}
        for i, table in enumerate(all_tables):
            table_id = f"table-{i+1}"
            table['id'] = table_id
            table_map[table] = {"number": i + 1, "id": table_id}

        file_data_points = []
        base_context = {
            'symbol': filing_meta.get('symbol'), 'form_type': filing_meta.get('form_type'),
            'date_filed': filing_meta.get('date_filed'), 'period_end_date': period_end_date,
        }
        
        # --- NEW TOC VALIDATION AND SCRAPING LOGIC ---
        guided_scrape_successful = False
        potential_toc_tables = find_all_toc_tables(soup)
        logger.info(f"Found {len(potential_toc_tables)} potential Table of Contents candidate(s).")

        if potential_toc_tables:
            required_statements = list(terms_dict.keys())
            for i, toc_table in enumerate(potential_toc_tables):
                logger.info(f"--- Analyzing ToC candidate #{i+1} ---")
                filing_index = parse_toc_table_to_index(toc_table, soup)
                
                if not filing_index:
                    logger.warning(f"ToC candidate #{i+1} could not be parsed. Skipping.")
                    continue

                # STEP 1: Use LLM to validate if this ToC contains all required financial statements.
                toc_descriptions = [item['item_description'] for item in filing_index]
                is_financial_toc = validate_financial_toc(toc_descriptions, required_statements)

                if is_financial_toc:
                    logger.info(f"ToC candidate #{i+1} was validated by LLM. Proceeding to scrape.")
                    # STEP 2: If validated, proceed with the detailed guided scrape.
                    if process_guided_scrape(filing_index, soup, base_context, terms_dict, file_data_points, status_report, table_map):
                        guided_scrape_successful = True
                        logger.info(f"Successfully scraped data using ToC candidate #{i+1}. This will be the final ToC used.")
                        break # Exit the loop once a valid, productive ToC is found and used.
                else:
                    logger.info(f"ToC candidate #{i+1} was rejected by LLM. Checking next candidate.")
        
        # STEP 3: Only run the global fallback if the guided scrape was not successful.
        if not guided_scrape_successful:
            logger.info("No ToC was successfully validated and scraped by the LLM.")
            find_and_scrape_financial_statements_fallback(soup, base_context, terms_dict, file_data_points, status_report, table_map)

        logger.info(f"Finished {os.path.basename(filepath)}, found {len(file_data_points)} data points.")
        return file_data_points, status_report

    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {os.path.basename(filepath)}: {e}", exc_info=True)
        return [], status_report
