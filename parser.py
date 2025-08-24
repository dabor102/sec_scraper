# parser.py

import logging
import re
import os
from datetime import datetime
from bs4 import BeautifulSoup, Tag
from llm_analyzer import get_llm_classification, classify_toc_items

# ==============================================================================
# SECTION 1: CORE PARSING UTILITIES
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
        cleaned_str = '-' + cleaned_str[1:-1]
    try:
        return float(cleaned_str)
    except (ValueError, TypeError):
        return None

def find_table_units(table):
    """
    Searches for financial units (e.g., 'in millions') associated with a table.
    """
    unit_pattern = re.compile(r'\((?:in\s+)?(?:millions|thousands|billions)[^)]*\)', re.IGNORECASE)
    for row in table.find_all('tr', limit=5):
        match = unit_pattern.search(row.get_text(" ", strip=True))
        if match:
            return match.group(0)
    for prev_tag in table.find_previous_siblings(limit=15):
        if prev_tag.name in ['p', 'div', 'span', 'b', 'strong']:
            match = unit_pattern.search(prev_tag.get_text(" ", strip=True))
            if match:
                return match.group(0)
    return None

def parse_table_headers(table):
    """
    Finds the header row in a table and returns an ordered list of fiscal years found.
    """
    header_rows = table.find_all('tr', limit=10)
    year_pattern = re.compile(r'\b(20\d{2})\b')
    for row in header_rows:
        years_found = year_pattern.findall(row.get_text(" ", strip=True))
        if len(set(years_found)) > 1:
            return sorted(list(set(years_found)), reverse=True)
    return []

def scrape_data_from_tables(tables, context, all_data_points, table_map, toc_href=None):
    """
    Extracts financial data from a list of tables.
    """
    for table in tables:
        # Find the table's number and ID from the global map
        table_info = None
        for tbl, info in table_map.items():
            if str(tbl) == str(table):
                table_info = info
                break

        if not table_info:
            continue

        fiscal_periods = parse_table_headers(table)
        num_periods = len(fiscal_periods)
        if num_periods == 0:
            continue

        units = find_table_units(table) or "N/A"
        current_category = ""
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if not cells:
                continue

            metric_name = cells[0].get_text(" ", strip=True)
            if not metric_name or metric_name.isdigit():
                continue

            full_row_text = " ".join([c.get_text(" ", strip=True) for c in cells[1:]])
            value_strings = re.findall(r'(\([\d,.-]+\)|—|[\d,.-]+)', full_row_text)

            # *** NEW LOGIC ***
            # A row is only a category header if it contains no extractable financial values.
            if not value_strings:
                current_category = metric_name
                continue

            if len(value_strings) == num_periods:
                for i, period in enumerate(fiscal_periods):
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
                            # Use ToC href if available, otherwise use the table's own ID
                            "href": toc_href if toc_href else f"#{table_info['id']}"
                        })
# ==============================================================================
# SECTION 2: TOC-GUIDED SCRAPING LOGIC (PRIMARY PATH)
# ==============================================================================

def extract_filing_index(soup):
    """
    Finds and parses the Table of Contents, mapping items to their anchor tags.
    """
    index = []
    toc_table = None

    # Primary Method: Find a centered "TABLE OF CONTENTS" header
    potential_headers = soup.find_all(lambda tag: tag.name in ['p', 'div', 'b'] and re.search(r'^\s*TABLE\s+OF\s+CONTENTS\s*$', tag.get_text(strip=True), re.IGNORECASE))
    for header in potential_headers:
        parent = header.find_parent(('div', 'p')) or header
        if 'center' in parent.get('align', '') or 'text-align:center' in parent.get('style', ''):
            potential_table = parent.find_next('table')
            if potential_table and len(potential_table.find_all('tr')) > 5:
                toc_table = potential_table
                logging.info("Found ToC table via primary method (header search).")
                break

    # Fallback Method: Find any table with a high density of internal links
    if not toc_table:
        for table in soup.find_all('table'):
            rows = table.find_all('tr')
            if len(rows) < 10:
                continue
            link_count = sum(1 for r in rows[:15] if r.find('a', href=lambda h: h and h.startswith('#')))
            if link_count > 5:
                toc_table = table
                logging.info("Found ToC table via fallback method (structural analysis).")
                break

    if not toc_table:
        return []

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
        desc = re.sub(r'[\s.]+\d+\s*$', '', desc).strip()

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
    Extracts all tags between a start and end anchor tag to "slice" the document.
    This final version correctly iterates through siblings starting from the anchor itself.
    """
    content_tags = []
    
    # Start with the anchor tag itself.
    current_tag = start_tag
    
    while current_tag:
        # The end_tag marks the beginning of the *next* section, so we stop when we see it.
        if current_tag == end_tag:
            break
        
        content_tags.append(current_tag)
        
        # Move to the next sibling tag in the document tree.
        current_tag = current_tag.find_next_sibling()

    if not content_tags:
        logging.warning("Slicing function found no content between anchors.")
        return BeautifulSoup("", 'html.parser')

    # Re-parse the collected tags into a new, self-contained soup object for isolated searching.
    return BeautifulSoup("".join(str(t) for t in content_tags), 'html.parser')

def process_guided_scrape(full_index, soup, base_context, terms_dict, all_data_points, status_report, table_map):
    """
    Orchestrates the precise, ToC-guided scraping workflow.
    """
    logging.info("Starting ToC-Guided scraping path...")
    mapped_statements = classify_toc_items(full_index, list(terms_dict.keys()))

    if not mapped_statements:
        logging.warning("LLM could not map any ToC items to statements. Aborting guided scrape.")
        return False

    tag_to_index_pos = {item['anchor_tag']: i for i, item in enumerate(full_index)}

    for statement_type, toc_item in mapped_statements.items():
        start_tag = toc_item['anchor_tag']
        # Capture the href from the ToC item
        toc_anchor_href = toc_item['anchor_href']
        current_pos = tag_to_index_pos.get(start_tag)

        end_tag = None
        if current_pos is not None and current_pos + 1 < len(full_index):
            end_tag = full_index[current_pos + 1]['anchor_tag']

        logging.info(f"Slicing document for '{statement_type}'...")
        section_soup = get_section_content_between_anchors(start_tag, end_tag)

        tables_in_section = section_soup.find_all('table')
        if tables_in_section:
            context = {**base_context, "table_description": statement_type.replace('_', ' ').title()}
            # Pass the toc_anchor_href to the scraping function
            scrape_data_from_tables(tables_in_section, context, all_data_points, table_map, toc_href=toc_anchor_href)
            status_report['statements'][statement_type] = 'Found (ToC-Guided)'
        else:
            logging.warning(f"Found section for '{statement_type}' but no tables within it.")
    
    return True # Indicate success of the guided path

# ==============================================================================
# SECTION 3: FALLBACK SCRAPING LOGIC
# ==============================================================================

def find_and_scrape_financial_statements_fallback(soup, base_context, terms_dict, all_data_points, status_report, table_map):
    """
    Scans all tables in the document if the ToC method fails.
    """
    logging.warning("Executing Fallback scraping path (global table scan).")
    
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
        scores = {}
        text = table.get_text(" ", strip=True).lower()
        if '%' in text[:500]: # Skip common-size percentage tables
            continue
        for s_type, terms in flat_terms.items():
            scores[s_type] = sum(1 for term in terms if term in text)
        if any(s > 10 for s in scores.values()):
            scored_tables.append({'table_obj': table, 'scores': scores})

    found_statements = {}
    for s_type in terms_dict.keys():
        # Find the best candidate table for this statement type that hasn't already been chosen
        best_candidate = max(
            (c for c in scored_tables if c['table_obj'] not in found_statements.values()),
            key=lambda x: x['scores'].get(s_type, 0),
            default=None
        )
        if best_candidate and best_candidate['scores'].get(s_type, 0) > 10:
            found_statements[s_type] = best_candidate['table_obj']

    for s_type, table in found_statements.items():
        context = {**base_context, "table_description": s_type.replace('_', ' ').title()}
        # For fallback, toc_href remains None, so the function will generate the href from the table ID
        scrape_data_from_tables([table], context, all_data_points, table_map)
        status_report['statements'][s_type] = 'Found (Fallback)'

# ==============================================================================
# SECTION 4: MAIN PROCESSING ROUTER
# ==============================================================================

def extract_fiscal_period(html_content):
    """
    Extracts the fiscal period end date from the HTML and creates the initial soup object.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    patterns = [r"for\s+the\s+fiscal\s+year\s+ended", r"for\s+the\s+quarterly\s+period\s+ended"]
    date_pattern = re.compile(r'([a-zA-Z]+\s+\d{1,2}\s*,\s*\d{4})', re.IGNORECASE)
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

def process_single_filing(filing_info, terms_dict):
    """
    Main worker function that routes processing to either the ToC-Guided path or the Fallback path.
    """
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    
    filepath, filing_meta = filing_info
    logging.info(f"[PID {os.getpid()}] Processing: {os.path.basename(filepath)}")
    status_report = {'filepath': os.path.basename(filepath), 'statements': {stype: 'Missing' for stype in terms_dict.keys()}}
    
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()

        period_end_date, soup = extract_fiscal_period(html_content)
        if not period_end_date:
            logging.warning(f"-> Skipping {os.path.basename(filepath)}: Could not find period end date.")
            return [], status_report
        
        # --- TABLE NUMBERING AND ID ASSIGNMENT ---
        all_tables = soup.find_all('table')
        table_map = {}
        for i, table in enumerate(all_tables):
            table_id = f"table-{i+1}"
            table['id'] = table_id
            table_map[table] = {"number": i + 1, "id": table_id}

        file_data_points = []
        base_context = {
            'symbol': filing_meta.get('symbol'),
            'form_type': filing_meta.get('form_type'),
            'date_filed': filing_meta.get('date_filed'),
            'period_end_date': period_end_date,
        }
        
        # --- ROUTING LOGIC ---
        filing_index = extract_filing_index(soup)
        
        guided_scrape_successful = False
        if filing_index:
            guided_scrape_successful = process_guided_scrape(filing_index, soup, base_context, terms_dict, file_data_points, status_report, table_map)
        
        # If the ToC path was not attempted or did not succeed, use the fallback method
        if not guided_scrape_successful:
            find_and_scrape_financial_statements_fallback(soup, base_context, terms_dict, file_data_points, status_report, table_map)

        logging.info(f"[PID {os.getpid()}] Finished {os.path.basename(filepath)}, found {len(file_data_points)} data points.")
        return file_data_points, status_report

    except Exception as e:
        logging.error(f"An unexpected error occurred while processing {os.path.basename(filepath)}: {e}", exc_info=True)
        return [], status_report