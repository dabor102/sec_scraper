# parser.py

import logging
import logging.handlers
import re
import os
from datetime import datetime
from bs4 import BeautifulSoup, Tag
# Import the TokenCounter and the updated LLM functions
from llm_analyzer import TokenCounter, classify_toc_items, validate_financial_toc, classify_table_by_surrounding_text

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
            # logger.info(f"Header parse found raw years: {years_found} in text: '{header_text[:150]}...'")
            return sorted(years_found, reverse=True)
            
    # logger.warning("Could not find any valid fiscal periods in table headers.")
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

            if not value_strings:
                current_category = metric_name
                continue
            
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
    """
    toc_tables = []
    potential_headers = soup.find_all(lambda tag: tag.name in ['p', 'div', 'b'] and re.search(r'^\s*(TABLE\s+OF\s+CONTENTS|INDEX\s+TO\s+FINANCIAL\s+STATEMENTS)\s*$', tag.get_text(strip=True), re.IGNORECASE))
    for header in potential_headers:
        parent = header.find_parent(('div', 'p')) or header
        if 'center' in parent.get('align', '') or 'text-align:center' in parent.get('style', ''):
            potential_table = parent.find_next('table')
            if potential_table and len(potential_table.find_all('tr')) > 5:
                if potential_table not in toc_tables:
                    toc_tables.append(potential_table)
    for table in soup.find_all('table'):
        if table in toc_tables: continue
        rows = table.find_all('tr')
        if len(rows) < 10: continue
        link_count = sum(1 for r in rows[:20] if r.find('a', href=lambda h: h and h.startswith('#')))
        if link_count > 7:
            toc_tables.append(table)
    return toc_tables

def parse_toc_table_to_index(toc_table, soup):
    """
    Parses a single BeautifulSoup table object into a structured list of items.
    """
    index = []
    for row in toc_table.find_all('tr'):
        links = row.find_all('a', href=lambda href: href and href.startswith('#'))
        if not links: continue
        main_href = links[-1]['href']
        anchor_name = main_href.lstrip('#')
        anchor_tag = soup.find('a', {'name': anchor_name}) or soup.find(id=anchor_name)
        text = row.get_text(" ", strip=True).replace('\xa0', ' ')
        item_match = re.search(r'(ITEM\s+\d+[A-Z]?\.?)', text, re.IGNORECASE)
        desc = re.sub(r'^\s*ITEM\s+\d+[A-Z]?\.?\s*', '', text, flags=re.IGNORECASE).strip()
        desc = re.sub(r'[\s.]+\d+\s*$', '', desc).strip()
        if desc and anchor_tag:
            index.append({'item_no': item_match.group(1).upper().strip() if item_match else "N/A", 'item_description': desc, 'anchor_href': main_href, 'anchor_tag': anchor_tag})
    return index

def get_section_content_between_anchors(start_tag, end_tag):
    """
    "Slices" the HTML document by collecting all sibling tags between a start and end anchor.
    """
    content_tags = []
    current_tag = start_tag
    while current_tag:
        if current_tag == end_tag: break
        content_tags.append(current_tag)
        current_tag = current_tag.find_next_sibling()
    if not content_tags:
        return BeautifulSoup("", 'html.parser')
    return BeautifulSoup("".join(str(t) for t in content_tags), 'html.parser')

def get_text_between_elements(start_element, end_element):
    """
    Extracts and cleans all the text between a start and end HTML element.
    """
    text_parts = []
    current_element = start_element.find_next_sibling()
    while current_element and current_element != end_element:
        if isinstance(current_element, Tag) and current_element.name != 'table':
            text_parts.append(current_element.get_text(" ", strip=True))
        current_element = current_element.find_next_sibling()
    full_text = " ".join(text_parts).strip()
    return re.sub(r'\s+', ' ', full_text)

def process_guided_scrape(full_index, soup, base_context, terms_dict, all_data_points, status_report, table_map, token_counter):
    """
    The main guided scraping workflow.
    """
    mapped_statements = classify_toc_items(full_index, list(terms_dict.keys()), token_counter)
    if not mapped_statements:
        return False
    statements_found_in_this_run = 0
    tag_to_index_pos = {item['anchor_tag']: i for i, item in enumerate(full_index)}
    needed_statements = set(mapped_statements.keys())
    for toc_statement_type, toc_item in mapped_statements.items():
        start_tag = toc_item['anchor_tag']
        toc_anchor_href = toc_item['anchor_href']
        current_pos = tag_to_index_pos.get(start_tag)
        end_tag = None
        if current_pos is not None and current_pos + 1 < len(full_index):
            end_tag = full_index[current_pos + 1]['anchor_tag']
        section_soup = get_section_content_between_anchors(start_tag, end_tag)
        tables_in_section = section_soup.find_all('table')
        if not tables_in_section: continue
        last_element = start_tag
        for table in tables_in_section:
            header_text = get_text_between_elements(last_element, table)
            if not header_text:
                last_element = table
                continue
            classified_type = classify_table_by_surrounding_text(header_text, list(terms_dict.keys()), token_counter)
            if classified_type and classified_type in needed_statements:
                context = {**base_context, "table_description": classified_type.replace('_', ' ').title()}
                initial_data_point_count = len(all_data_points)
                scrape_data_from_tables([table], context, all_data_points, table_map, toc_href=toc_anchor_href)
                if len(all_data_points) > initial_data_point_count:
                    status_report['statements'][classified_type] = 'Found (ToC-Guided & Verified)'
                    statements_found_in_this_run += 1
                    needed_statements.remove(classified_type)
            last_element = table
    return statements_found_in_this_run > 0

# ==============================================================================
# SECTION 3: FALLBACK SCRAPING LOGIC
# ==============================================================================

def find_and_scrape_financial_statements_fallback(soup, base_context, terms_dict, all_data_points, status_report, table_map, token_counter):
    """
    A multi-stage fallback method with the fix implemented.
    """
    logger = logging.getLogger()
    logger.warning("Executing Fallback scraping path...")
    
    all_tables = soup.find_all('table')
    needed_statements = set(terms_dict.keys())
    found_tables_by_type = {}
    processed_tables = set()

    # --- STAGE 1: LLM Header Classification ---
    logger.info("Fallback Stage 1: Attempting LLM header classification.")
    
    last_element = soup.find('body')
    for table in all_tables:
        if not needed_statements: break
        header_text = get_text_between_elements(last_element, table)
        if header_text:
            classified_type = classify_table_by_surrounding_text(header_text, list(needed_statements), token_counter)
            if classified_type:
                logger.info(f"LLM Fallback identified a potential '{classified_type}' for table #{table_map.get(table, {}).get('number')}.")
                if not parse_table_headers(table):
                    logger.warning(f"Table identified as '{classified_type}' but headers are not parsable. Skipping.")
                    status_report['statements'][classified_type] = 'Identified but Failed'
                    needed_statements.remove(classified_type)
                    processed_tables.add(table)
                    last_element = table
                    continue
                context = {**base_context, "table_description": classified_type.replace('_', ' ').title()}
                initial_data_count = len(all_data_points)
                scrape_data_from_tables([table], context, all_data_points, table_map)
                if len(all_data_points) > initial_data_count:
                    status_report['statements'][classified_type] = 'Found (Fallback - LLM)'
                    needed_statements.remove(classified_type)
                    found_tables_by_type[classified_type] = table
                    processed_tables.add(table)
        last_element = table

    if not needed_statements: return

    # --- STAGE 2: Keyword Scoring (for remaining statements) ---
    logger.info(f"Fallback Stage 2: Attempting keyword scoring for remaining: {list(needed_statements)}")
    
    def get_all_terms(data):
        if isinstance(data, list): return data
        terms = []
        if isinstance(data, dict):
            for v in data.values(): terms.extend(get_all_terms(v))
        return terms

    remaining_terms = {s_type: terms_dict[s_type] for s_type in needed_statements}
    flat_terms = {s: get_all_terms(c) for s, c in remaining_terms.items()}
    
    scored_tables = []
    tables_to_score = [t for t in all_tables if t not in processed_tables]

    for table in tables_to_score:
        table_info = table_map.get(table, {"number": "N/A"})
        scores = {}
        text = table.get_text(" ", strip=True).lower()
        if '%' in text[:500]: continue
        for s_type, terms in flat_terms.items():
            scores[s_type] = sum(1 for term in terms if term in text)
        if any(s > 5 for s in scores.values()):
            scored_tables.append({'table_obj': table, 'scores': scores, 'number': table_info['number']})

    for s_type in list(needed_statements):
        best_candidate = max(
            (c for c in scored_tables if c['table_obj'] not in found_tables_by_type.values()),
            key=lambda x: x['scores'].get(s_type, 0),
            default=None
        )
        if best_candidate and best_candidate['scores'].get(s_type, 0) > 10:
            table = best_candidate['table_obj']
            found_tables_by_type[s_type] = table
            context = {**base_context, "table_description": s_type.replace('_', ' ').title()}
            scrape_data_from_tables([table], context, all_data_points, table_map)
            status_report['statements'][s_type] = 'Found (Fallback - Score)'
            needed_statements.remove(s_type)

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
    The main function for a single worker process.
    """
    worker_configurer(queue)
    logger = logging.getLogger()
    
    filepath, filing_meta = filing_info
    logger.info(f"Processing: {os.path.basename(filepath)}")
    status_report = {'filepath': os.path.basename(filepath), 'statements': {stype: 'Missing' for stype in terms_dict.keys()}}
    
    # Each worker process gets its own token counter
    token_counter = TokenCounter()

    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            html_content = f.read()

        period_end_date, soup = extract_fiscal_period(html_content)
        if not period_end_date:
            return [], status_report, token_counter.get_counts()
        
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
        
        guided_scrape_successful = False
        potential_toc_tables = find_all_toc_tables(soup)

        if potential_toc_tables:
            required_statements = list(terms_dict.keys())
            for i, toc_table in enumerate(potential_toc_tables):
                filing_index = parse_toc_table_to_index(toc_table, soup)
                if not filing_index: continue
                
                toc_descriptions = [item['item_description'] for item in filing_index]
                is_financial_toc = validate_financial_toc(toc_descriptions, required_statements, token_counter)

                if is_financial_toc:
                    if process_guided_scrape(filing_index, soup, base_context, terms_dict, file_data_points, status_report, table_map, token_counter):
                        guided_scrape_successful = True
                        break
        
        if not guided_scrape_successful:
            find_and_scrape_financial_statements_fallback(soup, base_context, terms_dict, file_data_points, status_report, table_map, token_counter)

        logger.info(f"Finished {os.path.basename(filepath)}, found {len(file_data_points)} data points.")
        return file_data_points, status_report, token_counter.get_counts()

    except Exception as e:
        logger.error(f"An unexpected error occurred while processing {os.path.basename(filepath)}: {e}", exc_info=True)
        return [], status_report, token_counter.get_counts()