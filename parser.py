# parser.py
import logging
import re
import os
from datetime import datetime
from bs4 import BeautifulSoup
from llm_analyzer import get_llm_classification

def parse_financial_value(value_str):
    if not value_str: return None
    cleaned_str = value_str.strip().replace('$', '').replace(',', '')
    if cleaned_str in ['—', '-']: return 0.0
    if not cleaned_str: return None
    is_negative = cleaned_str.startswith('(') and cleaned_str.endswith(')')
    if is_negative: cleaned_str = '-' + cleaned_str[1:-1]
    try: return float(cleaned_str)
    except (ValueError, TypeError): return None

def find_table_units(table):
    unit_pattern = re.compile(r'\((?:in\s+)?(?:millions|thousands|billions)[^)]*\)', re.IGNORECASE)
    for row in table.find_all('tr', limit=5):
        match = unit_pattern.search(row.get_text(" ", strip=True))
        if match: return match.group(0)
    for prev_tag in table.find_previous_siblings(limit=15):
        if prev_tag.name in ['p', 'div', 'span', 'b', 'strong']:
            match = unit_pattern.search(prev_tag.get_text(" ", strip=True))
            if match: return match.group(0)
    return None

def parse_table_headers(table):
    header_rows = table.find_all('tr', limit=10)
    year_pattern = re.compile(r'\b(20\d{2})\b')
    for row in header_rows:
        years_found = year_pattern.findall(row.get_text(" ", strip=True))
        if len(set(years_found)) > 1: return sorted(list(set(years_found)), reverse=True)
    return []

def scrape_data_from_tables(tables, context, all_data_points):
    table_counter = context['table_counter']
    for table in tables:
        table_counter[0] += 1
        fiscal_periods = parse_table_headers(table)
        num_periods = len(fiscal_periods)
        if num_periods == 0:
            logging.warning(f"Table #{table_counter[0]}: Could not determine fiscal periods. Skipping.")
            continue
        
        units = find_table_units(table)
        if not units:
            context['missing_units_counter'][0] += 1
            units = "N/A"
        
        current_category = ""
        for row in table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if not cells: continue
            metric_name = cells[0].get_text(" ", strip=True)
            if not metric_name or metric_name.isdigit() or len(metric_name) > 100:
                is_header_or_footer = all(c.get_text(" ", strip=True).isspace() or c.get_text(" ", strip=True) == '' for c in cells[1:])
                if not is_header_or_footer: current_category = metric_name
                continue
            
            full_row_text = " ".join([c.get_text(" ", strip=True) for c in cells[1:]])
            value_strings = re.findall(r'(\(.*?\)|—|[\d,.-]+)', full_row_text)
            
            if len(value_strings) == num_periods:
                for i, period in enumerate(fiscal_periods):
                    value = parse_financial_value(value_strings[i])
                    if value is not None:
                        data_point = {**context, "table_no": table_counter[0], "category": current_category, "metric": metric_name, "value": value, "units": units, "fiscal_period": period}
                        all_data_points.append(data_point)

def extract_fiscal_period(html_content):
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
            except ValueError: pass
    return None, soup

def get_all_terms(data_structure):
    terms = []
    if isinstance(data_structure, list): terms.extend(data_structure)
    elif isinstance(data_structure, dict):
        for value in data_structure.values(): terms.extend(get_all_terms(value))
    return terms

def _score_single_table(table, flat_terms_by_statement, table_idx_for_log):
    header_text_to_check = " ".join(r.get_text(" ", strip=True).lower() for r in table.find_all('tr', limit=5))
    if '%' in header_text_to_check or 'as a percentage' in header_text_to_check:
        return {}
    table_text_lower = table.get_text(" ", strip=True).lower()
    scores = {statement: 0 for statement in flat_terms_by_statement}
    for statement, terms in flat_terms_by_statement.items():
        for term in terms:
            if term in table_text_lower: scores[statement] += 1
    return scores

def find_and_scrape_financial_statements(soup, base_context, terms_dict_granular, all_data_points):
    logging.info("Starting financial statement identification...")
    flat_terms_by_statement = {s: get_all_terms(c) for s, c in terms_dict_granular.items()}
    all_tables = soup.find_all('table')
    scored_tables = []
    for i, table in enumerate(all_tables):
        scores = _score_single_table(table, flat_terms_by_statement, i)
        if any(s > 10 for s in scores.values()):
            scored_tables.append({"index": i, "table_obj": table, "scores": scores, "best_match": max(scores, key=scores.get) if scores else None, "max_score": max(scores.values()) if scores else 0})

    found_statements = {}
    for statement_type in terms_dict_granular.keys():
        best_candidate = max(
            (cand for cand in scored_tables if cand["table_obj"] not in found_statements.values()),
            key=lambda x: x['scores'].get(statement_type, 0),
            default=None
        )
        if best_candidate and best_candidate['scores'].get(statement_type, 0) > 10:
            found_statements[statement_type] = best_candidate["table_obj"]
            logging.info(f"Found '{statement_type}' in Table #{best_candidate['index']}")

    if found_statements:
        for statement_type, table_obj in found_statements.items():
            table_context = {**base_context, "table_description": statement_type.replace('_', ' ').title()}
            scrape_data_from_tables([table_obj], table_context, all_data_points)
    
    return found_statements, scored_tables

def process_single_filing(filing_info, terms_dict):
    filepath, filing_meta = filing_info
    logging.info(f"[PID {os.getpid()}] Processing: {os.path.basename(filepath)}")
    status_report = {'filepath': os.path.basename(filepath), 'statements': {stype: 'Missing' for stype in terms_dict.keys()}}
    try:
        with open(filepath, 'r', encoding='utf-8') as f: html_content = f.read()
        period_end_date, soup = extract_fiscal_period(html_content)
        if not period_end_date:
            logging.warning(f"-> Skipping {os.path.basename(filepath)}: Could not find period end date.")
            return [], status_report, 0

        file_data_points = []
        base_context = {'symbol': filing_meta.get('symbol'), 'form_type': filing_meta.get('form_type'), 'date_filed': filing_meta.get('date_filed'), 'period_end_date': period_end_date, 'table_counter': [0], 'missing_units_counter': [0]}
        found_statements, candidates = find_and_scrape_financial_statements(soup, base_context, terms_dict, file_data_points)
        for stype in found_statements: status_report['statements'][stype] = 'Found'

        if len(found_statements) < 3:
            missing_types = set(terms_dict.keys()) - set(found_statements.keys())
            for missing_type in missing_types:
                potential_tables = [c for c in candidates if c.get("best_match") == missing_type]
                for cand in potential_tables:
                    llm_result = get_llm_classification(cand["table_obj"], missing_type, terms_dict)
                    if llm_result and llm_result.get("identified_statement_type") == missing_type:
                        logging.info(f"  -> LLM CONFIRMED '{missing_type}'. Scraping.")
                        table_context = {**base_context, "table_description": missing_type.replace('_', ' ').title()}
                        scrape_data_from_tables([cand["table_obj"]], table_context, file_data_points)
                        status_report['statements'][missing_type] = 'Found by LLM'
                        break

        logging.info(f"[PID {os.getpid()}] Finished {os.path.basename(filepath)}, found {len(file_data_points)} data points.")
        return file_data_points, status_report, base_context['missing_units_counter'][0]
    except Exception as e:
        logging.error(f"Error processing {os.path.basename(filepath)}: {e}", exc_info=True)
        return [], status_report, 0