# llm_analyzer.py
import logging
import json
import os
import re
import google.generativeai as genai

# --- Token Counter ---
class TokenCounter:
    """A simple class to count input and output tokens for API calls."""
    def __init__(self):
        self.input_tokens = 0
        self.output_tokens = 0

    def add_input(self, token_count):
        self.input_tokens += token_count

    def add_output(self, token_count):
        self.output_tokens += token_count

    @property
    def total_tokens(self):
        return self.input_tokens + self.output_tokens

    def get_counts(self):
        return {
            "input": self.input_tokens,
            "output": self.output_tokens,
            "total": self.total_tokens
        }

# --- Gemini API Configuration ---
GEMINI_CONFIGURED = False
try:
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)
        genai.list_models()
        GEMINI_CONFIGURED = True
        logging.info("Gemini API configured successfully.")
    else:
        logging.warning("GEMINI_API_KEY environment variable not set. LLM features will be disabled.")
except Exception as e:
    logging.warning(f"Could not configure Gemini API. LLM features will be disabled. Error: {e}")

def validate_financial_toc(toc_descriptions, statement_types, token_counter):
    """
    Uses an LLM to validate if a list of ToC descriptions likely contains all required financial statements.
    """
    if not GEMINI_CONFIGURED:
        logging.warning("Gemini not configured, cannot validate ToC.")
        return False

    prompt = f"""
You are an expert financial analyst. Your task is to determine if a given Table of Contents (ToC) is a specific "Index to Financial Statements".

A ToC is considered a valid "Index to Financial Statements" ONLY IF it contains clear references to ALL THREE of the following statement types:
{json.dumps(statement_types, indent=2)}

Analyze the list of "Available ToC Descriptions" below. Based on your analysis, decide if it meets the condition.

Available ToC Descriptions:
{json.dumps(toc_descriptions, indent=2)}

Respond with a single JSON object ONLY, in the format: {{"is_complete_financial_toc": boolean}}
- Set `is_complete_financial_toc` to `true` if you are confident that entries for all three required statement types are present.
- Set `is_complete_financial_toc` to `false` otherwise.
"""
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Count input tokens
        input_token_count = model.count_tokens(prompt).total_tokens
        token_counter.add_input(input_token_count)
        
        response = model.generate_content(prompt)

        # Count output tokens
        output_token_count = model.count_tokens(response.text).total_tokens
        token_counter.add_output(output_token_count)

        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if not json_match:
            logging.error(f"LLM validator did not return a valid JSON object. Response: {response.text}")
            return False

        result = json.loads(json_match.group(0))
        is_complete = result.get("is_complete_financial_toc", False)

        if is_complete:
            logging.info("LLM validated this ToC as a complete financial index.")
        else:
            logging.info("LLM did not validate this ToC as a complete financial index.")

        return is_complete

    except Exception as e:
        logging.error(f"Error during LLM ToC validation: {e}")
        return False

def classify_toc_items(toc_items, statement_types, token_counter):
    """
    Uses an LLM to classify ToC item descriptions into financial statement types.
    """
    if not GEMINI_CONFIGURED:
        logging.warning("Gemini not configured, cannot classify ToC items.")
        return {}

    toc_descriptions = [item.get('item_description') for item in toc_items if item.get('item_description')]

    prompt = f"""
You are a helpful assistant designed to return structured JSON data.
Your task is to analyze a list of item descriptions from a financial filing's Table of Contents and map them to a predefined list of financial statement types.

1.  **Required Statement Types**: You must find a match for each of these types:
    {json.dumps(statement_types, indent=2)}

2.  **Available Descriptions**: Here are the descriptions from the Table of Contents:
    {json.dumps(toc_descriptions, indent=2)}

3.  **Task**:
    - Review the "Available Descriptions" and find the best match for each of the "Required Statement Types".
    - The match must be the **EXACT** string from the "Available Descriptions" list.
    - If you cannot find a confident match for a specific statement type, omit it from your response.

4.  **Output Format**: Your entire response must be a single JSON object, with no other text before or after it.

    **Example Output:**
    {{
      "INCOME_STATEMENT": "Condensed Consolidated Statements of Operations",
      "BALANCE_SHEET_STATEMENT": "Condensed Consolidated Balance Sheets",
      "CASH_FLOW_STATEMENT": "Condensed Consolidated Statements of Cash Flows"
    }}
"""
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
        
        # Count input tokens
        input_token_count = model.count_tokens(prompt).total_tokens
        token_counter.add_input(input_token_count)

        response = model.generate_content(prompt)
        
        # Count output tokens
        output_token_count = model.count_tokens(response.text).total_tokens
        token_counter.add_output(output_token_count)
        
        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if not json_match:
            logging.error(f"LLM did not return a valid JSON object. Response: {response.text}")
            return {}
        
        mapped_descriptions = json.loads(json_match.group(0))

        final_mapping = {}
        desc_to_item_map = {item['item_description']: item for item in toc_items}

        for statement_type, description in mapped_descriptions.items():
            if statement_type in statement_types and description in desc_to_item_map:
                final_mapping[statement_type] = desc_to_item_map[description]
        
        if final_mapping:
            logging.info(f"LLM successfully mapped {len(final_mapping)} ToC items to financial statements.")
        else:
            logging.warning("LLM could not map any ToC items to statements.")

        return final_mapping

    except Exception as e:
        logging.error(f"Error during LLM ToC classification: {e}")
        return {}

def classify_table_by_surrounding_text(text_snippet, statement_types, token_counter):
    """
    Uses an LLM to classify a text snippet (e.g., a table header) into a financial statement type.
    """
    if not GEMINI_CONFIGURED:
        logging.warning("Gemini not configured, cannot classify table by text.")
        return None

    if len(text_snippet) > 1000 or text_snippet.lower().startswith("note"):
        return None

    prompt = f"""
You are an expert financial analyst. Your task is to identify which financial statement a given text heading refers to.

The possible financial statement types are:
{json.dumps(statement_types, indent=2)}

Analyze the "Text Heading" below. Based on your analysis, determine which statement type it represents.

Text Heading:
"{text_snippet}"

Respond with a single JSON object ONLY, in the format: {{"identified_statement_type": "YOUR_CONCLUSION"}}
- The value for "identified_statement_type" must be one of the provided statement types or "None" if it does not match any.
"""
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')

        # Count input tokens
        input_token_count = model.count_tokens(prompt).total_tokens
        token_counter.add_input(input_token_count)
        
        response = model.generate_content(prompt)
        
        # Count output tokens
        output_token_count = model.count_tokens(response.text).total_tokens
        token_counter.add_output(output_token_count)

        json_match = re.search(r'\{.*\}', response.text, re.DOTALL)
        if not json_match:
            logging.error(f"LLM text classifier did not return a valid JSON object. Response: {response.text}")
            return None
            
        result = json.loads(json_match.group(0))
        identified_type = result.get("identified_statement_type")

        if identified_type in statement_types:
            logging.info(f"LLM classified text snippet as: {identified_type}")
            return identified_type
        else:
            return None

    except Exception as e:
        logging.error(f"Error during LLM table text classification: {e}")
        return None