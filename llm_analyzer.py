# llm_analyzer.py
import logging
import json
import os
import re
import google.generativeai as genai

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

def validate_financial_toc(toc_descriptions, statement_types):
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
        response = model.generate_content(prompt)

        # Robustly find the JSON object in the response
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



def get_llm_classification(table_obj, potential_statement_type, all_keywords):
    """
    Asks Gemini to identify which financial statement a table represents.
    (This function is used in the fallback method)
    """
    if not GEMINI_CONFIGURED:
        return None
    # ... (rest of the function remains unchanged) ...
    try:
        model = genai.GenerativeModel('gemini-1.5-flash')
    except Exception as e:
        logging.warning(f"Gemini model could not be initialized, skipping LLM classification. Error: {e}")
        return None

    markdown_table = ""
    for row in table_obj.find_all('tr', limit=20):
        cols = [col.get_text(" ", strip=True).replace('|', '') for col in row.find_all(['th', 'td'])]
        markdown_table += "| " + " | ".join(cols) + " |\n"

    prompt = f"""
    You are an expert financial analyst. Your task is to identify if the table below belongs to one of the following statement_types: INCOME_STATEMENT, BALANCE_SHEET_STATEMENT, CASH_FLOW_STATEMENT.
    Analyze the table and respond with a JSON object ONLY in the format: {{"identified_statement_type": "YOUR_CONCLUSION"}}
    Table Content:
    ```markdown
    {markdown_table}
    ```
    """
    try:
        response = model.generate_content(prompt)
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        result = json.loads(cleaned_response)
        return result if "identified_statement_type" in result else None
    except Exception as e:
        logging.error(f"Gemini classification failed or returned invalid JSON. Error: {e}")
        return None

### NEW ###
def classify_toc_items(toc_items, statement_types):
    """
    Uses an LLM to classify ToC item descriptions into financial statement types.
    """
    if not GEMINI_CONFIGURED:
        logging.warning("Gemini not configured, cannot classify ToC items.")
        return {}

    toc_descriptions = [item.get('item_description') for item in toc_items if item.get('item_description')]

    # This new prompt is more robust and includes a one-shot example to guide the model.
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
        response = model.generate_content(prompt)
        
        # Robust JSON parsing
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
            # This is the source of the warning. With the new prompt, this should happen less often.
            logging.warning("LLM could not map any ToC items to statements. Prompt may need tuning or ToC is non-standard.")

        return final_mapping

    except Exception as e:
        logging.error(f"Error during LLM ToC classification: {e}")
        return {}