# llm_analyzer.py
import logging
import json
import os
import google.generativeai as genai

# --- Gemini API Configuration ---
GEMINI_CONFIGURED = False
try:
    # It's recommended to use an environment variable for the API key.
    # E.g., set GEMINI_API_KEY in your shell or a .env file.
    api_key = os.getenv("GEMINI_API_KEY")
    if api_key:
        genai.configure(api_key=api_key)
        genai.list_models() # Test if the API key is valid
        GEMINI_CONFIGURED = True
        logging.info("Gemini API configured successfully.")
    else:
        logging.warning("GEMINI_API_KEY environment variable not set. LLM features will be disabled.")
except Exception as e:
    logging.warning(f"Could not configure Gemini API. LLM features will be disabled. Error: {e}")


def get_llm_classification(table_obj, potential_statement_type, all_keywords):
    """
    Asks Gemini to identify which financial statement a table represents.
    """
    if not GEMINI_CONFIGURED:
        logging.warning("Gemini not configured, skipping LLM classification.")
        return None

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

    Use the provided keywords as a guide: {json.dumps(all_keywords, indent=2)}

    After analyzing the table, respond with a JSON object ONLY in the following format:
    {{
      "identified_statement_type": "YOUR_CONCLUSION"
    }}

    Replace "YOUR_CONCLUSION" with the one statement type you are most confident about.

    Table Content:
    ```markdown
    {markdown_table}
    ```
    """

    try:
        response = model.generate_content(prompt)
        # More robust parsing
        cleaned_response = response.text.strip().replace("```json", "").replace("```", "")
        result = json.loads(cleaned_response)
        
        if "identified_statement_type" in result:
            return result
        else:
            logging.warning("LLM response lacked 'identified_statement_type' key.")
            return None
    except Exception as e:
        logging.error(f"Gemini classification failed or returned invalid JSON. Error: {e}")
        return None