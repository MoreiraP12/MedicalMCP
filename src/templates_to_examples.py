import pandas as pd
import io
import sys
import argparse # For command-line arguments
import re # For finding placeholders
import itertools # For generating permutations
import csv # For writing output CSV
from google.cloud import bigquery # Re-added for BigQuery access
from collections import defaultdict # To store fetched examples

# --- Configuration ---
# Public PhysioNet project containing MIMIC-IV data
PHYSIONET_PROJECT = "physionet-data"

# Specific MIMIC-IV v3.1 dataset names (and optionally ED)
# Adjust if your access uses different names
MIMIC_HOSP_DATASET = f"{PHYSIONET_PROJECT}.mimiciv_3_1_hosp"
MIMIC_ICU_DATASET = f"{PHYSIONET_PROJECT}.mimiciv_3_1_icu"
# MIMIC_ED_DATASET = f"{PHYSIONET_PROJECT}.mimiciv_ed" # Uncomment if needed

# IMPORTANT: Use YOUR Google Cloud project ID for billing and authentication.
BILLING_PROJECT_ID = "level-strategy-383218" # <<<--- REPLACE WITH YOUR ACTUAL BILLING PROJECT ID

# --- BigQuery Client Initialization ---
# Moved initialization logic here to be accessible globally after validation
bq_client = None

def initialize_bq_client():
    """Initializes the global BigQuery client."""
    global bq_client
    # Validate BILLING_PROJECT_ID
    """ if not BILLING_PROJECT_ID or BILLING_PROJECT_ID == "your-billing-project-id-here" or BILLING_PROJECT_ID == "level-strategy-383218": # Check generic placeholder too
         print(f"\n!!! ERROR: BILLING_PROJECT_ID is set to '{BILLING_PROJECT_ID}'.")
         print("!!! Please replace it with YOUR actual Google Cloud project ID in the script.")
         return False # Indicate failure
    """
    print(f"Attempting to connect to Google BigQuery using billing project: {BILLING_PROJECT_ID}")
    try:
        # Initialize client with the specified BILLING_PROJECT_ID
        bq_client = bigquery.Client(project=BILLING_PROJECT_ID)
        # Simple test query to check connection and permissions
        bq_client.query(f"SELECT 1 FROM `{MIMIC_HOSP_DATASET}.patients` LIMIT 1").result()
        print("BigQuery client initialized and connection tested successfully.")
        return True # Indicate success

    except Exception as e:
        print(f"\n!!! Error initializing BigQuery client or testing connection for project {BILLING_PROJECT_ID}: {e}")
        print("!!! Please check:")
        print("!!! 1. Google Cloud SDK Authentication (`gcloud auth application-default login` or service account key)")
        print(f"!!! 2. The BILLING_PROJECT_ID ('{BILLING_PROJECT_ID}') is correct and exists.")
        print(f"!!! 3. Necessary IAM roles are granted on project '{BILLING_PROJECT_ID}' (e.g., BigQuery User) and '{PHYSIONET_PROJECT}' (BigQuery Data Viewer).")
        print(f"!!! 4. The BigQuery API is enabled in your Google Cloud project ('{BILLING_PROJECT_ID}').")
        bq_client = None # Ensure client is None on failure
        return False # Indicate failure

# --- Functions ---

def fetch_dynamic_examples(placeholder_type: str, fetch_limit: int) -> list[str]:
    """
    Fetches a list of example values for a given placeholder type by querying BigQuery.
    """
    global bq_client
    if not bq_client:
        print(f"Error: BigQuery client not initialized. Cannot fetch examples for {placeholder_type}.", file=sys.stderr)
        return []

    print(f"Fetching up to {fetch_limit} examples for placeholder type: {placeholder_type}...")
    query = ""
    results = []

    try:
        # Define targeted queries for each placeholder type
        # These queries aim for common, readable examples and use LIMIT
        if placeholder_type == "Exposure/Intervention":
            # Get common drugs and procedure titles
            query = f"""
            (SELECT DISTINCT drug AS example FROM `{MIMIC_HOSP_DATASET}.prescriptions` WHERE drug IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT DISTINCT LTRIM(RTRIM(long_title)) AS example FROM `{MIMIC_HOSP_DATASET}.d_icd_procedures` WHERE long_title IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            ORDER BY example
            LIMIT {fetch_limit}
            """
        elif placeholder_type == "Outcome":
             # Get common discharge locations and diagnosis titles
            query = f"""
            (SELECT DISTINCT discharge_location AS example FROM `{MIMIC_HOSP_DATASET}.admissions` WHERE discharge_location IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT DISTINCT LTRIM(RTRIM(long_title)) AS example FROM `{MIMIC_HOSP_DATASET}.d_icd_diagnoses` WHERE long_title IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT 'In-hospital mortality' AS example) -- Add common outcome explicitly
            ORDER BY example
            LIMIT {fetch_limit}
            """
        elif placeholder_type == "Measurement":
            # Get common lab test labels and chartevent labels (vitals/common assessments)
            query = f"""
            (SELECT DISTINCT label AS example FROM `{MIMIC_ICU_DATASET}.d_labitems` WHERE fluid = 'Blood' AND label IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT DISTINCT label AS example FROM `{MIMIC_ICU_DATASET}.d_items` WHERE category IN ('Routine Vital Signs', 'Respiratory', 'Labs', 'Neurological') AND label IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            ORDER BY example
            LIMIT {fetch_limit}
            """
        elif placeholder_type == "Patient Population":
             # Get common admission diagnoses and diagnosis titles
            query = f"""
            (SELECT DISTINCT diagnosis AS example FROM `{MIMIC_HOSP_DATASET}.admissions` WHERE diagnosis IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT DISTINCT LTRIM(RTRIM(long_title)) AS example FROM `{MIMIC_HOSP_DATASET}.d_icd_diagnoses` WHERE long_title IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            ORDER BY example
            LIMIT {fetch_limit}
            """
        elif placeholder_type == "Outcome/Measurement":
             # Combine Outcome and Measurement queries (simplified version)
             query = f"""
            (SELECT DISTINCT label AS example FROM `{MIMIC_ICU_DATASET}.d_labitems` WHERE fluid = 'Blood' AND label IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT DISTINCT LTRIM(RTRIM(long_title)) AS example FROM `{MIMIC_HOSP_DATASET}.d_icd_diagnoses` WHERE long_title IS NOT NULL ORDER BY COUNT(*) DESC LIMIT {fetch_limit // 2})
            UNION ALL
            (SELECT 'In-hospital mortality' AS example)
            ORDER BY example
            LIMIT {fetch_limit}
             """
        else:
            print(f"Warning: No query defined for placeholder type '{placeholder_type}'.", file=sys.stderr)
            return []

        # Execute the query
        if query:
            print(f"  Running query for {placeholder_type}...")
            query_job = bq_client.query(query)
            results_df = query_job.result().to_dataframe()
            results = results_df['example'].astype(str).tolist()
            print(f"  Fetched {len(results)} examples.")

    except Exception as e:
        print(f"Error executing BigQuery query for {placeholder_type}: {e}", file=sys.stderr)
        # Extract more specific BQ error details if available
        if hasattr(e, 'errors') and e.errors:
            try:
                error_detail = e.errors[0].get('message', 'No specific error message found.')
                print(f"  BigQuery Error Detail: {error_detail}", file=sys.stderr)
            except Exception: pass # Ignore parsing errors
        return [] # Return empty list on error

    return results


def find_placeholders(template_string: str) -> list[str]:
    """Finds all unique placeholders like [Placeholder Name] in a string."""
    # Regex to find text within square brackets
    # Use set to get unique placeholders
    return list(set(re.findall(r'\[(.*?)\]', template_string)))

def generate_permutations(template_string: str, placeholder_map: dict, limit: int) -> list[str]:
    """Generates question permutations by substituting placeholders."""
    # Note: This function now expects placeholder_map to contain dynamically fetched lists
    placeholders_in_template = find_placeholders(template_string)
    if not placeholders_in_template:
        return [template_string] # Return template itself if no placeholders

    # Get the example lists for the placeholders found in this specific template
    example_lists = []
    valid_template = True
    for ph in placeholders_in_template:
        if ph in placeholder_map and placeholder_map[ph]: # Check if list exists and is not empty
            example_lists.append(placeholder_map[ph])
        else:
            print(f"Warning: No examples fetched or found for placeholder '[{ph}]' in template '{template_string}'. Skipping permutations for this template.", file=sys.stderr)
            valid_template = False
            break # Stop processing this template

    if not valid_template:
        return []

    generated_questions = []
    # Use itertools.product to get combinations of examples
    count = 0
    try:
        for combo in itertools.product(*example_lists):
            if count >= limit:
                print(f"Limit of {limit} permutations reached for template: '{template_string[:50]}...'")
                break

            temp_question = template_string
            # Create a mapping for this specific combo
            combo_map = dict(zip(placeholders_in_template, combo))

            # Replace placeholders using the combo map
            # This handles cases where a placeholder appears multiple times
            for ph_name, ph_value in combo_map.items():
                 temp_question = temp_question.replace(f'[{ph_name}]', str(ph_value))


            generated_questions.append(temp_question)
            count += 1
    except TypeError as e:
         print(f"Error during permutation generation (likely empty list for a placeholder): {e}", file=sys.stderr)
         return [] # Return empty on error


    return generated_questions

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate example research questions by permuting placeholders using dynamically fetched MIMIC-IV examples.")
    parser.add_argument("-i", "--input", required=True, help="Path to the input CSV file containing question templates.")
    parser.add_argument("-o", "--output", required=True, help="Path to the output CSV file where generated questions will be saved.")
    parser.add_argument("-l", "--limit", type=int, default=10, help="Maximum number of permutations to generate per template (default: 10).")
    parser.add_argument("--fetch-limit", type=int, default=20, help="Maximum number of example values to fetch from BigQuery per placeholder type (default: 20).")

    args = parser.parse_args()

    print(f"--- MIMIC-IV Question Permutation Generator (Dynamic Examples) ---")
    print(f"Input template file: {args.input}")
    print(f"Output file: {args.output}")
    print(f"Permutation limit per template: {args.limit}")
    print(f"Example fetch limit per placeholder: {args.fetch_limit}")

    # Initialize BigQuery Client - Exit if fails
    if not initialize_bq_client():
        sys.exit(1)

    output_rows = [] # List to hold rows for the output CSV
    fetched_examples_cache = defaultdict(list) # Cache fetched examples

    try:
        print(f"Reading input CSV: {args.input}")
        input_df = pd.read_csv(args.input)
        if 'Original_Phrasing' not in input_df.columns:
             raise ValueError("Input CSV must contain a column named 'Original_Phrasing'")
        if 'Template_Type' not in input_df.columns:
             print("Warning: 'Template_Type' column not found in input CSV. Output will not include it.", file=sys.stderr)

        print("Processing templates and fetching examples as needed...")
        total_generated = 0
        # First pass: Identify all unique placeholders needed across all templates
        all_needed_placeholders = set()
        for template in input_df['Original_Phrasing']:
            placeholders = find_placeholders(template)
            # Handle combined placeholder
            cleaned_placeholders = set()
            for ph in placeholders:
                if ph == "Outcome/Measurement":
                    cleaned_placeholders.add("Outcome")
                    cleaned_placeholders.add("Measurement")
                else:
                    cleaned_placeholders.add(ph)
            all_needed_placeholders.update(cleaned_placeholders)

        # Second pass: Fetch examples for each unique placeholder type
        print(f"Identified needed placeholder types: {list(all_needed_placeholders)}")
        for ph_type in all_needed_placeholders:
            if ph_type not in fetched_examples_cache: # Avoid re-fetching
                 fetched_examples_cache[ph_type] = fetch_dynamic_examples(ph_type, args.fetch_limit)

        # Combine Outcome and Measurement if needed for the special placeholder
        if "Outcome" in fetched_examples_cache or "Measurement" in fetched_examples_cache:
             fetched_examples_cache["Outcome/Measurement"] = list(set(
                 fetched_examples_cache.get("Outcome", []) + fetched_examples_cache.get("Measurement", [])
             ))[:args.fetch_limit] # Apply limit again after combining


        print("\nGenerating permutations using fetched examples...")
        # Third pass: Generate permutations for each template
        for index, row in input_df.iterrows():
            template = row['Original_Phrasing']
            template_type = row.get('Template_Type', 'N/A')

            # Pass the cache of fetched examples to the generation function
            permutations = generate_permutations(template, fetched_examples_cache, args.limit)

            for question in permutations:
                output_rows.append({'Template_Type': template_type, 'Generated_Question': question})
                total_generated += 1

        print(f"Generated a total of {total_generated} questions.")

    except FileNotFoundError:
        print(f"Error: Input file not found at {args.input}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred during processing: {e}", file=sys.stderr)
        sys.exit(1)

    # Write the results to the output CSV
    if output_rows:
        try:
            print(f"Writing {len(output_rows)} rows to output CSV: {args.output}")
            output_df = pd.DataFrame(output_rows)
            output_df.to_csv(args.output, index=False, quoting=csv.QUOTE_ALL)
            print("Output CSV written successfully.")
        except Exception as e:
            print(f"Error writing output CSV file: {e}", file=sys.stderr)
            sys.exit(1)
    else:
        print("No permutations were generated to write to the output file.")

