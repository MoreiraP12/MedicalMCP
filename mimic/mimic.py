import os
from typing import Any, Dict, Literal, Optional
from google.cloud import bigquery
import pandas as pd
from pydantic import BaseModel, Field # Using Pydantic for structured parameters

# Assuming FastMCP is installed and available
# pip install fastmcp google-cloud-bigquery pandas db-dtypes pydantic
# Ensure db-dtypes is installed for pandas compatibility with some BQ types
try:
    from mcp.server.fastmcp import FastMCP
except ImportError:
    print("Error: FastMCP library not found. Please install it: pip install fastmcp")
    exit(1)

# --- Configuration ---
# Public PhysioNet project and datasets (adjust version numbers if needed)
PHYSIONET_PROJECT = "physionet-data"
MIMIC_HOSP_DATASET = "mimiciv_3_1_hosp" # Or the specific version you need
MIMIC_ICU_DATASET = "mimiciv_3_1_icu"     # Or the specific version you need
MIMIC_DERIVED_DATASET = "mimic_derived" # Example, adjust version if needed

# 1. IMPORTANT: Use YOUR Google Cloud project ID for billing and authentication.
#    Ensure this project has the BigQuery API enabled.
#    The service account/user running this code needs these IAM roles:
#    - On YOUR project: 'BigQuery User', 'BigQuery Job User'
#    - On physionet-data project: 'BigQuery Data Viewer' (or viewer on specific datasets)
BILLING_PROJECT_ID = "level-strategy-383218" # <<<--- REPLACE WITH YOUR BILLING PROJECT ID

# 2. Initialize BigQuery client specifying YOUR project for billing/quota
try:
    # The client needs your project ID to know where to bill jobs and manage quotas,
    # even when querying public datasets like physionet-data.
    bq_client = bigquery.Client(project=BILLING_PROJECT_ID)
    # Test the connection by trying to get client project details (doesn't require external permissions)
    print(f"BigQuery client initialized successfully. Billing project: {BILLING_PROJECT_ID}")
    # You could add an optional check for dataset access here if desired:
    # try:
    #     bq_client.get_dataset(f"{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}")
    #     print(f"Successfully accessed dataset: {PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}")
    # except Exception as e:
    #     print(f"Warning: Could not directly confirm access to {PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}. Ensure permissions are set. Error: {e}")

except Exception as e:
    print(f"Error initializing BigQuery client for project {BILLING_PROJECT_ID}: {e}")
    print("Please check:")
    print("1. Google Cloud SDK Authentication (`gcloud auth application-default login` or service account key)")
    print(f"2. The BILLING_PROJECT_ID ('{BILLING_PROJECT_ID}') is correct.")
    print(f"3. Necessary IAM roles are granted on project '{BILLING_PROJECT_ID}' and '{PHYSIONET_PROJECT}'.")
    print("4. The BigQuery API is enabled in your Google Cloud project.")
    bq_client = None # Indicate failure

# --- Initialize FastMCP server ---
# Give the server a descriptive name
mcp = FastMCP("mimic_dynamic_query_server")

# --- Helper Function for BigQuery ---

async def execute_bq_query(sql_query: str, query_params: list = None) -> pd.DataFrame | str:
    """
    Executes a BigQuery query using the global client and returns a DataFrame or error string.

    Args:
        sql_query: The SQL query string to execute.
        query_params: Optional list of bigquery.ScalarQueryParameter or similar for parameterized queries.

    Returns:
        A pandas DataFrame with the query results, or a string containing an error message.
    """
    if not bq_client:
        return "Error: BigQuery client is not initialized. Check configuration and authentication."
    try:
        # Configure the job. Use parameters if provided.
        job_config = bigquery.QueryJobConfig(query_parameters=query_params) if query_params else bigquery.QueryJobConfig()

        # Log the query being executed
        print(f"Executing BigQuery Query:\n---\n{sql_query}\n---")

        # Execute the query, ensuring it runs under the billing project context
        # This tells Google Cloud which project to bill for this query.
        query_job = bq_client.query(sql_query, job_config=job_config, project=BILLING_PROJECT_ID)

        # Wait for the job to complete and fetch results into a pandas DataFrame
        # This requires the 'db-dtypes' package for full compatibility
        results_df = query_job.to_dataframe()
        print(f"Query successful. Fetched {len(results_df)} rows.")
        return results_df

    except Exception as e:
        # Attempt to provide a detailed error message
        error_message = f"Error executing BigQuery query: {str(e)}"
        print(error_message)
        # Extract more specific BQ error details if available (e.g., syntax errors)
        if hasattr(e, 'errors') and e.errors:
            try:
                error_detail = e.errors[0].get('message', 'No specific error message found.')
                reason_detail = e.errors[0].get('reason', 'No specific reason found.')
                location_detail = e.errors[0].get('location', 'No specific location found.')
                error_message += (f"\nBigQuery Error Details:\n  Reason: {reason_detail}\n"
                                  f"  Location: {location_detail}\n  Message: {error_detail}")
            except Exception as inner_e:
                error_message += f"\n(Could not parse detailed BigQuery error: {inner_e})"
        # Check if the error message specifically mentions db-dtypes
        if 'db-dtypes' in str(e):
            error_message += "\n\nHint: Try installing the required package: pip install db-dtypes"
        return error_message

# --- Tool Definitions ---

@mcp.tool()
async def execute_arbitrary_mimic_query(sql_query: str) -> str:
    """
    Executes an arbitrary SQL query against the MIMIC datasets hosted on BigQuery.

    **IMPORTANT SECURITY WARNING:** This tool allows executing *any* SQL query provided
    by the client. Ensure the service account running this server has *read-only*
    permissions and is restricted *only* to the necessary `physionet-data` datasets
    (`mimiciv_3_1_hosp`, `mimiciv_3_1_icu`, etc.) to prevent unintended data access,
    modification, or excessive costs. Do not expose this server publicly without
    robust authentication and authorization layers.

    The client (e.g., Claude) is responsible for constructing valid SQL queries
    referencing the correct tables within the PhysioNet project.

    Args:
        sql_query: A valid BigQuery SQL query string. The query *must* explicitly reference
                   tables using the full path, e.g.,
                   `physionet-data.mimiciv_3_1_hosp.admissions` or
                   `physionet-data.mimiciv_3_1_icu.chartevents`.

    Returns:
        A string containing the query results (formatted as a string table, max 50 rows)
        or an error message if the query fails or is disallowed.
    """
    # Basic check for potentially harmful keywords (very rudimentary, not foolproof!)
    # Primary security should rely on IAM permissions.
    disallowed_keywords = ["UPDATE ", "DELETE ", "INSERT ", "DROP ", "CREATE ", "ALTER ", "GRANT ", "TRUNCATE "]
    query_upper = sql_query.upper()
    for keyword in disallowed_keywords:
        if keyword in query_upper:
            return f"Error: Query contains disallowed keyword ('{keyword.strip()}'). Only SELECT queries are permitted."

    # Enforce explicit project reference for clarity and safety
    required_project_prefix = f"`{PHYSIONET_PROJECT}`." # Check for backticks too
    required_project_prefix_no_ticks = f"{PHYSIONET_PROJECT}."
    # Use lower() for case-insensitive check
    sql_query_lower = sql_query.lower()
    if required_project_prefix not in sql_query_lower and required_project_prefix_no_ticks not in sql_query_lower:
         return (f"Error: Query must explicitly reference tables within the '{PHYSIONET_PROJECT}' project "
                 f"(e.g., `{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}.patients`).")

    # Execute the query using the helper function
    result = await execute_bq_query(sql_query)

    if isinstance(result, str): # An error message string was returned
        return result
    elif isinstance(result, pd.DataFrame):
        if result.empty:
            return "Query executed successfully, but returned no results."
        else:
            # Format the DataFrame to a string for the client
            # Add max_rows to prevent overly large outputs flooding the client
            try:
                # Consider other formats like to_markdown() or to_json() based on client needs
                return result.to_string(index=False, max_rows=50)
            except Exception as e:
                return f"Error formatting query results: {e}"
    else:
        # Should not happen with current helper function logic
        return "Error: Unexpected return type from query execution."


@mcp.tool()
async def get_mimic_aggregation(
    metric: Literal["average_age", "patient_count"],
    condition: Optional[str] = None # e.g., "sepsis"
) -> str:
    """
    Performs specific, pre-defined aggregation queries on the MIMIC-IV dataset.
    Calculates age at admission by joining patients and admissions tables.
    (Note: execute_arbitrary_mimic_query offers more flexibility).

    Args:
        metric: The aggregation metric ('average_age', 'patient_count').
        condition: The condition to filter on (e.g., 'sepsis'). Required for 'average_age'.

    Returns:
        A string containing the aggregation result or an error message.
    """
    sql_query = ""
    query_params = [] # Using query parameters is safer against SQL injection for structured tools

    # --- Construct the SQL query based on parameters ---
    if metric == "average_age":
        if not condition:
            return "Error: 'condition' parameter is required for 'average_age' metric."

        # Example: Average age for patients with a specific diagnosis (e.g., sepsis)
        # Using ICD codes. NOTE: Ensure these codes accurately represent the condition.
        condition_icd9_pattern = None
        condition_icd10_pattern = None

        # Define condition patterns (expand this section for more conditions)
        condition_lower = condition.lower()
        if condition_lower == "sepsis":
            condition_icd9_pattern = '99591' # Example ICD-9 for Sepsis
            condition_icd10_pattern = 'A41.%' # Example ICD-10 pattern for Sepsis
        # elif condition_lower == "diabetes":
        #     condition_icd9_pattern = '250.%' # Example ICD-9 pattern for Diabetes
        #     # Note: LIKE doesn't support full regex. For E10-E13 use multiple LIKEs or REGEXP_CONTAINS
        #     condition_icd10_pattern = 'E1[0-3].%' # This LIKE pattern might not work as intended in BQ SQL for ranges.
        #     # Better approach for diabetes ICD-10 might be:
        #     # (d.icd_code LIKE 'E10.%' OR d.icd_code LIKE 'E11.%' OR d.icd_code LIKE 'E12.%' OR d.icd_code LIKE 'E13.%')
        else:
             return f"Error: Aggregation for condition '{condition}' not specifically implemented in this simplified tool. Use `execute_arbitrary_mimic_query` for custom conditions."

        # Proceed only if we have at least one pattern
        if condition_icd9_pattern or condition_icd10_pattern:
            # ***** CORRECTED SQL QUERY *****
            sql_query = f"""
            SELECT
                -- Calculate age at admission using anchor_age, anchor_year, and admittime year
                AVG(CAST( (p.anchor_age + EXTRACT(YEAR FROM a.admittime) - p.anchor_year) AS NUMERIC )) as average_admission_age
            FROM
                `{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}.admissions` a
            INNER JOIN
                -- Join with patients table to get anchor age/year
                `{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}.patients` p ON a.subject_id = p.subject_id
            INNER JOIN
                -- Join with diagnoses table to filter by condition
                `{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}.diagnoses_icd` d ON a.hadm_id = d.hadm_id
            WHERE
                -- Filter based on ICD codes provided using parameterized query
                -- Use COALESCE to handle NULL parameters gracefully if needed, though sending '' is preferred
                (d.icd_version = 9 AND d.icd_code LIKE @condition_icd9)
                OR
                (d.icd_version = 10 AND d.icd_code LIKE @condition_icd10)
            """
            # Set parameters, ensuring None becomes an empty string for LIKE comparison
            query_params = [
                bigquery.ScalarQueryParameter("condition_icd9", "STRING", condition_icd9_pattern or ''),
                bigquery.ScalarQueryParameter("condition_icd10", "STRING", condition_icd10_pattern or ''),
            ]
            # If only one pattern exists, we could potentially optimize the WHERE clause,
            # but the current OR structure works correctly even if one pattern is ''.
        else:
             # This case should ideally not be reached if the condition logic above is complete
             return f"Internal Error: No valid ICD patterns generated for condition '{condition}'."

    elif metric == "patient_count":
        if condition:
             # Filtering patient count by condition would require joins similar to 'average_age'
             return f"Error: Filtering by condition '{condition}' not fully implemented in this simplified tool for 'patient_count'. Use `execute_arbitrary_mimic_query`."
        else:
            # Count all unique patients - This part is simple and doesn't need parameters here
            sql_query = f"""
            SELECT COUNT(DISTINCT subject_id) as total_patients
            FROM `{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}.patients`
            """
            query_params = [] # No parameters needed

    else:
        return f"Error: Unsupported metric '{metric}' for this tool."

    # --- Execute the constructed query ---
    if not sql_query:
        # This might happen if metric is valid but condition logic fails unexpectedly
        return "Error: Failed to construct SQL query for the given parameters."

    result = await execute_bq_query(sql_query, query_params)

    # --- Format and return the result ---
    if isinstance(result, str): # Error occurred during execution
        return result
    elif isinstance(result, pd.DataFrame):
        if result.empty:
            # Provide more context in the message
            condition_str = f" with condition '{condition}'" if condition else ""
            return f"Query executed successfully, but returned no results for metric '{metric}'{condition_str}."
        # Format result DataFrame to string
        try:
            return result.to_string(index=False)
        except Exception as e:
            return f"Error formatting results for metric '{metric}': {e}"
    else:
        # Should not happen with current helper function logic
        return "Error: Unexpected return type from query execution."


# --- Placeholder Tool for ML Prediction ---
# Define a Pydantic model for complex prediction parameters for clarity
class PatientProfile(BaseModel):
    age: int
    gender: str # e.g., "F", "M"
    race: Optional[str] = None # Using MIMIC categories
    vital_signs: Optional[Dict[str, Any]] = Field(default_factory=dict) # e.g., {"heart_rate": 90, "sbp": 110}
    lab_results: Optional[Dict[str, Any]] = Field(default_factory=dict) # e.g., {"creatinine": 1.2, "wbc": 11.5}
    # Add more fields as required by the ML model

@mcp.tool()
async def predict_icd_code(
    patient_profile: Dict[str, Any] # Use Dict for flexibility with MCP, validate internally if needed
) -> str:
    """
    Placeholder for predicting ICD code based on patient data.
    Requires a trained ML model and likely extensive feature engineering via BQ queries.
    (This tool does NOT directly execute arbitrary queries based on the profile).

    Args:
        patient_profile: A dictionary containing patient demographic, vital, and lab data.

    Returns:
        A placeholder string indicating receipt of the request.
    """
    print(f"Received prediction request for profile: {patient_profile}")
    # --- Placeholder Logic ---
    # In a real implementation:
    # 1. Validate patient_profile structure (e.g., using the PatientProfile Pydantic model).
    # 2. Construct specific, targeted BigQuery queries based on the profile to fetch relevant features
    #    (e.g., average heart rate in the first 24h, latest creatinine). Use execute_bq_query.
    # 3. Preprocess the fetched data.
    # 4. Load a pre-trained ML model (e.g., scikit-learn, TensorFlow).
    # 5. Make a prediction.
    # 6. Format and return the prediction.
    return (
        "Prediction endpoint received request. "
        "Actual prediction requires a trained ML model and a feature engineering pipeline. "
        f"Received profile keys: {list(patient_profile.keys())}"
    )


# --- Main Execution Block ---
if __name__ == "__main__":
    # Check if BigQuery client initialized correctly before starting the server
    if not bq_client:
         print("\n--- CRITICAL ERROR: BigQuery Client Initialization FAILED ---")
         print("The server cannot execute BigQuery queries. Please check the error messages above.")
         print("Ensure configuration, authentication, IAM roles, and API enablement are correct.")
         print("Exiting.")
         exit(1) # Exit if BQ client is not available
    else:
        # Display server configuration and available tools on startup
        print("\n--- Server Configuration ---")
        print(f"Billing Project ID: {BILLING_PROJECT_ID}")
        print(f"Querying Project: {PHYSIONET_PROJECT}")
        print(f"Target MIMIC Hosp Dataset: {MIMIC_HOSP_DATASET}")
        print(f"Target MIMIC ICU Dataset: {MIMIC_ICU_DATASET}")
        print("\n--- Available Tools ---")
        print("1. execute_arbitrary_mimic_query(sql_query: str)")
        print("   - Executes any valid BigQuery SQL SELECT query against MIMIC.")
        print("   - **SECURITY WARNING**: Use with extreme caution. Ensure strict, read-only IAM permissions.")
        print(f"   - Client must provide full table paths (e.g., `{PHYSIONET_PROJECT}.{MIMIC_HOSP_DATASET}.patients`).")
        print("2. get_mimic_aggregation(metric: Literal['average_age', 'patient_count'], condition: Optional[str])")
        print("   - Performs pre-defined aggregations (e.g., average admission age for sepsis).")
        print("3. predict_icd_code(patient_profile: Dict[str, Any])")
        print("   - Placeholder for ML predictions based on patient profile.")

        # Start the FastMCP server
        print("\nStarting FastMCP server for MIMIC queries...")
        print("Server ready to accept connections (e.g., via stdio). Send JSON RPC requests.")
        try:
            # Run the server using stdio transport by default
            # Other transports like 'websocket' might be configured if needed
            mcp.run(transport='stdio')
        except KeyboardInterrupt:
            print("\nServer stopped by user.")
        except Exception as e:
            print(f"\nAn error occurred while running the server: {e}")

