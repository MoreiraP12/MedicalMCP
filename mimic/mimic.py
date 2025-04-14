import os
from typing import Any, Dict, Literal, Optional
from google.cloud import bigquery
import pandas as pd
from pydantic import BaseModel, Field # Using Pydantic for structured parameters

# Assuming FastMCP is installed and available
# pip install fastmcp
from mcp.server.fastmcp import FastMCP

# --- Configuration ---
MIMIC_HOSP_DATASET = "mimiciv_3_1_hosp"
MIMIC_ICU_DATASET = "mimiciv_3_1_icu" # Not used in current example, but potentially useful
MIMIC_DERIVED_DATASET = "mimic_derived" # Not used, but potentially useful

# 1. Use a single project ID - your own Google Cloud project
PROJECT_ID = "level-strategy-383218"  # Your project ID


# 2. Initialize only one BigQuery client with your project
try:
    bq_client = bigquery.Client(project=PROJECT_ID)
    print(f"Connected to BigQuery project: {PROJECT_ID}")
except Exception as e:
    print(f"Error connecting to BigQuery: {e}")
    print("Falling back to local data source.")
    USE_BIGQUERY = False
    bq_client = None

# --- Initialize FastMCP server ---
# Give the server a name relevant to its function
mcp = FastMCP("mimic_query_server")

# --- Helper Function for BigQuery ---

async def execute_bq_query(sql_query: str, query_params: list = None) -> pd.DataFrame | str:
    """Executes a BigQuery query and returns a DataFrame or error string."""
    if not bq_client:
        return "BigQuery client not initialized."
    try:
        job_config = bigquery.QueryJobConfig(query_parameters=query_params) if query_params else None
        print(f"Executing BigQuery Query:\n{sql_query}")
        query_job = bq_client.query(sql_query, job_config=job_config)
        results_df = query_job.to_dataframe() # Waits for job completion
        return results_df
    except Exception as e:
        print(f"BigQuery query failed: {e}")
        return f"Error executing BigQuery query: {str(e)}"

# --- Tool Definitions ---

@mcp.tool()
async def get_mimic_aggregation(
    metric: Literal["average_age", "patient_count"],
    condition: Optional[str] = None # e.g., "sepsis"
    # Add more parameters as needed: time_range, demographics etc.
) -> str:
    """
    Performs aggregation queries on the MIMIC-IV dataset.

    Args:
        metric: The aggregation metric to calculate (e.g., 'average_age', 'patient_count').
        condition: The condition to filter on (e.g., 'sepsis'). Required for some metrics.
    """
    sql_query = ""
    query_params = []

    if metric == "average_age":
        if not condition:
            return "Error: 'condition' parameter is required for 'average_age' metric."
        if condition.lower() == "sepsis":
            # Using the same example query as before for average age of sepsis patients
            # NOTE: Ensure ICD codes are accurate and comprehensive for your definition of sepsis.
            sepsis_icd9_pattern = '99591' # Simplified - needs proper list
            sepsis_icd10_pattern = 'A41%'  # Simplified - needs proper list

            sql_query = f"""
            SELECT
                AVG(CAST(a.admission_age AS BIGNUMERIC)) as average_age
            FROM
                `physionet-data.{MIMIC_HOSP_DATASET}.admissions` a
            JOIN
                `physionet-data.{MIMIC_HOSP_DATASET}.patients` p ON a.subject_id = p.subject_id
            JOIN
                `physionet-data.{MIMIC_HOSP_DATASET}.diagnoses_icd` d ON a.hadm_id = d.hadm_id
            WHERE
                (d.icd_version = 9 AND d.icd_code LIKE @sepsis_icd9_pattern)
                OR
                (d.icd_version = 10 AND d.icd_code LIKE @sepsis_icd10_pattern)
            """
            query_params = [
                bigquery.ScalarQueryParameter("sepsis_icd9_pattern", "STRING", sepsis_icd9_pattern),
                bigquery.ScalarQueryParameter("sepsis_icd10_pattern", "STRING", sepsis_icd10_pattern),
            ]
        else:
            return f"Error: Condition '{condition}' not implemented for 'average_age'."

    elif metric == "patient_count":
        # Example: Count all patients (can be refined with conditions)
         sql_query = f"""
         SELECT COUNT(DISTINCT subject_id) as total_patients
         FROM `physionet-data.{MIMIC_HOSP_DATASET}.patients`
         """
         # Add condition logic here if needed
         if condition:
             return f"Error: Filtering by condition '{condition}' not yet implemented for 'patient_count'."

    else:
        return f"Error: Unsupported metric '{metric}'."

    # Execute query
    result = await execute_bq_query(sql_query, query_params)

    if isinstance(result, str): # Error occurred
        return result
    elif isinstance(result, pd.DataFrame):
        if result.empty:
            return "No results found for the specified aggregation."
        # Format result DataFrame to string
        return result.to_string(index=False)
    else:
        return "Unexpected error during query execution."


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
    patient_profile: Dict[str, Any] # Use Dict for flexibility with MCP, or potentially PatientProfile if supported
    # latitude: float, longitude: float # Example args from original code - remove
) -> str:
    """
    Placeholder for predicting ICD code based on patient data.
    Requires a trained ML model.

    Args:
        patient_profile: A dictionary containing patient demographic, vital, and lab data.
                         Example: {"age": 40, "gender": "F", "race": "BLACK/AFRICAN AMERICAN",
                                  "vital_signs": {"heart_rate": 88}, "lab_results": {"wbc": 10.1}}
    """
    print(f"Received prediction request for profile: {patient_profile}")

    # --- Placeholder Logic ---
    # In a real implementation:
    # 1. Validate patient_profile structure (Pydantic can help here if integrated)
    # 2. Perform complex feature engineering using BigQuery based on the profile.
    #    This might involve querying chartevents, labevents, etc., potentially over time windows.
    #    Example conceptual query (needs actual implementation):
    #    "SELECT AVG(valuenum) FROM mimiciv_icu.chartevents WHERE itemid = '220045' AND subject_id = ... AND charttime BETWEEN ..."
    # 3. Load a pre-trained ML model.
    # 4. Pass engineered features to the model.
    # 5. Return the prediction (e.g., predicted ICD code and confidence).

    return (
        "Prediction endpoint received request. "
        "Actual prediction requires a trained ML model and complex feature engineering pipeline using BigQuery. "
        f"Received profile keys: {list(patient_profile.keys())}"
    )


# --- Main Execution Block ---
if __name__ == "__main__":
    if not bq_client:
         print("Exiting: BigQuery client failed to initialize.")
    else:
        print("Starting FastMCP server for MIMIC queries...")
        print("Server ready to accept connections (e.g., via stdio).")
        # Run the server using stdio transport by default
        # Other transports like 'websocket' might be available depending on FastMCP version/setup
        mcp.run(transport='stdio')
