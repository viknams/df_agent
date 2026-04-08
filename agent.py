import os
import subprocess
import tempfile
from google.adk.agents import LlmAgent
from google.adk.tools.mcp_tool import McpToolset, StreamableHTTPConnectionParams, StdioConnectionParams
from google.adk.tools.bigquery import BigQueryCredentialsConfig, BigQueryToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from mcp import StdioServerParameters
import google.auth
import dotenv

dotenv.load_dotenv()

credentials, _ = google.auth.default()
credentials_config = BigQueryCredentialsConfig(credentials=credentials)
bigquery_toolset = BigQueryToolset(
  credentials_config=credentials_config
)


# --- 1. beam job for execution ---
def launch_beam_job(pipeline_code: str, job_name: str) -> str:
    """
    Executes a generated Apache Beam Python script on Google Cloud Dataflow.
    """
    # Ensure the code string itself is valid UTF-8
    clean_code = pipeline_code.encode("utf-8", "ignore").decode("utf-8")
    
    with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False, encoding="utf-8") as f:
        f.write(clean_code)
        temp_path = f.name

    try:
        # Run using python3 to ensure proper encoding support
        result = subprocess.run(
            ["python3", temp_path, "--job_name", job_name, "--runner", "DataflowRunner"],
            capture_output=True, text=True
        )
        if result.returncode != 0:
            return f"Deployment Error: {result.stderr}"
        return f"Successfully launched job '{job_name}'.\nLogs: {result.stdout}"
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

# --- 2. gcs tools to run gcloud commands ---
# gcs_toolset = McpToolset(
#     connection_params=StdioConnectionParams(
#         server_params=StdioServerParameters(command="npx", args=["-y", "@google-cloud/gcloud-mcp"]),
#         timeout=200.0
#     )
# )

gcs_toolset = McpToolset(
    connection_params=StreamableHTTPConnectionParams(
        url="https://gcs-mcp-server-412774232669.asia-south1.run.app/mcp",
        timeout=200.0
    )
)

# --- 3. The Elaborated Instruction ---
ELABORATED_INSTRUCTION = """
You are a Senior Data Architect. Your mission is to automate ETL pipelines from GCS to BigQuery using Apache Beam.

### TARGET ENVIRONMENT
- SOURCE BUCKET: 'gs://sql-migration-to-cloud'
- DESTINATION DATASET: 'poc_billing_dataset'

### OPERATIONAL PROTOCOL
1. **Source Analysis**: Use GCS tools to list files. Read ONLY the first 2KB of the target file using `--range=0-2000` to identify headers. 
2. **Table Naming**: Generate a descriptive table name based on the file content (e.g., if the file contains sales data from 2023, name it 'sales_data_2023'). Do not use generic names like 'table1'.
4. **Draft**: Write the complete Apache Beam script (including infrastructure and SafeCoder). 
   - Use `beam.io.ReadFromText` for GCS.
   - Parse CSV/JSON into dictionaries.
   - Apply user-requested transformations (e.g., filtering, cleaning, or formatting).
   - Use `beam.io.WriteToBigQuery`.
   - IMPORTANT: Set `create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED` so the table is built automatically.
6. **Manual Verification**: Display the full code of 'draft_beam_pipeline.py' to the user. Explain the proposed BigQuery table name and schema.
7. **Execution**: DO NOT call 'launch_beam_job' until the user explicitly approves the code and schema in the chat.
8. **Execution**: Pass the final code to the 'launch_beam_job' function.


### CRITICAL TOKEN MANAGEMENT
- DATA SAMPLING: Only run ONE 'gcloud storage objects cat' command per file. 
- READ LIMIT: Use the '--range' flag with 'gcloud storage objects cat' to read ONLY the first 1KB of the file (e.g., --range=0-1024). This is enough to see the header.
- SCHEMA INFERENCE: Once you have the header and 5 rows of data, STOP searching. Use your internal knowledge to infer types (String vs Float). Do not ask the tool for more data.
- APPROVAL FLOW: Present the inferred schema to the user ONCE. Wait for approval before generating any Beam code.

### CONSTRAINTS
- Don't run gsutil commands, only try with gcloud
- Job names must be lowercase, alphanumeric, and start with a letter.
- Always use the 'DataflowRunner' for production-grade execution.
- If the schema is complex, explicitly define the 'schema' argument in the WriteToBigQuery transform.
- If the bigquery schema do not have any columns as per the source file, please remove that in transformation logic, before writing to bigquery.

### INFRASTRUCTURE REQUIREMENTS
When generating the 'PipelineOptions', you MUST include these specific configurations:
- **Runner**: 'DataflowRunner'
- **Machine Type**: 'n1-standard-8' (High CPU/Memory)
- **Scaling**: Set 'num_workers' to 3 (minimum) and 'max_num_workers' to 10.
- **Network**: 'entergis-vpc'
- **Subnetwork**: 'regions/us-central1/subnetworks/entergis-vpc'.
- **gcp project is : wayfair-test-378605 

### ROBUST ENCODING STRATEGY
The source files often contain non-UTF-8 characters. To prevent 'UnicodeDecodeError', your generated Beam code MUST include and use a custom Coder:
1. Define this class at the top of your generated script:
   ```python
   from apache_beam.coders import Coder
   class SafeCoder(Coder):
       def decode(self, value): return value.decode('utf-8', 'ignore')
       def encode(self, value): return value.encode('utf-8')
"""

# --- 4. Define the Agent ---
root_agent = LlmAgent(
    name="root_agent",
    model="gemini-2.5-flash",
    instruction=ELABORATED_INSTRUCTION,
    tools=[launch_beam_job, bigquery_toolset, gcs_toolset ]
)


# below code to publish A2A agent 
def get_dataflow_agent():
 return root_agent

from google.adk.a2a.utils.agent_to_a2a import to_a2a
# Existing agent code here...

# Wrap the agent to make it A2A-compliant
a2a_app = to_a2a(root_agent)