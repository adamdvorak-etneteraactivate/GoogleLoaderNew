import pyodbc
import json
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import datetime
import decimal
# Import Google API specific exceptions
from google.api_core.exceptions import GoogleAPIError, NotFound 
import logging
import azure.functions as func
# Import Azure SDK Clients
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient

# Initialize Azure Function App instance
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- Helper Function for JSON Serialization ---
def json_serial(obj):
    """Serialize datetime and decimal objects to JSON compatible formats."""
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# --- Core Pipeline Logic Function (Parameters are now raw values, resolved in the main function) ---
def run_full_load_pipeline(sql_id, sql_pwd, sql_server, sql_db, sql_schema, sql_table, gcs_bucket_name, bq_table_id_short, max_rows=None, replace_table=1, credentials=None, gcs_client=None, bq_client=None):
    logging.info(f"Spouštím FULL pipeline z SQL do BQ: {bq_table_id_short}")

    # Credentials (clients) are passed in, so we skip auth in this sub-function

    full_bq_table_id = f"{credentials.project_id}.{bq_table_id_short}"

    # --- MODIFIED: Handle Auto-Creation and Schema Reading ---
    expected_bq_column_names = None
    autodetect_schema = True # Default to True (for creation/autodetect)

    try:
        table = bq_client.get_table(full_bq_table_id)
        expected_bq_column_names = [field.name for field in table.schema]
        logging.info(f"Očekávané pořadí sloupců v BQ: {expected_bq_column_names}")
        autodetect_schema = False # If table found, we use the explicit schema/order
    except NotFound:
        logging.warning(f"BQ Table {full_bq_table_id} not found. Proceeding with schema auto-detection for load job.")
    except Exception as e:
        logging.error(f"Chyba při čtení schématu BQ tabulky: {e}"); return False

    # --- 2. Připojení k Azure SQL DB a získání dat using pyodbc ---
    rows_unsorted = []
    conn = None 
    try:
        # NOTE: sql_pwd is now the actual password string
        odbc_driver = os.environ.get("SQL_ODBC_DRIVER_NAME", "ODBC Driver 17 for SQL Server") 
        conn_string = (
            f"DRIVER={{{odbc_driver}}};"
            f"SERVER={sql_server};"
            f"DATABASE={sql_db};"
            f"UID={sql_id};"
            f"PWD={sql_pwd};" # Use the actual password string here
            f"Encrypt=yes;"
            f"TrustServerCertificate=no;"
        )
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        if max_rows and int(max_rows) > 0: sql_query = f"SELECT TOP {int(max_rows)} * FROM {sql_schema}.{sql_table}"
        else: sql_query = f"SELECT * FROM {sql_schema}.{sql_table}"
        cursor.execute(sql_query)
        
        columns = [desc[0] for desc in cursor.description]
        
        for row_tuple in cursor.fetchall():
            row_dict = dict(zip(columns, row_tuple))
            rows_unsorted.append(row_dict)
            
        cursor.close()
        conn.close()
        logging.info(f"Načteno {len(rows_unsorted)} řádků ze SQL DB.")
    except Exception as e: 
        logging.error(f"Chyba pri cteni ze SQL DB (pyodbc error): {e}"); 
        if conn: conn.close()
        return False
    
    # --- 3. Nahrání dat do GCS ---
    temp_blob_name = f"temp_staging/{sql_schema}_{sql_table}_{os.urandom(8).hex()}.json"
    try:
        bucket = gcs_client.bucket(gcs_bucket_name)
        blob = bucket.blob(temp_blob_name)
        with blob.open("w", content_type="application/json") as f:
            for row_unsorted in rows_unsorted:
                if expected_bq_column_names:
                    row_sorted = {col: row_unsorted[col] for col in expected_bq_column_names if col in row_unsorted}
                else:
                    row_sorted = row_unsorted
                f.write(json.dumps(row_sorted, default=json_serial) + "\n")
        logging.info(f"Data uložena do GCS: gs://{gcs_bucket_name}/{temp_blob_name} in proper order (or unsorted for autodetect).")
    except Exception as e: logging.error(f"Chyba pri zapisu do GCS: {e}"); return False
    
    # --- 4. Spuštění BQ Load Jobu z GCS ---
    full_bq_table_id = f"{credentials.project_id}.{bq_table_id_short}"
    try:
        if replace_table == 1: write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else: write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        
        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, autodetect=autodetect_schema, write_disposition=write_disposition,)
        
        load_job = bq_client.load_table_from_uri(f"gs://{gcs_bucket_name}/{temp_blob_name}", full_bq_table_id, job_config=job_config)
        load_job.result()
        logging.info(f"BQ Load Job {load_job.job_id} dokončen. Načteno {load_job.output_rows} řádků.")

        # --- 5. Smazání dočasného souboru ---
        blob.delete()
        logging.info(f"SUCCESS: Full load dokončen a dočasný soubor smazán. Řádků: {load_job.output_rows}.")
        return True
    except GoogleAPIError as e:
        logging.error(f"Chyba pri BQ load jobu: {e.message}"); 
        try: blob.delete(); logging.info(f"Dočasný soubor {temp_blob_name} smazán po chybě BQ jobu.")
        except: pass
        return False
    except Exception as e: 
        logging.error(f"Neočekavana chyba: {e}"); return False

@app.route(route="RunFullLoadPipeline", auth_level=func.AuthLevel.FUNCTION)
def HttpTriggerRunFullLoadPipeline(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request to run the data pipeline.')
    
    # Initialize connection objects
    credentials, gcs_client, bq_client = None, None, None

    try:
        req_body = req.get_json()
        
        # We extract secret NAMES here, not raw values
        key_vault_name = req_body["key_vault_name"]
        gcp_secret_name = req_body["gcp_secret_name"]
        sql_pwd_secret_name = req_body["sql_pwd_secret_name"] # New parameter name

        sql_id = req_body["sql_id"]
        # sql_pwd is now a secret name, we fetch the value below
        sql_server = req_body["sql_server"]
        sql_db = req_body["sql_db"]
        sql_schema = req_body["sql_schema"]
        sql_table = req_body["sql_table"]
        gcs_bucket = req_body["gcs_bucket"]
        bq_table_id_short = req_body["bq_dataset_table"]
        max_rows = req_body.get("max_rows", None)
        replace_table = int(req_body.get("replace_table", 1)) 
    except (ValueError, KeyError) as e:
        return func.HttpResponse(f"Bad Request: Missing or invalid parameter: {e}", status_code=400)

    # --- Fetch all secrets dynamically at the start ---
    try:
        credential = DefaultAzureCredential()
        vault_url = f"https://{key_vault_name}.vault.azure.net/"
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        # Fetch GCP key
        gcp_key_secret = secret_client.get_secret(gcp_secret_name)
        key_json_str = gcp_key_secret.value
        credentials_info = json.loads(key_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        gcs_client = storage.Client(credentials=credentials)
        bq_client = bigquery.Client(credentials=credentials)
        logging.info("GCP Clients initialized using KV secret.")
        
        # Fetch SQL password
        sql_pwd_secret = secret_client.get_secret(sql_pwd_secret_name)
        actual_sql_pwd = sql_pwd_secret.value
        logging.info("SQL password fetched from KV.")

    except Exception as e:
        logging.error(f"Key Vault Access or Auth Failed: {e}")
        return func.HttpResponse(f"Internal Server Error: Key Vault access/Auth Failed. {e}", status_code=500)


    # Pass the resolved credentials and the actual password string to the core pipeline function
    success = run_full_load_pipeline(
        sql_id, actual_sql_pwd, sql_server, sql_db, sql_schema, sql_table, 
        gcs_bucket, bq_table_id_short, max_rows, replace_table,
        credentials=credentials,
        gcs_client=gcs_client,
        bq_client=bq_client
    )

    if success:
        return func.HttpResponse(f"Data pipeline executed successfully for {bq_table_id_short}.", status_code=200)
    else:
        return func.HttpResponse("Data pipeline failed. Check Azure Function logs for details.", status_code=500)
