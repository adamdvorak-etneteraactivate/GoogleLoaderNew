import pyodbc
import json
from google.cloud import storage
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import datetime
import decimal
from google.api_core.exceptions import GoogleAPIError, NotFound 
import logging
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
import threading
import requests

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def run_full_load_pipeline(sql_id, sql_pwd, sql_server, sql_db, sql_schema, sql_table, gcs_bucket_name, bq_table_id_short, max_rows=None, replace_table=1, credentials=None, gcs_client=None, bq_client=None):
    logging.info(f"Spouštím STREAMING pipeline: {bq_table_id_short}")
    
    full_bq_table_id = f"{credentials.project_id}.{bq_table_id_short}"
    conn = None 

    try:
        # --- 1. SQL PŘIPOJENÍ ---
        odbc_driver = os.environ.get("SQL_ODBC_DRIVER_NAME", "ODBC Driver 18 for SQL Server") 
        conn_string = (
            f"DRIVER={{{odbc_driver}}};SERVER={sql_server};DATABASE={sql_db};"
            f"UID={sql_id};PWD={sql_pwd};Encrypt=yes;TrustServerCertificate=no;LoginTimeout=60;" 
        )
        conn = pyodbc.connect(conn_string)
        cursor = conn.cursor()
        
        top_clause = f"TOP {int(max_rows)} " if max_rows and int(max_rows) > 0 else ""
        sql_query = f"SELECT {top_clause}* FROM {sql_schema}.{sql_table}"
        
        cursor.execute(sql_query)
        sql_column_names = [column[0] for column in cursor.description]

        # --- 2. PŘÍPRAVA BIGQUERY SCHÉMATU (Před zápisem do GCS) ---
        try:
            table = bq_client.get_table(full_bq_table_id)
            expected_bq_column_names = [field.name for field in table.schema]
            bq_schema_fields = None
        except NotFound:
            bq_schema_fields = [
                bigquery.SchemaField(name=name, field_type='STRING', mode='NULLABLE') 
                for name in sql_column_names
            ]
            expected_bq_column_names = [field.name for field in bq_schema_fields]

        # --- 3. STREAMOVANÝ ZÁPIS DO GCS (Ošetření paměti) ---
        temp_blob_name = f"temp_staging/{sql_schema}_{sql_table}_{os.urandom(8).hex()}.json"
        bucket = gcs_client.bucket(gcs_bucket_name)
        blob = bucket.blob(temp_blob_name)
        
        row_count = 0
        # Otevíráme stream do GCS. Data se posílají průběžně, neukládají se v RAM.
        with blob.open("w", content_type="application/json") as f:
            for row_tuple in cursor:  # Iterujeme přímo přes kurzor (řádek po řádku)
                row_dict = dict(zip(sql_column_names, row_tuple))
                # Seřazení klíčů dle schématu
                row_sorted = {col: row_dict.get(col) for col in expected_bq_column_names}
                f.write(json.dumps(row_sorted, default=json_serial) + "\n")
                
                row_count += 1
                if row_count % 50000 == 0:
                    logging.info(f"Zpracováno {row_count} řádků...")

        cursor.close()
        conn.close()
        logging.info(f"Celkem uloženo {row_count} řádků do GCS.")

        # --- 4. BQ LOAD JOB (Zůstává stejný) ---
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE if replace_table == 1 else bigquery.WriteDisposition.WRITE_APPEND
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON, 
            schema=bq_schema_fields, 
            autodetect=True if bq_schema_fields is None else False,
            write_disposition=write_disposition,
        )
        
        load_job = bq_client.load_table_from_uri(f"gs://{gcs_bucket_name}/{temp_blob_name}", full_bq_table_id, job_config=job_config)
        load_job.result() 
        
        logging.info(f"BQ Load Job hotov. Načteno {load_job.output_rows} řádků.")
        blob.delete() 
        return True

    except Exception as e:
        if conn: conn.close()
        raise e

# --- Zbytek kódu (send_callback, background_worker, route) zůstává stejný ---
def send_callback(uri, status_code, output_data):
    try:
        payload = {"StatusCode": status_code, "Output": output_data}
        requests.post(uri, json=payload, headers={'Content-Type': 'application/json'}, timeout=60)
    except Exception as e:
        logging.error(f"Callback error: {e}")

def background_worker(callback_uri, req_body):
    try:
        credential = DefaultAzureCredential()
        vault_url = f"https://{req_body['key_vault_name']}.vault.azure.net/"
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        key_json_str = secret_client.get_secret(req_body['gcp_secret_name']).value
        actual_sql_pwd = secret_client.get_secret(req_body['sql_pwd_secret_name']).value

        credentials_info = json.loads(key_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        
        run_full_load_pipeline(
            req_body["sql_id"], actual_sql_pwd, req_body["sql_server"], 
            req_body["sql_db"], req_body["sql_schema"], req_body["sql_table"], 
            req_body["gcs_bucket"], req_body["bq_dataset_table"], 
            max_rows=req_body.get("max_rows"), 
            replace_table=int(req_body.get("replace_table", 1)),
            credentials=credentials, 
            gcs_client=storage.Client(credentials=credentials), 
            bq_client=bigquery.Client(credentials=credentials)
        )
        send_callback(callback_uri, 200, {"status": "Success"})
    except Exception as e:
        logging.error(f"Worker failed: {str(e)}")
        send_callback(callback_uri, 500, {"status": "Error", "detail": str(e)})

@app.route(route="RunFullLoadPipeline", auth_level=func.AuthLevel.ANONYMOUS)
def HttpTriggerRunFullLoadPipeline(req: func.HttpRequest) -> func.HttpResponse:
    try:
        req_body = req.get_json()
        callback_uri = req_body.get('callBackUri')
        if not callback_uri: return func.HttpResponse("Missing callBackUri", status_code=400)

        thread = threading.Thread(target=background_worker, args=(callback_uri, req_body), daemon=True)
        thread.start()

        return func.HttpResponse(json.dumps({"accepted": True}), status_code=202, mimetype="application/json")
    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)
