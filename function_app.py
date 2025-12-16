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
import time
import threading  # Nutné pro asynchronní běh
import requests   # Nutné pro odeslání callbacku (přidejte do requirements.txt)

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# --- Helper pro JSON serializaci ---
def json_serial(obj):
    if isinstance(obj, (datetime.datetime, datetime.date, datetime.time)):
        return obj.isoformat()
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

# --- Funkce pro odeslání výsledku zpět do ADF ---
def send_callback(uri, status_code, output_data):
    try:
        payload = {
            "StatusCode": status_code,
            "Output": output_data
        }
        requests.post(uri, json=payload, headers={'Content-Type': 'application/json'})
        logging.info(f"Callback odeslán s kódem {status_code}")
    except Exception as e:
        logging.error(f"Selhalo odeslání callbacku: {e}")

# --- Původní run_full_load_pipeline zůstává stejná ---
def run_full_load_pipeline(sql_id, sql_pwd, sql_server, sql_db, sql_schema, sql_table, gcs_bucket_name, bq_table_id_short, max_rows=None, replace_table=1, credentials=None, gcs_client=None, bq_client=None):
    # ... (Zde nechte váš původní kód run_full_load_pipeline beze změn) ...
    # (Předpokládejme, že vrací True/False)
    return True # Zkráceno pro přehlednost

# --- Wrapper pro běh na pozadí ---
def background_worker(callback_uri, req_body):
    try:
        # 1. Načtení parametrů
        key_vault_name = req_body["key_vault_name"]
        gcp_secret_name = req_body["gcp_secret_name"]
        sql_pwd_secret_name = req_body["sql_pwd_secret_name"]
        sql_id = req_body["sql_id"]
        sql_server = req_body["sql_server"]
        sql_db = req_body["sql_db"]
        sql_schema = req_body["sql_schema"]
        sql_table = req_body["sql_table"]
        gcs_bucket = req_body["gcs_bucket"]
        bq_table_id_short = req_body["bq_dataset_table"]
        max_rows = req_body.get("max_rows", None)
        replace_table = int(req_body.get("replace_table", 1))

        # 2. Autentizace a klienti
        credential = DefaultAzureCredential()
        vault_url = f"https://{key_vault_name}.vault.azure.net/"
        secret_client = SecretClient(vault_url=vault_url, credential=credential)

        key_json_str = secret_client.get_secret(gcp_secret_name).value
        actual_sql_pwd = secret_client.get_secret(sql_pwd_secret_name).value

        credentials_info = json.loads(key_json_str)
        credentials = service_account.Credentials.from_service_account_info(credentials_info)
        gcs_client = storage.Client(credentials=credentials)
        bq_client = bigquery.Client(credentials=credentials)

        # 3. Spuštění pipeline
        success = run_full_load_pipeline(
            sql_id, actual_sql_pwd, sql_server, sql_db, sql_schema, sql_table, 
            gcs_bucket, bq_table_id_short, max_rows, replace_table,
            credentials=credentials, gcs_client=gcs_client, bq_client=bq_client
        )

        if success:
            send_callback(callback_uri, 200, {"message": "Pipeline dokončena úspěšně."})
        else:
            send_callback(callback_uri, 500, {"message": "Pipeline selhala uvnitř run_full_load_pipeline."})

    except Exception as e:
        logging.error(f"Chyba v workeru: {e}")
        send_callback(callback_uri, 500, {"error": str(e)})

@app.route(route="RunFullLoadPipeline", auth_level=func.AuthLevel.FUNCTION)
def HttpTriggerRunFullLoadPipeline(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Požadavek na ADF Webhook pipeline.')
    
    try:
        req_body = req.get_json()
        callback_uri = req_body.get('callBackUri') # Získání URL z ADF

        if not callback_uri:
            logging.error("Chybí callBackUri v požadavku!")
            return func.HttpResponse("Tento endpoint vyžaduje Webhook activity z ADF (chybí callBackUri).", status_code=400)

        # SPUŠTĚNÍ VLÁKNA: Funkce běží dál na pozadí
        thread = threading.Thread(target=background_worker, args=(callback_uri, req_body))
        thread.start()

        # OKAMŽITÁ ODPOVĚĎ: ADF uvidí status "InProgress" a spojení se uzavře (žádný 504 timeout)
        return func.HttpResponse(
            json.dumps({"status": "Accepted", "message": "Work started in background"}),
            status_code=202,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Fatální chyba při startu: {e}")
        return func.HttpResponse(f"Chyba: {e}", status_code=500)
