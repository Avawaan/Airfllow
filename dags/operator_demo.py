import os
from datetime import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

# ──────────────────────────────────────────────────────────────────────────────
# Azure ADLS Gen2 / Blob Storage connection via SAS Token
# Airflow 3.0 compatible: connection defined via environment variable
#
# Set this env var in your docker-compose.yml / .env file:
#
#   AIRFLOW_CONN_AZURE_DATA_LAKE=wasb://:<SAS_TOKEN>@<ACCOUNT_NAME>.blob.core.windows.net
#
# Or in docker-compose.yml under environment:
#   - AIRFLOW_CONN_AZURE_DATA_LAKE=wasb://:<SAS_TOKEN>@<ACCOUNT_NAME>.blob.core.windows.net
# ──────────────────────────────────────────────────────────────────────────────

AZURE_STORAGE_ACCOUNT_NAME = "snowflakeloadpractice"   # 👈 change this
AZURE_SAS_TOKEN             = "sv=2025-11-05&ss=bfqt&srt=sco&sp=rwdlacupiytfx&se=2026-04-17T01:32:07Z&st=2026-04-16T17:17:07Z&spr=https&sig=gCEE7IKj9XpjCqIAmbrNtJNESEm%2F5q2QucyfI1A6VlQ%3D"         # 👈 change this (starts with "?sv=...")
AIRFLOW_CONN_ID             = "azure_data_lake"

# Register the connection via environment variable (Airflow 3.0 compatible)
# Format: wasb://:<sas_token>@<account>.blob.core.windows.net
os.environ[f"AIRFLOW_CONN_{AIRFLOW_CONN_ID.upper()}"] = (
    f"wasb://:{AZURE_SAS_TOKEN}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net"
)


def upload_to_adls(
    local_file_path: str,
    container_name: str,
    blob_name: str,
    wasb_conn_id: str = AIRFLOW_CONN_ID,
):
    """
    Uploads a local file to Azure Data Lake Storage Gen2.

    Args:
        local_file_path: Full path to the local file to upload.
        container_name:  ADLS Gen2 filesystem / container name.
        blob_name:       Destination path inside the container.
        wasb_conn_id:    Airflow connection ID for Azure Blob/ADLS Gen2.
    """
    hook = WasbHook(wasb_conn_id=wasb_conn_id)
    hook.load_file(
        file_path=local_file_path,
        container_name=container_name,
        blob_name=blob_name,
        overwrite=True,
    )
    print(f"✅ Uploaded '{local_file_path}' → adls://{container_name}/{blob_name}")


@dag(
    dag_id="bash_operator_demo",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["bashdemo"],
)
def bash_operator_demo():

    list_dag_files = BashOperator(
        task_id="list_dag_folder_files",
        bash_command="echo 'Files in DAGS Folder' && ls -lh /opt/airflow/dags",
    )

    upload_file_to_adls = PythonOperator(
        task_id="upload_file_to_adls_gen2",
        python_callable=upload_to_adls,
        op_kwargs={
            "local_file_path": "/opt/airflow/dags/flight-time.csv",   # 👈 change this
            "container_name": "demo",                 # 👈 change this
            "blob_name": "flight-time.csv",                      # 👈 change this
            "wasb_conn_id": AIRFLOW_CONN_ID,
        },
    )

    list_dag_files >> upload_file_to_adls


bash_operator_demo()