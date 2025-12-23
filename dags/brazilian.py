from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
import pandas as pd
from sqlalchemy import create_engine, text
import os

local_tz = pendulum.timezone("Asia/Bangkok")

default_args = {
    "owner": "Brazilian-team",
    "email": ["bi@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2, 
    "retry_delay": timedelta(minutes=5),
}

def ingest_csv_to_postgres():
    """
    Reads all specified CSV files and ingests them into respective tables
    in the 'staging' schema of a PostgreSQL database.
    """
    data_path = "/opt/airflow/dags/data/"
    
    csv_files = [
        "olist_customers_dataset.csv",
        "olist_geolocation_dataset.csv",
        "olist_order_items_dataset.csv",
        "olist_order_payments_dataset.csv",
        "olist_order_reviews_dataset.csv",
        "olist_orders_dataset.csv",
        "olist_products_dataset.csv",
        "olist_sellers_dataset.csv",
        "product_category_name_translation.csv"
    ]

    engine = create_engine("postgresql+psycopg2://dpuser:dppass@dp_postgres:5432/brazilian_ecommerce")

    try:
        # สร้าง schema 'staging' หากยังไม่มี
        with engine.connect() as connection:
            connection.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
        print("Schema 'staging' is ready.")

        # วนลูปเพื่อนำเข้าข้อมูล
        for file_name in csv_files:
            full_path = os.path.join(data_path, file_name)
            try:
                print(f"Processing file: {full_path}...")
                
                if not os.path.exists(full_path):
                    print(f"ERROR: File not found at {full_path}")
                    continue

                df = pd.read_csv(full_path)
                table_name = os.path.splitext(file_name)[0]
                
                df.to_sql(
                    name=table_name,
                    con=engine,
                    schema="staging",
                    if_exists="replace",
                    index=False
                )
                
                print(f"Successfully ingested {file_name} into staging.{table_name}")
                
            except Exception as e:
                print(f"An error occurred while processing {file_name}: {e}")

    except Exception as e:
        print(f"Could not connect to the database or create the schema: {e}")
        
with DAG(
    dag_id="brazilian_pipeline",
    start_date=pendulum.datetime(2025, 9, 1, tz=local_tz),
    schedule_interval="0 4 * * *",
    catchup=False,
    default_args=default_args,
    tags=["brazilian", "daily work"],
) as dag:

    # 1) Ingestion (Strict)
    ingest_csv = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv_to_postgres,
    )
    validate_staging = BashOperator(
        task_id="validate_staging",
        bash_command=(
            "cd /workspace/gx_brazilian && "
            "great_expectations checkpoint run brazilian_ecommerce_staging_checkpoint"
        ),
    )

    # 2) Core (Strict)
    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command="cd /usr/app/core && dbt run --select core",
    )
    validate_core = BashOperator(
        task_id="validate_core",
        bash_command="cd /usr/app/core && dbt test --select core",
    )

    # 3) Data Marts (Soft)
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /usr/app/mart && dbt run --select marts",
    )
    strict_validate_marts = BashOperator(
        task_id="strict_validate_marts",
        bash_command="cd /usr/app/mart && dbt test --select marts",
    )  # strict check

    soft_validate_mart_sales = BashOperator(
        task_id="soft_validate_mart_sales",
        bash_command=(
            "cd /workspace/gx_brazilian && "
            "great_expectations checkpoint run mart_sales_checkpoint"
        ),
    )  # soft check
    
    soft_validate_mart_customer = BashOperator(
        task_id="soft_validate_mart_customers",
        bash_command=(
            "cd /workspace/gx_brazilian && "
            "great_expectations checkpoint run mart_customer_checkpoint" # ✅ แก้ไข: ชื่อ checkpoint
        ),
    )  # soft check
    
    # ✅ แก้ไข: เปลี่ยนชื่อตัวแปรไม่ให้ซ้ำกัน
    soft_validate_mart_product = BashOperator( 
        task_id="soft_validate_mart_products",
        bash_command=(
            "cd /workspace/gx_brazilian && "
            "great_expectations checkpoint run mart_product_checkpoint" # ✅ แก้ไข: ชื่อ checkpoint
        ),
    )  # soft check

    # 4) Publish (รอแค่ strict layer ผ่าน)
    publish = EmailOperator(
        task_id="publish",
        to="bi@example.com",
        subject="✅ Brazilian Pipeline - Data Marts Ready", # ✅ แก้ไข: ชื่อ Pipeline
        html_content="""
        <h3>✅ Data Marts Build Completed</h3>
        <p>Your marts are now ready in PostgreSQL
        and can be accessed from Metabase.</p>
        
        <p>
        - fct_sales<br>
        - fct_customers<br>
        - fct_product
        </p>
        
        <p>Timestamp: {{ ds }}</p>
        """,
    )

    # DAG dependencies
    # Strict flow: ต้องผ่าน staging → validate_staging → core → validate_core
    ingest_csv >> validate_staging >> dbt_run_core >> validate_core

    # สร้าง mart ได้ก็ต่อเมื่อ core ผ่าน
    (
        validate_core
        >> dbt_run_marts
        >> strict_validate_marts
        # ✅ แก้ไข: เพิ่ม product เข้าไปใน dependency
        >> [soft_validate_mart_customer, soft_validate_mart_sales, soft_validate_mart_product]
    )

    # Publish รอแค่ build mart เสร็จ (ไม่รอ soft_validate_marts)
    strict_validate_marts >> publish
