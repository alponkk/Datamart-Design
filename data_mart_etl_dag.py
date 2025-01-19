from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from sqlalchemy import create_engine, text
import pandas as pd

# Konfigurasi koneksi database
SOURCE_DB_URI = Variable.get("REC_DEV_MYSQL_CONN")
TARGET_DB_URI = Variable.get("PG_CONN_STRING")

# Query Sumber
SOURCE_QUERY = """
SELECT 
	e.id AS entity_id,
	e.name AS entity_name,
	et.title AS entity_tag,
	p.name AS province,
	r.name AS regency,
	mm.id AS material_id,
	mm.name AS material_name,	
	mm.is_vaccine AS vaccine_status,
	mm.is_stockcount AS stockcount_status,
	mm.is_addremove AS addremove_status,
	mm.is_openvial AS openvial_status,
	ma.id AS activity_id,
	ma.name AS activity_name,
	s.updatedAt AS date,
	s.qty AS on_hand_stock
FROM 
    stocks s
JOIN 
    entity_has_master_materials ehmm ON s.material_entity_id = ehmm.id
JOIN 
    entities e ON ehmm.entity_id = e.id
LEFT JOIN 
    provinces p ON e.province_id = p.id 
LEFT JOIN 
    regencies r ON e.regency_id = r.id
LEFT JOIN 
    entity_entity_tags eet ON e.id = eet.entity_id 
JOIN 
	entity_tags et ON eet.entity_tag_id = et.id 
JOIN 
    master_materials mm ON ehmm.master_material_id = mm.id
JOIN 
    master_activities ma ON s.activity_id = ma.id;
"""

# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    "data_mart_etl_dag",
    default_args=default_args,
    description="ETL DAG for loading data mart",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    def extract_data(**kwargs):
        print("""Extract data from the source database.""")
        source_engine = create_engine(SOURCE_DB_URI)
        with source_engine.connect() as conn:
            df = pd.read_sql(SOURCE_QUERY, conn)
        
        # Konversi kolom datetime/Timestamp ke string
        for col in df.select_dtypes(include=["datetime", "datetime64[ns]"]).columns:
            df[col] = df[col].astype(str)

        # Konversi NaN ke None agar kompatibel dengan JSON
        df = df.where(pd.notnull(df), None)
        
        kwargs["ti"].xcom_push(key="extracted_data", value=df.to_dict(orient="records"))

    def transform_data(**kwargs):
        """Transform the extracted data."""
        data = kwargs["ti"].xcom_pull(key="extracted_data", task_ids="extract_data")
        df = pd.DataFrame(data)

        # Mapping ke tabel dimensi dan fakta
        dim_entity = df[["entity_id", "entity_name", "entity_tag", "province", "regency"]].drop_duplicates()
        dim_material = df[
            ["material_id", "material_name", "vaccine_status", "stockcount_status", "addremove_status", "openvial_status"]
        ].drop_duplicates()
        dim_activity = df[["activity_id", "activity_name"]].drop_duplicates()

        fact_stock = df[["entity_id", "material_id", "activity_id", "date", "on_hand_stock"]]

        transformed_data = {
            "dim_entity": dim_entity.to_dict(orient="records"),
            "dim_material": dim_material.to_dict(orient="records"),
            "dim_activity": dim_activity.to_dict(orient="records"),
            "fact_stock": fact_stock.to_dict(orient="records"),
        }
        kwargs["ti"].xcom_push(key="transformed_data", value=transformed_data)

    def load_data(**kwargs):
        """Load transformed data into the target database."""
        target_engine = create_engine(TARGET_DB_URI)
        data = kwargs["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")

        with target_engine.connect() as conn:
            # Load dimension tables
            for row in data["dim_entity"]:
                conn.execute(text("""
                    INSERT INTO data_mart_design.dim_entities (entity_id, entity_name, entity_tag, province, regency)
                    VALUES (:entity_id, :entity_name, :entity_tag, :province, :regency)
                    ON CONFLICT (entity_id) DO NOTHING;
                """), row)

            for row in data["dim_material"]:
                conn.execute(text("""
                    INSERT INTO data_mart_design.dim_materials (material_id, material_name, vaccine_status, stockcount_status, addremove_status, openvial_status)
                    VALUES (:material_id, :material_name, :vaccine_status, :stockcount_status, :addremove_status, :openvial_status)
                    ON CONFLICT (material_id) DO NOTHING;
                """), row)

            for row in data["dim_activity"]:
                conn.execute(text("""
                    INSERT INTO data_mart_design.dim_activities (activity_id, activity_name)
                    VALUES (:activity_id, :activity_name)
                    ON CONFLICT (activity_id) DO NOTHING;
                """), row)

            # Load fact table
            for row in data["fact_stock"]:
                conn.execute(text("""
                    INSERT INTO data_mart_design.fact_stocks (entity_id, material_id, activity_id, date, on_hand_stock)
                    VALUES (:entity_id, :material_id, :activity_id, :date, :on_hand_stock)
                    ON CONFLICT DO NOTHING;
                """), row)

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
        provide_context=True,
    )

    extract_task >> transform_task >> load_task
