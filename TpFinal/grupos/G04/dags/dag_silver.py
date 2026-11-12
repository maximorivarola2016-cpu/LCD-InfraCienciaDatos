from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta

# Escucha el Asset de Bronze para dispararse automáticamente (Ítem 5 y 7)
bronze_asset = Dataset("postgres://data_warehouse/bronze/clima_raw")
silver_asset = Dataset("postgres://data_warehouse/silver/clima_limpio")

default_args = {
    'owner': 'G04',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transformar_a_silver(**kwargs):
    # Aquí irá la lógica de transformación de tu compañero (Integrante 2)
    print("Iniciando transformación de datos crudos (Bronze) a limpios (Silver)...")
    print("Datos guardados exitosamente en silver.clima_actual y silver.pronostico_diario")

with DAG(
    'g04_silver_clima',
    default_args=default_args,
    description='DAG de la capa Silver para limpieza y validación',
    schedule=[bronze_asset],  # Se dispara automáticamente cuando Bronze termina (Ítem 7)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['g04', 'silver', 'transformacion'],
) as dag:

    task_transformar = PythonOperator(
        task_id='limpiar_y_guardar_silver',
        python_callable=transformar_a_silver,
        outlets=[silver_asset],  # Dispara el Asset para activar Gold
    )

    task_transformar
