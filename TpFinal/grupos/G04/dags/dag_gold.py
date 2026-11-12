from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta

# Escucha el Asset de Silver para dispararse automáticamente (Ítem 5 y 7)
silver_asset = Dataset("postgres://data_warehouse/silver/clima_limpio")

default_args = {
    'owner': 'G04',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def modelar_a_gold(**kwargs):
    # Aquí irá la lógica del modelo estrella de tu compañero (Integrante 3)
    print("Iniciando carga del Modelo Estrella (Gold)...")
    print("Datos y dimensiones cargados exitosamente en gold.fact_clima_diario")

with DAG(
    'g04_gold_clima',
    default_args=default_args,
    description='DAG de la capa Gold para el modelo estrella',
    schedule=[silver_asset],  # Se dispara automáticamente cuando Silver termina (Ítem 7)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['g04', 'gold', 'modelo-estrella'],
) as dag:

    task_modelar = PythonOperator(
        task_id='generar_modelo_estrella_gold',
        python_callable=modelar_a_gold,
    )

    task_modelar
