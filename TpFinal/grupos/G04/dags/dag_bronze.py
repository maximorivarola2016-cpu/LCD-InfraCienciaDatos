from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
from datetime import datetime, timedelta
import requests
import json
import os

# Definición del Asset de Airflow para Bronze (Ítem 5)
bronze_asset = Dataset("postgres://data_warehouse/bronze/clima_raw")

default_args = {
    'owner': 'G04',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extraer_clima_bronze(**kwargs):
    # Aquí irá la lógica de extracción de tu compañero (Integrante 1)
    print("Iniciando extracción de Open-Meteo para las 5 ciudades...")
    # Ejemplo básico de estructura de conexión a postgres usando las variables del .env
    # Se conectará a data_warehouse mediante la red docker compartida de_stack_network
    print("Datos guardados exitosamente en bronze.clima_raw")

with DAG(
    'g04_bronze_clima',
    default_args=default_args,
    description='DAG de la capa Bronze para extracción de clima',
    schedule='@hourly',  # Ejecución periódica (Ítem 6)
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['g04', 'bronze', 'open-meteo'],
) as dag:

    # Tarea principal de extracción
    task_extraer = PythonOperator(
        task_id='extraer_y_guardar_bronze',
        python_callable=extraer_clima_bronze,
        outlets=[bronze_asset], # Dispara el Asset para encadenar con Silver (Ítem 5 y 7)
    )

    task_extraer
