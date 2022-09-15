from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import extraer
import normalizado
import carga
from decouple import config

logging.basicConfig(filename='app.log',
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d',
                    level=logging.DEBUG)



# Definición de DAG
with DAG(
    dag_id = 'universidades_C',
    default_args={
    'depends_on_past': False,
    'email': ['amonza621@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'Hacer un ETL para 2 universidades distintas',
    schedule_interval = '@daily',
    start_date = datetime(2022, 8, 21),
    catchup = False,
) as dag:
    t1 = PythonOperator(
        task_id = 'query_universidad_c',
        python_callable = extraer.extract
        )
    t2 = PythonOperator(
        task_id = 'normalize_data',
        python_callable = normalizado.transform
    )
    t3 = PythonOperator(
        task_id = 'load_data_jujuy',
        python_callable = carga.load,
        op_kwargs={
            
            'filename': 'universidad_de_jujuy.txt',
            'key': 'universidad_de_jujuy.txt',
            'bucket_name': 'cohorte-agosto-38d749a7'
        }
    )
    t4 = PythonOperator(
        task_id = 'load_data_palermo',
        python_callable = carga.load,
        op_kwargs={
            
            'filename': 'universidad_de_palermo.txt',
            'key': 'universidad_de_palermo.txt',
            'bucket_name': 'cohorte-agosto-38d749a7'
        }
    )
    
    t1.doc_md = dedent(
        """\ ### Función
        la tarea realiza una consulta a la tablas de Universidad_C obteniendo las columnas necesarias y procesamiento de datos"""
    )
    
    t2.doc_md = dedent(
        """\ ### Función
        la tarea realiza el procesamiento de datos con pandas"""
    )
    t3.doc_md = dedent(
        """\ ### Función
        la tarea realiza la carga de datos en S3"""
    )
    
# Configuración de dependencia
t1 >> t2 >> t3 >> t4