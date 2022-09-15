import pandas as pd
import logging
from airflow.hooks.S3_hook import S3Hook

#Task 3
def load(filename: str, key: str, bucket_name: str) -> None:
    try:
        hook = S3Hook('s3_conn')
        hook.load_file(filename=filename, key=key, bucket_name=bucket_name)
        logging.info('Datos cargado con éxito')
    except:
        logging.error('Error de carga de datos a S3')

if __name__ == '__main__':
    load()