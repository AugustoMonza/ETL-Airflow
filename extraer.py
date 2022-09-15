import pandas as pd
import logging
from decouple import config
from sqlalchemy import create_engine

#Task 1
def extract():
    try:
        querys = {'jujuy': 'OT282-15-tabla jujuy.sql',
                  'palermo': 'OT282-15-tabla palermo.sql'
                  }
        user_name = config('USER_NAME')
        password = config('PASSWORD')
        host = config('HOST')
        server_ = config('PORT')
        database = config('DB_NAME')
        engine = create_engine(f'postgresql+psycopg2://{user_name}:{password}@{host}:{server_}/{database}')

        for keys,values in querys.items():
            with open (values, encoding='utf-8') as query:
                query_ = query.read()
            universidad = pd.read_sql_query(query_, engine)
            name = f'universidad_{keys}.csv'
            universidad.to_csv(name)
        logging.info('Lectura y descarga de tablas realizado')
    except:
        logging.error('Error de conexión')

if __name__ == '__main__':
    extract()