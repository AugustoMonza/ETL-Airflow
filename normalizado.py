import pandas as pd
import logging
from decouple import config

#Task 2
def transform():
    try:
        #read CSV files
        tabla_jujuy = pd.read_csv('universidad_jujuy.csv', index_col=0)
        tabla_palermo = pd.read_csv('universidad_palermo.csv', index_col=0)
        codigos_postales = pd.read_csv('codigos_postales.csv')

        #rename columns
        tabla_jujuy.rename(columns={'sexo':'gender',
                                        'nombre': 'first_name'}, inplace=True)
        tabla_palermo.rename(columns={'universidad':'university',
                                        'careers': 'career',
                                        'fecha_de_inscripcion': 'inscription_date',
                                        'names': 'first_name',
                                        'sexo': 'gender',
                                        'codigo_postal': 'postal_code',
                                        'correos_electronicos': 'email'}, inplace=True)
        codigos_postales.rename(columns={'codigo_postal': 'postal_code',
                                            'localidad': 'location'}, inplace=True)

        #Obtendo las columnas first_name y last_name
        tabla_jujuy['last_name'] = tabla_jujuy['first_name'].str.split(' ').str.get(1)
        tabla_jujuy['first_name'] = tabla_jujuy['first_name'].str.split(' ').str.get(0)
        tabla_palermo['first_name'] = tabla_palermo['first_name'].str.replace('dr.', '').str.replace('mrs', '').str.replace('mr', '').str.replace('_', ' ').str.strip()
        tabla_palermo['last_name'] = tabla_palermo['first_name'].str.split(' ').str.get(1)
        tabla_palermo['first_name'] = tabla_palermo['first_name'].str.split(' ').str.get(0)

        #Normalizo columna 'age'
        tabla_jujuy['age'] = tabla_jujuy['age'].str.split(' ').str.get(0)
        tabla_palermo['age'] = tabla_palermo['age'].str.split(' ').str.get(0)
        tabla_jujuy = tabla_jujuy.astype({'age': 'float32'})
        tabla_palermo = tabla_palermo.astype({'age': 'float32'})
        tabla_jujuy['age'] = tabla_jujuy['age']/365
        tabla_palermo['age'] = tabla_palermo['age']/365
        tabla_jujuy = tabla_jujuy.astype({'age': 'int'})
        tabla_palermo = tabla_palermo.astype({'age': 'int'})

        #Unión con datos faltantes
        codigos_postales['location'] = codigos_postales['location'].str.lower()
        jujuy = pd.merge(tabla_jujuy, codigos_postales, how='left', on='location')
        palermo = pd.merge(tabla_palermo, codigos_postales, how='left', on='postal_code')

        #Normalizado de tablas
        columns = ['university', 'career', 'first_name', 'last_name', 'location', 'email']

        for column in columns:
            jujuy[f'{column}'] = jujuy[f'{column}'].str.strip().str.lower().str.replace('_', ' ')
            palermo[f'{column}'] = palermo[f'{column}'].str.strip().str.lower().str.replace('_', ' ')

        #Elimino filas duplicadas
        jujuy = jujuy.drop_duplicates(['email', 'career'], keep='first')
        palermo = palermo.drop_duplicates(['email', 'career'], keep='first')

        #Tablas finales to_txt
        jujuy.to_csv('universidad_de_jujuy.txt')
        palermo.to_csv('universidad_de_palermo.txt')
        logging.info('Normalizado de datos con éxito')
    except:
        logging.error('No se realizó el normalizado de las tablas')


if __name__ == '__main__':
    transform()
