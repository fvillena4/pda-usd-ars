from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import redshift_connector
import pandas as pd
import requests
import json
import logging
import os  

from dotenv import load_dotenv
load_dotenv()

# Credenciales para Redshift obtenidas desde variables de entorno
REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
REDSHIFT_PORT = int(os.getenv('REDSHIFT_PORT', 5439))
REDSHIFT_DB = os.getenv('REDSHIFT_DB')
REDSHIFT_USER = os.getenv('REDSHIFT_USER')
REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')

# Nombres del esquema y la tabla
SCHEMA_NAME = os.getenv('SCHEMA_NAME', '2024_facundo_villena_schema')  
TABLE_NAME = os.getenv('TABLE_NAME', 'cotizaciones_dolares')  

# Conexión con Redshift
def get_redshift_connection():
    """
    Establece una conexión a la base de datos de Amazon Redshift.

    Returns:
        conn: Conexión a la base de datos Redshift.
    """
    conn = redshift_connector.connect(
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASSWORD
    )
    return conn

def table_exists(conn, table_name):
    """
    Verifica si una tabla existe en el esquema de Redshift especificado.

    Args:
        conn: Conexión a Redshift.
        table_name (str): Nombre de la tabla a verificar.

    Returns:
        bool: True si la tabla existe, False sino.
    """
    query = f"""
    SELECT COUNT(*)
    FROM information_schema.tables
    WHERE table_schema = '{SCHEMA_NAME}'
    AND table_name = '{table_name}';
    """
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return result[0] > 0

def get_last_date(conn):
    """
    Obtiene la última fecha en la tabla.

    Args:
        conn: Conexión a Redshift.

    Returns:
        datetime or None: La última fecha registrada o None si no hay registros.
    """
    full_table_name = f'"{SCHEMA_NAME}"."{TABLE_NAME}"'  
    query = f"SELECT MAX(updated_at) FROM {full_table_name};"
    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchone()
    return pd.to_datetime(result[0]) if result[0] else None

# Mapeo de casa para type_id
TYPE_ID_MAPPING = {
    'oficial': 0,
    'mayorista': 1,
    'contadoconliqui': 2,
    'bolsa': 3,
    'tarjeta': 4,
    'solidario': 5,
    'cripto': 6,
    'blue': 7
}

def extract_data(**kwargs):
    """
    Extrae los datos de la API de cotizaciones
    Args:
        kwargs: Argumentos proporcionados por Airflow
    Raises:
        Exception: Si la API falla.
    """
    conn = get_redshift_connection()
    
    # Verificar si la tabla existe
    if table_exists(conn, TABLE_NAME):
        # La tabla existe, hacer carga incremental
        last_date = get_last_date(conn)
        logging.info(f"Última fecha registrada: {last_date}")
    else:
        # La tabla no existe, carga completa
        last_date = None
        logging.info("La tabla no existe, se hará una carga completa.")

    api_url = "https://api.argentinadatos.com/v1/cotizaciones/dolares"
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)

        # Asegurarse de que 'fecha' sea de tipo datetime
        df['fecha'] = pd.to_datetime(df['fecha'])

        # Si tenemos una última fecha, traer solo los datos nuevos
        if last_date:
            df = df[df['fecha'] > last_date]

        df['fecha'] = df['fecha'].astype(str)
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_dict())
        logging.info("Datos extraídos y almacenados en XCom.")
    else:
        raise Exception(f"Error en la conexión a la API. Código de estado: {response.status_code}")

def transform_data(**kwargs):
    """
    Transforma los datos extraídos, incluyendo renombrado de columnas y cálculos.

    Args:
        kwargs: Argumentos proporcionados por Airflow
    """
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(key='raw_data', task_ids='extract_data')
    df = pd.DataFrame.from_dict(raw_data)

    # Renombra las columnas
    df.rename(columns={
        'casa': 'type_desc',
        'compra': 'buy_price',
        'venta': 'sell_price',
        'fecha': 'updated_at'
    }, inplace=True)

    # Calcula el promedio entre compra y venta
    df['avg_price'] = df[['buy_price', 'sell_price']].mean(axis=1)

    # Crea una columna 'type_id' usando el mapeo
    df['type_id'] = df['type_desc'].map(TYPE_ID_MAPPING)

    # Asegurarse de que 'updated_at' sea un datetime
    df['updated_at'] = pd.to_datetime(df['updated_at'])
    df['updated_at'] = df['updated_at'].astype(str)

    ti.xcom_push(key='transformed_data', value=df.to_dict())
    logging.info("Datos transformados y almacenados en XCom.")

def load_data(**kwargs):
    """
    Carga los datos transformados en  Redshift.

    Args:
        kwargs: Argumentos de Airflow

    Raises:
        Exception: Si ocurre un error durante la carga de los datos.
    """
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
    df = pd.DataFrame.from_dict(transformed_data)
    conn = get_redshift_connection()
    full_table_name = f'"{SCHEMA_NAME}"."{TABLE_NAME}"'  

    if df.empty:
        logging.info("No hay datos nuevos.")
        return

    try:
        cursor = conn.cursor()

        if not table_exists(conn, TABLE_NAME):
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {full_table_name} (
                type_id INT,
                type_desc VARCHAR(50),
                buy_price FLOAT,
                sell_price FLOAT,
                avg_price FLOAT,
                updated_at TIMESTAMP
            );
            """
            cursor.execute(create_table_query)
            logging.info(f"Tabla '{full_table_name}' creada.")

        logging.info(f"Iniciando la carga de datos en la tabla '{full_table_name}'.")

        for _, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {full_table_name} (type_id, type_desc, buy_price, sell_price, avg_price, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s);
            """
            cursor.execute(insert_query, (
                row['type_id'],
                row['type_desc'],
                row['buy_price'],
                row['sell_price'],
                row['avg_price'],
                row['updated_at']
            ))

        conn.commit()
        logging.info(f"Datos cargados correctamente en la tabla '{full_table_name}'.")

    except Exception as e:
        logging.error(f"Error al cargar los datos: {e}")
        raise 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 15),
    'retries': 1,
}

dag = DAG(
    'redshift_etl_dag',
    default_args=default_args,
    description='ETL DAG para extraer, transformar y cargar datos en Redshift',
    schedule_interval='@daily',
    catchup=False
)

run_tests_task = BashOperator(
    task_id='run_tests',
    bash_command='PYTHONPATH=/opt/airflow/dags pytest /opt/airflow/tests/test_etl.py',
    dag=dag
)

extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag
)

run_tests_task >> extract_task >> transform_task >> load_task
