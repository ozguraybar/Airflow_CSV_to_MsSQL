import pandas as pd
import pyodbc
from datetime import timedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def csvToSql():
    data = pd.read_csv(r'CSV_FilePath',encoding= 'unicode_escape',sep=';')
    df = pd.DataFrame(data)

#Connect SQL Server
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=ServerName;'
                      'Database=DatabaseName;'
                      'Trusted_Connection=yes;')
    cursor = conn.cursor()

    #cursor.execute('Truncate table kisibilgileri')

    for row in df.itertuples():
        cursor.execute('''
                INSERT INTO kisibilgileri (ad, soyad, yas)
                VALUES (?,?,?)
                ''',
                row.Ad, 
                row.Soyad,
                row.Yas
                )
    conn.commit()
    conn.close()

default_args = {
    'owner': 'airflow',    
    'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(2018, 12, 30),
    'depends_on_past': False,
    'email': ['example@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }

dag = DAG(
    'csvToSqlDAG',
    default_args=default_args,
    description='Data transfer from CSV to SQL',
    # Continue to run DAG once per day
    schedule_interval=timedelta(days=1),
)

t1 = PythonOperator(
    task_id='islemyap',
    python_callable= csvToSql,
    op_kwargs = {"x" : "Apache Airflow"},
    dag=dag,
)

t1
