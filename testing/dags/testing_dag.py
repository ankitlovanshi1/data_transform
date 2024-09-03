from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from datetime import datetime, timedelta
import pandas as pd

def create_connection():
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default', schema='testing')
    connection = mysql_hook.get_conn()
    return connection   

def call_stored_procedure(**kwargs):
    connection = kwargs['ti'].xcom_pull(task_ids='create_connection')
    cursor = connection.cursor()
    try:
        cursor.callproc('check_status')
        for result in cursor.stored_results():
            return result.fetchall()
    except Exception as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def fetch_table_for_archival(**kwargs):
    connection = kwargs['ti'].xcom_pull(task_ids='create_connection')
    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM table_for_archival")
            tables = cursor.fetchall()
            for table in tables:
                if table[2] == 'archival':
                    fetch_archival_data(connection, cursor, table[1])
        except Exception as e:
            print(f"The error '{e}' occurred")
        finally:
            cursor.close()
            connection.close()

def fetch_archival_data(connection, cursor, table):
    cursor.close()
    connection.close()
    connection = create_connection()
    cursor = connection.cursor()
    try:
        query = f"SELECT * FROM `{table}`"
        cursor.execute(query)
        data = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(data, columns=columns)
        parquet_file = f"/path/to/save/{table}.parquet"  # Specify your desired path here
        df.to_parquet(parquet_file, engine='pyarrow', index=False)

        cursor.close()
        connection.close()
        connection = create_connection()
        cursor = connection.cursor()

        update_query = f"UPDATE table_for_archival SET status = 'completed' WHERE table_name='{table}'"
        cursor.execute(update_query)
        connection.commit()

        truncate_table(connection, table)
    except Exception as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()
        connection.close()

def truncate_table(connection, table):
    try:
        cursor = connection.cursor()
        cursor.callproc('truncate_table', [table])
        connection.commit()
    except Exception as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mysql_archival_dag', 
    default_args=default_args, 
    schedule_interval='* * * * *', 
    catchup=False) as dag:

    # start_task = DummyOperator(
    #     task_id='start_task',
    #     dag=dag
    # )

    create_connection_task = PythonOperator(
        task_id='create_connection',
        python_callable=create_connection
    )

    call_stored_procedure_task = PythonOperator(
        task_id='call_stored_procedure',
        python_callable=call_stored_procedure,
        provide_context=True,
    )

    fetch_table_for_archival_task = PythonOperator(
        task_id='fetch_table_for_archival',
        python_callable=fetch_table_for_archival,
        provide_context=True,
    )

    # end_task = DummyOperator(
    #     task_id='end_task',
    #     dag=dag
    # )

    create_connection_task >> call_stored_procedure_task >> fetch_table_for_archival_task
