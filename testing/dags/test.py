import pandas as pd
import mysql.connector
from mysql.connector import Error

def create_connection():
    connection = None
    try:
        connection = mysql.connector.connect(
            host='localhost',
            port=3306,
            user='root',
            passwd='root',
            database='testing'
        )
        if connection.is_connected():
            print("Connection to MySQL DB successful")
            return connection
    except Error as e:
        print(f"The error '{e}' occurred")

# check status
def call_stored_procedure(connection):
    cursor = connection.cursor()
    try:
            cursor.callproc('check_status')
            print("check status")
            for result in cursor.stored_results():
                return result.fetchall()
        
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

def fetch_table_for_archival(connection):

    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM table_for_archival")
            tables = cursor.fetchall()
            
            print("check status for archival status")
            for table in tables:
                if table[2] == 'archival':
                    fetch_archival_data(connection, cursor, table[1])
        except Error as e:
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
        # Execute the query to fetch data from the specified table
        query = f"SELECT * FROM `{table}`"  # Use backticks to handle table names with special characters
        cursor.execute(query)
        data = cursor.fetchall()

        print("save parquet file")
        columns = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the f
        df = pd.DataFrame(data, columns=columns)
        
        # Save the DataFrame to a Parquet file
        parquet_file = f"{table}.parquet"
        df.to_parquet(parquet_file, engine='pyarrow', index=False)
        
        # update status
        cursor.close()
        connection.close()
        connection = create_connection()
        cursor = connection.cursor()

        print("update status")
        update_query = f"update table_for_archival set status = 'completed' where table_name='{table}'"
        cursor.execute(update_query)
        connection.commit()
        # truancate table 
        truncate_table(table)
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()
        connection.close()  # Ensure the connection is closed after fetching data

def truncate_table(table):
    try:
        connection = create_connection()
        cursor = connection.cursor()

        print("Truncate table")
        # Call the stored procedure to truncate the table
        cursor.callproc('truncate_table', [table])
        
        # Commit the changes to the database
        connection.commit()
    except Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    connection = create_connection()
    status_results = call_stored_procedure(connection)
    fetch_table_for_archival(connection)

for record in status_results:
    print(record)