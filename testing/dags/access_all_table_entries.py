import pandas as pd
from db_connection import create_connection
from mysql.connector import Error
def fetch_table_for_archival(connection):

    if connection:
        cursor = connection.cursor()
        try:
            cursor.execute("SELECT * FROM table_for_archival")
            tables = cursor.fetchall()
            
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

        columns = [desc[0] for desc in cursor.description]

        # Create a DataFrame from the f
        df = pd.DataFrame(data, columns=columns)
        
        # Save the DataFrame to a Parquet file
        parquet_file = f"{table}.parquet"
        df.to_parquet(parquet_file, engine='pyarrow', index=False)
        
        print(f"Data from table {table} saved to {parquet_file}")

        # update status
        cursor.close()
        connection.close()
        connection = create_connection()
        cursor = connection.cursor()

        update_query = f"update table_for_archival set status = 'completed' where table_name='{table}'"
        cursor.execute(update_query)
        connection.commit()
        print("completed status")
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

        # Call the stored procedure to truncate the table
        cursor.callproc('truncate_table', [table])
        
        # Commit the changes to the database
        connection.commit()
        print(f"{table}, truncate")
    except Error as err:
        print(f"Error: {err}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

if __name__ == "__main__":
    connection = create_connection()
    fetch_table_for_archival(connection)  # Call the function to fetch and display data





