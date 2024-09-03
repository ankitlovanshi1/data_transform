from db_connection import create_connection
from mysql.connector import Error

def fetch_table_data():
    # Establish the database connection
    connection = create_connection()

    if connection:
        cursor = connection.cursor()
        try:
            # Execute the query to fetch data from the table_for_archival
            cursor.execute("SELECT * FROM table_for_archival")
            rows = cursor.fetchall()
            
            print("Data fetched successfully:")
            for row in rows:
                print(row)  # Print each row fetched from the database
        except Error as e:
            print(f"The error '{e}' occurred")
        finally:
            cursor.close()
            connection.close()  # Ensure the connection is closed after fetching data

if __name__ == "__main__":
    fetch_table_data()  # Call the function to fetch and display data
