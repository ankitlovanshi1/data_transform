from db_connection import create_connection
from mysql.connector import Error

def call_stored_procedure(connection):
    cursor = connection.cursor()
    try:
        # Call the stored procedure
        cursor.callproc('check_status')
        
        # Fetch results from the stored procedure
        for result in cursor.stored_results():
            return result.fetchall()
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

connection = create_connection()

if connection:
    status_results = call_stored_procedure(connection)
else:
    print(f"The error '{e}' occurred")

for record in status_results:
    print(record)