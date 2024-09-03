import mysql.connector
from mysql.connector import Error

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to MySQL DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print("Query executed successfully")
    except Error as e:
        print(f"The error '{e}' occurred")

def execute_read_query(connection, query):
    cursor = connection.cursor()
    result = None
    try:
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")
    
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

# Replace these with your actual MySQL database credentials
host_name = "localhost"
user_name = "root"
user_password = "root"
db_name = "testing"

connection = create_connection(host_name, user_name, user_password, db_name)

# Call the stored procedure and fetch results
status_results = call_stored_procedure(connection)

for record in status_results:
    print(record)