import mysql.connector
from mysql.connector import errorcode


def create_table_schema(table_schema):
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            user='root',
            password='Orobou',
            database='game'
        )

        cursor = conn.cursor()

        cursor.execute(table_schema)

        conn.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Access denied. Check your username and password.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Database does not exist.")
        else:
            print(f"Error: {err}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

user_data_schema = (
    "CREATE TABLE IF NOT EXISTS user_data ("
    "  username VARCHAR(25) PRIMARY KEY,"
    "  userID INTEGER,"
    "  gold INTEGER,"
    "  colorSkin INTEGER "
    ");"
)
player_data_schema = (
    "CREATE TABLE IF NOT EXISTS player_data ("
    "  user_id INTEGER PRIMARY KEY"
)
    

create_table_schema(user_data_schema)
create_table_schema(player_data_schema)

def ingest_data_from_dataframe(dataframe, table_name):
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            user='root',
            password='Orobou',
            database='game',
        )

        cursor = conn.cursor()

        placeholders = ', '.join(['%s'] * len(dataframe.columns))

        insert_query = f"INSERT IGNORE INTO {table_name}({', '.join(dataframe.columns)}) VALUES ({placeholders})"

        for _, row in dataframe.iterrows():
            values = tuple(row.values)
            cursor.execute(insert_query, values)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        if 'conn' in locals():
            conn.rollback()
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()
        

