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
    "  user_id INTEGER PRIMARY KEY,"
    "  adventurerClass INTEGER,"
    "  level INTEGER,"
    "  curExp INTEGER,"
    "  soul VARCHAR(25),"
    "  elo INTEGER,"
    "  voteScore INTEGER,"
    "  classMastery VARCHAR(25),"
    "  curMana INTEGER,"
    "  manualManaActivated BOOLEAN,"
    "  element VARCHAR(25),"
    "  weapon_id VARCHAR(25),"
    "  skillPoints INTEGER,"
    "  item_id VARCHAR(25)"
    ");"
)

player_permanent_stat_data_schema = (
    "CREATE TABLE IF NOT EXISTS player_permanent_stat_data ("
    "  user_id INTEGER PRIMARY KEY,"
    "  vitality FLOAT,"
    "  strength FLOAT,"
    "  agility FLOAT,"
    "  intelligence FLOAT,"
    "  resistance FLOAT,"
    "  luck FLOAT"
    ");"
)

player_item_data_schema = (
    "CREATE TABLE IF NOT EXISTS player_item_data ("
    "  user_id INTEGER PRIMARY KEY,"
    "  item_id INTEGER REFERENCES player_item_stat(item_id),"
    "  name VARCHAR(25),"
    "  origin VARCHAR(45),"
    "  element VARCHAR(45),"
    "  specialAttribute FLOAT,"
    "  evolution FLOAT,"
    "  evolutionMax FLOAT"
    ");"
)
    
player_item_stat_schema = (
    "CREATE TABLE IF NOT EXISTS player_item_stat ("
    "  item_id INTEGER PRIMARY KEY,"
    "  vitality FLOAT,"
    "  strength FLOAT,"
    "  agility FLOAT,"
    "  intelligence FLOAT,"
    "  resistance FLOAT,"
    "  luck FLOAT,"
    "  baseDefense FLOAT,"
    "  upgradeDefense FLOAT"
    ");"
)


player_weapon_stat_schema = (
    "CREATE TABLE IF NOT EXISTS player_weapon_stat ("
    "  weapon_id INTEGER PRIMARY KEY,"
    "  vitality FLOAT,"
    "  strength FLOAT,"
    "  agility FLOAT,"
    "  intelligence FLOAT,"
    "  resistance FLOAT,"
    "  luck FLOAT,"
    "  baseDamage FLOAT,"
    "  curExp FLOAT,"
    "  maxExp FLOAT,"
    "  foodTarget VARCHAR(15),"
    "  category FLOAT"
    ");"
)

player_weapon_data_schema = (
    "CREATE TABLE IF NOT EXISTS player_weapon_data ("
    "  user_id INTEGER PRIMARY KEY,"
    "  weapon_id INTEGER REFERENCES player_weapon_stat(weapon_id),"
    "  name VARCHAR(25),"
    "  element VARCHAR(25),"
    "  specialAttribute VARCHAR(25),"
    "  evolution INTEGER,"
    "  evolutionMax INTEGER"
    ");"
)

player_pet_data_schema = (
    "CREATE TABLE IF NOT EXISTS player_pet_data ("
    "  user_id INTEGER PRIMARY KEY,"
    "  name VARCHAR(25),"
    "  origin VARCHAR(45),"
    "  element FLOAT,"
    "  curHealth FLOAT,"
    "  foodTarget VARCHAR(30),"
    "  baseHealth FLOAT,"
    "  fedCount FLOAT,"
    "  equipped BOOLEAN,"
    "  elementResistancePercent FLOAT,"
    "  fedCountToday FLOAT,"
    "  bonusType FLOAT,"
    "  bonusTypePercent FLOAT"
    ");"
)

create_table_schema(user_data_schema)
create_table_schema(player_data_schema)
create_table_schema(player_permanent_stat_data_schema)
create_table_schema(player_item_data_schema)
create_table_schema(player_item_stat_schema)
create_table_schema(player_weapon_stat_schema)
create_table_schema(player_weapon_data_schema)
create_table_schema(player_pet_data_schema)

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
        

