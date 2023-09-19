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
    "  username VARCHAR(25),"
    "  user_id INTEGER PRIMARY KEY,"
    "  gold INTEGER,"
    "  colorSkin INTEGER "
    ");"
)

player_data_schema = (
    "CREATE TABLE IF NOT EXISTS player_data ("
    "  user_id INTEGER PRIMARY KEY,"
    "  adventurerClass FLOAT,"
    "  level FLOAT,"
    "  curExp FLOAT,"
    "  elo FLOAT,"
    "  voteScore FLOAT,"
    "  curMana FLOAT,"
    "  manualManaActivated BOOLEAN,"
    "  element FLOAT,"
    "  weapon_id INTEGER REFERENCES player_weapon_stat(weapon_id),"
    "  skillPoints FLOAT,"
    "  item_id INTEGER REFERENCES player_item_stat(item_id)"
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
    "  element FLOAT,"
    "  specialAttribute FLOAT,"
    "  evolution FLOAT,"
    "  evolutionMax FLOAT"
    ");"
)
    
player_item_stat_schema = (
    "CREATE TABLE IF NOT EXISTS player_item_stat ("
    "  user_id INTEGER PRIMARY KEY,"
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
    "  user_id INTEGER PRIMARY KEY,"
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
    "  origin VARCHAR(25),"
    "  element FLOAT,"
    "  specialAttribute FLOAT,"
    "  evolution FLOAT,"
    "  evolutionMax FLOAT"
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

def ingest_data_from_dataframe(dataframe, table_name, primary_key_column):
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

        insert_query = f"INSERT INTO {table_name} ({', '.join(dataframe.columns)}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {primary_key_column} = VALUES({primary_key_column})"

        for _, row in dataframe.iterrows():
            # Convert 'userID' to int if it's not empty, otherwise, set it to None
            user_id = int(row['user_id']) if row['user_id'] is not None and row['user_id'] != '' else None
            values = tuple([user_id] + list(row.values)[1:]) 
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
        
def ingest_data_permanent_stat(dataframe):
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            user='root',
            password='Orobou',
            database='game',
        )

        cursor = conn.cursor()

        insert_query = """
            INSERT INTO player_permanent_stat_data (user_id, vitality, strength, agility, intelligence, resistance, luck)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            vitality = VALUES(vitality),
            strength = VALUES(strength),
            agility = VALUES(agility),
            intelligence = VALUES(intelligence),
            resistance = VALUES(resistance),
            luck = VALUES(luck)
        """

        for row in dataframe.itertuples():
            values = row[1:]  # Exclude the index
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


def ingest_item_stat(dataframe):
    try:
        conn = mysql.connector.connect(
            host='127.0.0.1',
            port='3306',
            user='root',
            password='Orobou',
            database='game',
        )

        cursor = conn.cursor()

        insert_query = """
            INSERT INTO player_item_stat (user_id, vitality, strength, agility, intelligence, resistance, luck, baseDefense, upgradeDefense)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            vitality = VALUES(vitality),
            strength = VALUES(strength),
            agility = VALUES(agility),
            intelligence = VALUES(intelligence),
            resistance = VALUES(resistance),
            luck = VALUES(luck),
            baseDefense = VALUES(baseDefense),
            upgradeDefense = VALUES(upgradeDefense)
        """

        for row in dataframe.itertuples():
            values = row[1:]  # Exclude the index
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
