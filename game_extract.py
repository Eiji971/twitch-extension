import pandas as pd 
import json
import import_sql 

filepath = '/Users/emerybosc/Downloads/users2.json'
with open(filepath, "r") as file:
    data = json.load(file)

df_raw_data = pd.DataFrame(data)

df_raw_data = pd.json_normalize(data)

#print(df_raw_data.head())

#print(df_raw_data.columns)

"""
'username', 
'userId',
'gold', 
'colorSkin'


'player.adventurerClass',
'player.level', 
'player.curExp',
'player.soul', 
'player.elo', 
'player.voteScore', 
'player.inventory',
'player.classMastery', 
'player.curMana', 
'player.badges',
'player.manualManaActivated', 
'player.element',
'player.weapon',
'player.skillPoints',
'player.item'

'player.permaStats.vitality',
'player.permaStats.strength', 
'player.permaStats.agility',
'player.permaStats.intelligence', 
'player.permaStats.resistance',
'player.permaStats.luck'


'player.item.name', 
'player.item.origin', 
'player.item.element',
'player.item.specialAttribute', 
'player.item.evolution',
'player.item.evolutionMax', 
'player.item.bonusStats.vitality',
'player.item.bonusStats.strength', 
'player.item.bonusStats.agility',
'player.item.bonusStats.intelligence',
'player.item.bonusStats.resistance', 
'player.item.bonusStats.luck',
'player.item.baseDefense', 
'player.item.upgradeDefense'

'player.weapon.name',
'player.weapon.origin', 
'player.weapon.element',
'player.weapon.specialAttribute', 
'player.weapon.evolution',
'player.weapon.evolutionMax', 
'player.weapon.bonusStats.vitality',
'player.weapon.bonusStats.strength', 
'player.weapon.bonusStats.agility',
'player.weapon.bonusStats.intelligence',
'player.weapon.bonusStats.resistance', 
'player.weapon.bonusStats.luck',
'player.weapon.baseDamage', 
'player.weapon.curExp',
'player.weapon.maxExp', 
'player.weapon.foodTarget',
'player.weapon.category'

'player.pet.name', 
'player.pet.origin',
'player.pet.element', 
'player.pet.curHealth', 
'player.pet.foodTarget',
'player.pet.baseHealth', 
'player.pet.fedCount', 
'player.pet.equipped',
'player.pet.elementResistancePercent', 
'player.pet.fedCountToday',
'player.pet.bonusType', 
'player.pet.bonusTypePercent'

"""
df_raw_data = df_raw_data.drop(['hoursOnlineMonth', 'hoursOnline', 'watchStreak', 'lastMessageSent'], axis=1)

df_general_user_info = df_raw_data[['username', 'userId', 'gold', 'colorSkin']]

print(df_general_user_info.head())

import_sql.ingest_data_from_dataframe(df_general_user_info, 'user_data')

df_player_data = df_raw_data.copy()


df_player_data = df_player_data.drop((['username', 'gold', 'colorSkin']), axis=1)



df_player_data[['userId', 'adventurerClass', 'Level', 'curExp', 'soul', 'elo', 'voteScore', 
                'classMastery', 'curMana',  'manualManaActivated', 'element', 'weapon', 'skillPoints', 'item']] = df_raw_data['player'].apply(
                    lambda x: pd.Series([
                        x['userId'],
                        x['adventurerClass'],
                        x['level'],
                        x['curExp'],
                        x['soul'],
                        x['elo'],
                        x['voteScore'],
                        x['classMastery'],
                        x['curMana'],
                        x['manualManaActivated'],
                        x['element'],
                        x['weapon'],
                        x['skillPoints'],
                        x['item']
                    ]) if isinstance(x, dict) else pd.Series([None, None, None, None, None, None, None, None, None, None, None, None, None, None])
)

import_sql.ingest_data_from_dataframe(df_player_data, 'player_data')

df_player_stat = df_raw_data.copy()



df_player_stat = df_player_stat.drop((['username', 'gold', 'colorSkin']), axis=1)


df_player_stat = df_player_stat[['userId', 'player.permaStats.vitality', 'player.permaStats.strength', 'player.permaStats.agility', 
                                 'player.permaStats.intelligence', 'player.permaStats.resistance', 'player.permaStats.luck']]


df_player_stat = df_player_stat.rename(columns={
    'userId': 'user_id',
    'player.permaStats.vitality': 'vitality', 
    'player.permaStats.strength': 'strength', 
    'player.permaStats.agility': 'agility', 
    'player.permaStats.intelligence': 'intelligence', 
    'player.permaStats.resistance': 'resistance', 
    'player.permaStats.luck': 'luck'
})

print(df_player_stat.head())

import_sql.ingest_data_from_dataframe(df_player_stat, 'player_permanent_stat_data')

df_item_data = df_raw_data.copy()

df_item_data_raw = df_item_data.drop((['username', 'gold', 'colorSkin']), axis=1)

df_item_data = df_item_data_raw[['userId', 'player.item.name', 'player.item.origin', 'player.item.element', 'player.item.specialAttribute', 'player.item.evolution', 'player.item.evolutionMax']]

df_item_data = df_item_data.rename(columns={
    'userId': 'user_id',
    'player.item.name': 'name', 
    'player.item.origin': 'origin', 
    'player.item.element': 'element', 
    'player.item.specialAttribute': 'specialAttribute', 
    'player.item.evolution': 'evolution', 
    'player.item.evolutionMax': 'evolutionMax'
})

print(df_item_data.head())

import_sql.ingest_data_from_dataframe(df_item_data, 'player_item_data')

df_item_stat = df_item_data_raw[['userId', 'player.item.bonusStats.vitality', 'player.item.bonusStats.strength', 'player.item.bonusStats.agility', 'player.item.bonusStats.intelligence', 
                                 'player.item.bonusStats.resistance', 'player.item.bonusStats.luck', 'player.item.baseDefense', 'player.item.upgradeDefense']]

df_item_stat = df_item_stat.rename(columns={
    'userId': 'user_id',
    'player.item.bonusStats.vitality': 'vitality', 
    'player.item.bonusStats.strength': 'strength', 
    'player.item.bonusStats.agility': 'agility', 
    'player.item.bonusStats.intelligence': 'intelligence', 
    'player.item.bonusStats.resistance': 'resistance', 
    'player.item.bonusStats.luck': 'luck',
    'player.item.baseDefense': 'baseDefense',
    'player.item.upgradeDefense': 'upgradeDefense'
})

print(df_item_stat.head())

import_sql.ingest_data_from_dataframe(df_item_stat, 'player_item_stat')

df_weapon_raw = df_raw_data.copy()

df_weapon_data = df_weapon_raw[['userId',
                                'player.weapon.name',
                                'player.weapon.origin', 
                                'player.weapon.element',
                                'player.weapon.specialAttribute', 
                                'player.weapon.evolution',
                                'player.weapon.evolutionMax']]

df_weapon_data = df_weapon_data.rename(columns={
                                'userId': 'user_id',
                                'player.weapon.name':'name',
                                'player.weapon.origin': 'origin',
                                'player.weapon.element': 'element',
                                'player.weapon.specialAttribute': 'attribute',
                                'player.weapon.evolution': 'evolution',
                                'player.weapon.evolutionMax': 'evolutionMax'
})

print(df_weapon_data.head())

import_sql.ingest_data_from_dataframe(df_weapon_data, 'player_weapon_data')

df_weapon_stat = df_weapon_raw[['userId',
                                'player.weapon.bonusStats.vitality',
                                'player.weapon.bonusStats.strength', 
                                'player.weapon.bonusStats.agility',
                                'player.weapon.bonusStats.intelligence',
                                'player.weapon.bonusStats.resistance', 
                                'player.weapon.bonusStats.luck',
                                'player.weapon.baseDamage', 
                                'player.weapon.curExp',
                                'player.weapon.maxExp', 
                                'player.weapon.foodTarget',
                                'player.weapon.category']]


df_weapon_stat = df_weapon_stat.rename(columns={
                                'userId': 'user_id',
                                'player.weapon.bonusStats.vitality': 'vitality',
                                'player.weapon.bonusStats.strength': 'strength', 
                                'player.weapon.bonusStats.agility': 'agility',
                                'player.weapon.bonusStats.intelligence': 'intelligence',
                                'player.weapon.bonusStats.resistance': 'resistance', 
                                'player.weapon.bonusStats.luck': 'luck',
                                'player.weapon.baseDamage': 'baseDamage', 
                                'player.weapon.curExp': 'curExp',
                                'player.weapon.maxExp': 'maxExp', 
                                'player.weapon.foodTarget': 'foodTarget',
                                'player.weapon.category': 'category'
})


print(df_weapon_stat.head())

import_sql.ingest_data_from_dataframe(df_weapon_stat, 'player_weapon_stat')

df_pet_data = df_raw_data.copy()

df_pet_data = df_pet_data[['userId', 'player.pet.name', 'player.pet.origin',
                        'player.pet.element', 'player.pet.curHealth', 
                        'player.pet.foodTarget', 'player.pet.baseHealth', 
                        'player.pet.fedCount',  'player.pet.equipped',
                        'player.pet.elementResistancePercent', 'player.pet.fedCountToday',  
                        'player.pet.bonusType', 'player.pet.bonusTypePercent']]

df_pet_data = df_pet_data.rename(columns={
                    'userId': 'user_id',
                    'player.pet.name': 'name', 
                    'player.pet.origin': 'origin',
                    'player.pet.element': 'element', 
                    'player.pet.curHealth': 'curHealth', 
                    'player.pet.foodTarget': 'foodTarget',
                    'player.pet.baseHealth': 'baseHealth', 
                    'player.pet.fedCount': 'fedCount', 
                    'player.pet.equipped': 'equipped',
                    'player.pet.elementResistancePercent': 'elementResistancePercent', 
                    'player.pet.fedCountToday': 'fedCountToday',
                    'player.pet.bonusType': 'bonusType', 
                    'player.pet.bonusTypePercent': 'bonusTypePercent'
})


print(df_pet_data.head())

import_sql.ingest_data_from_dataframe(df_pet_data, 'player_pet_data')