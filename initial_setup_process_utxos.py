import pandas as pd

import helper
from constants import tx_table, tx_db, last_moved_table, last_moved_db, \
    config_table, config_db, transacted_table, transacted_db, \
    block_time_table, block_time_db


txid_table_creation = '''CREATE TABLE IF NOT EXISTS %s (
                            txid CHAR(64) NOT NULL,
                            vout INTEGER NOT NULL, 
                            height INTEGER NOT NULL,
                            amount INTEGER NOT NULL,
                            PRIMARY KEY (txid, vout)
                      )''' % tx_table
helper.create_db(tx_db, txid_table_creation, [])

last_moved_table_creation = '''CREATE TABLE IF NOT EXISTS %s ( 
                            height INTEGER PRIMARY KEY,
                            amount INTEGER NOT NULL
                        )''' % last_moved_table
helper.create_db(last_moved_db, last_moved_table_creation, [])

config_table_creation = '''CREATE TABLE IF NOT EXISTS %s (
                            base_url CHAR(64) NOT NULL,
                            block_height INTEGER NOT NULL
                        )''' % config_table
helper.create_db(config_db, config_table_creation, [])

transacted_creation = '''CREATE TABLE IF NOT EXISTS %s (
                        height INTEGER PRIMARY KEY,
                        transacted INTEGER NOT NULL
                      )''' % transacted_table
helper.create_db(transacted_db, transacted_creation, [])

block_time_creation = '''CREATE TABLE IF NOT EXISTS %s (
                            block INTEGER PRIMARY KEY,
                            time INTEGER
                      )''' % block_time_table
helper.create_db(block_time_db, block_time_creation, [])

utxo = 'out.txt'
chunksize = 10 ** 6
i = 1
txid_inserts = []
last_moved_dict = {}
last_block = 0
for chunk in pd.read_csv(utxo, chunksize=chunksize):
    for index, row in chunk.iterrows():
        txid_inserts.append(tuple(row.values))
        height, amount = txid_inserts[-1][2], txid_inserts[-1][3]
        last_block = max(last_block, height)
        if height in last_moved_dict:
            last_moved_dict[height] += amount
        else:
            last_moved_dict[height] = amount

    if txid_inserts:
        helper.insert(tx_db, txid_inserts, tx_table)
        txid_inserts.clear()
    print(i * chunksize)
    i += 1

last_moved_inserts = []
for height in last_moved_dict:
    last_moved_inserts.append((int(height), int(last_moved_dict[height])))

if last_moved_inserts:
    helper.insert(last_moved_db, last_moved_inserts, last_moved_table)

helper.update_config_block(last_block)
