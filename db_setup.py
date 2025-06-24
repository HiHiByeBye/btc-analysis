import pandas as pd

import helper
# block 901957

txid_table = 'txid_vout_height_amount'
txid_table_creation = '''CREATE TABLE IF NOT EXISTS %s (
                            txid CHAR(64) NOT NULL,
                            vout INTEGER NOT NULL, 
                            height INTEGER NOT NULL,
                            amount INTEGER NOT NULL,
                            PRIMARY KEY (txid, vout)
                    )''' % txid_table
txid_db = txid_table + '.db'
helper.create_db(txid_db, txid_table_creation, [])

height_table = 'height_amount'
height_table_creation = '''CREATE TABLE IF NOT EXISTS %s ( 
                            height INTEGER PRIMARY KEY,
                            amount INTEGER NOT NULL
                    )''' % height_table
height_db = height_table + '.db'
helper.create_db(height_db, height_table_creation, [])

chunksize = 10 ** 6
i = 1
txid_inserts = []
height_dict = {}
for chunk in pd.read_csv('out.txt', chunksize=chunksize):
    for index, row in chunk.iterrows():
        txid_inserts.append(tuple(row.values))
        height, amount = txid_inserts[-1][2], txid_inserts[-1][3]
        if height in height_dict:
            height_dict[height] += amount
        else:
            height_dict[height] = amount

    if txid_inserts:
        helper.insert(txid_db, txid_inserts, len(txid_inserts[-1]), txid_table)
        txid_inserts.clear()
    print(i * chunksize)
    i += 1

height_inserts = []
for height in height_dict:
    height_inserts.append((int(height), int(height_dict[height])))

if height_inserts:
    helper.insert(height_db, height_inserts, len(height_inserts[-1]), height_table)
