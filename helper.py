import sqlite3

from constants import config_table, config_db

def create_db(db_name, table_creation, index_creation):
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute(table_creation)
        for i in index_creation:
            cur.execute(i)
        conn.commit()


def insert(db_name, inserts, table_name):
    if inserts:
        num_columns = len(inserts[0])
    else:
        return
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        try:
            cur.executemany('INSERT INTO %s VALUES(%s)' %
                (table_name, ', '.join(['?'] * num_columns)), inserts)
        except Exception as error:
            print(error)
        conn.commit()


def update_config_block(end):
    with sqlite3.connect(config_db) as conn:
        cur = conn.cursor()
        cur.execute('UPDATE %s SET block_height=?' % config_table, (end,))
        conn.commit()