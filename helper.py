import sqlite3


def create_db(db_name, table_creation, index_creation):
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        cur.execute(table_creation)
        for i in index_creation:
            cur.execute(i)
        conn.commit()


def insert(db_name, inserts, num_columns, table_name):
    with sqlite3.connect(db_name) as conn:
        cur = conn.cursor()
        try:
            cur.executemany('INSERT INTO %s VALUES(%s)' % (table_name, ', '.join(['?'] * num_columns)), inserts)
        except Exception as error:
            print(error)
        conn.commit()
