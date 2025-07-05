import helper

from constants import config_table, config_db, transacted_table, transacted_db

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
