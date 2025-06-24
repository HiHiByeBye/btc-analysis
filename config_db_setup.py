import helper


config_table = 'config'
config_table_creation = '''CREATE TABLE IF NOT EXISTS %s (
                            base_url CHAR(64) NOT NULL,
                            block_height INTEGER NOT NULL
                    )''' % config_table
config_db = config_table + '.db'
helper.create_db(config_db, config_table_creation, [])