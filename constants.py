database = 'databases/%s.db'

tx_table = 'tx'
tx_db = database % tx_table

last_moved_table = 'last_moved'
last_moved_db = database % last_moved_table

config_table = 'config'
config_db = database % config_table

transacted_table = 'transacted_at_block'
transacted_db = database % transacted_table

block_time_table = 'block_time'
block_time_db = database % block_time_table

coin_value = 100000000
