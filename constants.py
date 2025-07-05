database = 'databases/%s.db'

tx_table = 'txid_vout_height_amount'
tx_db = database % tx_table

height_table = 'height_amount'
height_db = database % height_table

config_table = 'config'
config_db = database % config_table

transacted_table = 'transacted'
transacted_db = database % transacted_table
