from bitcoinlib.services.bitcoind import BitcoindClient
import sqlite3

from constants import tx_table, tx_db, last_moved_table, last_moved_db, \
    config_table, config_db, transacted_table, transacted_db, \
    block_time_table, block_time_db, coin_value
import helper


def get_block(bdc, height, header):
    block_hash = bdc.proxy.getblockhash(height)
    if header:
        return bdc.proxy.getblockheader(block_hash, 2)
    else:
        return bdc.proxy.getblock(block_hash, 2)


def get_transactions(block, new_transactions, coin_value, height):
    used_transactions = []
    amount_at_height = 0
    for tx in block['tx']:
        txid = tx['txid']
        for vin in tx['vin']:
            if 'txid' in vin:
                used_transactions.append((vin['txid'], vin['vout']))
        for vout in tx['vout']:
            amount = int(float(vout['value']) * coin_value)
            if amount:
                vout_index = vout['n']
                new_transactions[(txid, vout_index)] = (height, amount)
                amount_at_height += amount
    return used_transactions, amount_at_height


def process_transactions(
        used_transactions,
        new_transactions,
        last_moved,
        deletes
    ):
    with sqlite3.connect(tx_db) as conn:
        cur = conn.cursor()
        for tx in used_transactions:
            if tx in new_transactions:
                height, amount = new_transactions.pop(tx)
            else:
                cur.execute('SELECT height, amount FROM %s WHERE txid=? AND ' +
                    'vout=?' % tx_table, tx)
                result = cur.fetchone()
                if result:
                    height, amount = result
                    deletes.append(tx)
                else:
                    raise Exception(tx, 'not found')
            if height in last_moved:
                last_moved[height] -= amount
            else:
                last_moved[height] = -amount


def get_transaction_inserts(new_transactions):
    inserts = []
    for tx in new_transactions:
        txid, vout = tx
        height, amount = new_transactions[tx]
        inserts.append((txid, vout, height, amount))
    return inserts


def adjust_transactions(inserts, deletes):
    with sqlite3.connect(tx_db) as conn:
        cur = conn.cursor()
        cur.executemany('DELETE FROM %s WHERE txid=? AND vout=?' % tx_table,
            deletes)
        cur.close()
        conn.commit()
    helper.insert(tx_db, inserts, tx_table)


def get_last_moved_updates(processed_block_height, last_moved):
    updates = []
    inserts = []
    for height in last_moved:
        if height > processed_block_height:
            inserts.append((height, last_moved[height]))
        else:
            updates.append((last_moved[height], height))
    return updates, inserts


def adjust_last_moved(updates, inserts):
    with sqlite3.connect(last_moved_db) as conn:
        cur = conn.cursor()
        cur.executemany('UPDATE %s SET amount=amount+? WHERE height=?' %
            last_moved_table, updates)
        cur.executemany('INSERT INTO %s VALUES(?,?)' % last_moved_table,
            inserts)
        cur.close()
        conn.commit()


def insert_transacted(block_num, total_transacted):
    inserts = []
    for i in range(len(total_transacted)):
        inserts.append((block_num, total_transacted[i]))
        block_num += 1
    helper.insert(transacted_db, inserts, transacted_table)


def insert_block_time(start, timestamps):
    inserts = []
    for timestamp in timestamps:
        inserts.append((start, timestamp))
        start += 1
    helper.insert(block_time_db, inserts, block_time_table)


def get_last_processed_block():
    with sqlite3.connect(config_db) as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM %s' % config_table)
        base_url, processed_block = cur.fetchone()
        return base_url, processed_block


def update_block_time_db(bdc, block_end):
    timestamps = []
    with sqlite3.connect(block_time_db) as conn:
        cur = conn.cursor()
        cur.execute('SELECT MAX(height) FROM %s' % block_time_table)
        first_block = cur.fetchone()
    if first_block is None:
        first_block = 0
    else:
        first_block += 1
    for i in range(first_block, block_end + 1):
        block = get_block(bdc, i, True)
        timestamps.append(block['mediantime'])
    insert_block_time(first_block, timestamps)


def process_blocks():
    base_url, processed_block = get_last_processed_block()
    bdc = BitcoindClient(base_url=base_url)
    new_transactions = {}
    deletes = []
    last_moved = {}
    total_transacted = []
    block_height_end = bdc.blockcount() - 6    # 6 blocks deep because chance that someone can overwrite 6 blocks is very low 
    block_height_start = processed_block + 1
    for i in range(block_height_start, block_height_end + 1):
        block = get_block(bdc, i, False)
        used_transactions, amount_at_height = get_transactions(block,
            new_transactions, coin_value, i)
        last_moved[i] = amount_at_height
        total_transacted.append(amount_at_height)
        process_transactions(used_transactions, new_transactions, last_moved,
            deletes)

    transaction_inserts = get_transaction_inserts(new_transactions)
    adjust_transactions(transaction_inserts, deletes)

    updates, inserts = get_last_moved_updates(processed_block,
        last_moved)
    adjust_last_moved(updates, inserts)

    insert_transacted(block_height_start, total_transacted)

    update_block_time_db(bdc, block_height_end)

    helper.update_config_block(block_height_end)


process_blocks()
