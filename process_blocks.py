from bitcoinlib.transactions import *
from bitcoinlib.services.bitcoind import BitcoindClient
import sqlite3

from constants import tx_table, tx_db, height_table, height_db, config_table, config_db, transacted_table, transacted_db
import helper


def get_block(bdc, height):
    block_hash = bdc.proxy.getblockhash(height)
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


def process_transactions(used_transactions, new_transactions, heights, deletes):
    with sqlite3.connect(tx_db) as conn:
        cur = conn.cursor()
        for tx in used_transactions:
            if tx in new_transactions:
                height, amount = new_transactions.pop(tx)
            else:
                cur.execute('SELECT height, amount FROM %s WHERE txid=? AND vout=?' % tx_table, tx)
                result = cur.fetchone()
                if result:
                    height, amount = result
                    deletes.append(tx)
                else:
                    raise Exception(tx, 'not found')
            if height in heights:
                heights[height] -= amount
            else:
                heights[height] = -amount


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
        cur.executemany('DELETE FROM %s WHERE txid=? AND vout=?' % tx_table, deletes)
        cur.close()
        conn.commit()
    helper.insert(tx_db, inserts, tx_table)


def get_height_inserts_and_updates(processed_block_height, heights):
    updates = []
    inserts = []
    for height in heights:
        if height > processed_block_height:
            inserts.append((height, heights[height]))
        else:
            updates.append((heights[height], height))
    return updates, inserts


def adjust_heights(updates, inserts):
    with sqlite3.connect(height_db) as conn:
        cur = conn.cursor()
        cur.executemany('UPDATE %s SET amount=amount+? WHERE height=?' % height_table, updates)
        cur.executemany('INSERT INTO %s VALUES(?,?)' % height_table, inserts)
        cur.close()
        conn.commit()


def insert_transacted(block_num, total_transacted):
    inserts = []
    for i in range(len(total_transacted)):
        inserts.append((block_num, total_transacted[i]))
        block_num += 1
    helper.insert(transacted_db, inserts, transacted_table)


def process_blocks():
    with sqlite3.connect(config_db) as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM %s' % config_table)
        base_url, processed_block_height = cur.fetchone()

    bdc = BitcoindClient(base_url=base_url)
    new_transactions = {}
    deletes = []
    heights = {}
    total_transacted = []
    coin_value = 100000000
    block_height = bdc.blockcount() - 6    # 6 blocks deep for security
    for i in range(processed_block_height+1, block_height+1):
        block = get_block(bdc, i)
        used_transactions, amount_at_height = get_transactions(block, new_transactions, coin_value, i)
        heights[i] = amount_at_height
        total_transacted.append(amount_at_height)
        process_transactions(used_transactions, new_transactions, heights, deletes)

    transaction_inserts = get_transaction_inserts(new_transactions)
    adjust_transactions(transaction_inserts, deletes)

    updates, inserts = get_height_inserts_and_updates(processed_block_height, heights)
    adjust_heights(updates, inserts)

    insert_transacted(processed_block_height+1, total_transacted)

    with sqlite3.connect(config_db) as conn:
        cur = conn.cursor()
        cur.execute('UPDATE %s SET block_height=?' % config_table, (block_height,))
        conn.commit()


process_blocks()
