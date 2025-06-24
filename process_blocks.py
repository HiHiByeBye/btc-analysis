from bitcoinlib.transactions import *
from bitcoinlib.services.bitcoind import BitcoindClient
import sqlite3
import schedule
import time


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


def process_transactions(conn, used_transactions, new_transactions, heights, deletes):
    cur = conn.cursor()
    for tx in used_transactions:
        if tx in new_transactions:
            height, amount = new_transactions.pop(tx)
        else:
            cur.execute('SELECT height, amount FROM txid_vout_height_amount WHERE txid=? AND vout=?', tx)
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
    cur.close()


def get_transaction_inserts(new_transactions):
    inserts = []
    for tx in new_transactions:
        txid, vout = tx
        height, amount = new_transactions[tx]
        inserts.append((txid, vout, height, amount))
    return inserts


def adjust_transactions(conn, inserts, deletes):
    cur = conn.cursor()
    cur.executemany('DELETE FROM txid_vout_height_amount WHERE txid=? AND vout=?', deletes)
    cur.executemany('INSERT INTO txid_vout_height_amount VALUES(?,?,?,?)', inserts)
    cur.close()
    conn.commit()


def get_height_inserts_and_updates(processed_block_height, heights):
    updates = []
    inserts = []
    for height in heights:
        if height > processed_block_height:
            inserts.append((height, heights[height]))
        else:
            updates.append((heights[height], height))
    return updates, inserts


def adjust_heights(conn, updates, inserts):
    cur = conn.cursor()
    cur.executemany('UPDATE height_amount SET amount=amount+? WHERE height=?', updates)
    cur.executemany('INSERT INTO height_amount VALUES(?,?)', inserts)
    cur.close()
    conn.commit()


def process_blocks():
    with sqlite3.connect(config_db) as conn:
        cur = conn.cursor()
        cur.execute('SELECT * FROM config')
        base_url, processed_block_height = cur.fetchone()

    bdc = BitcoindClient(base_url=base_url)
    new_transactions = {}
    deletes = []
    heights = {}
    coin_value = 100000000
    with sqlite3.connect(tx_db) as tx_conn:
        with sqlite3.connect(height_db) as height_conn:
            block_height = bdc.blockcount() - 6    # 6 blocks deep for security
            for i in range(processed_block_height+1, block_height+1):
                block = get_block(bdc, i)
                used_transactions, amount_at_height = get_transactions(block, new_transactions, coin_value, i)
                heights[i] = amount_at_height
                process_transactions(tx_conn, used_transactions, new_transactions, heights, deletes)

            transaction_inserts = get_transaction_inserts(new_transactions)
            adjust_transactions(tx_conn, transaction_inserts, deletes)

            updates, inserts = get_height_inserts_and_updates(processed_block_height, heights)
            adjust_heights(height_conn, updates, inserts)

    with sqlite3.connect(config_db) as conn:
        cur = conn.cursor()
        cur.execute('UPDATE config SET block_height=?', (block_height,))
        conn.commit()


config_db = 'config.db'
tx_db = 'txid_vout_height_amount.db'
height_db = 'height_amount.db'

process_blocks()
