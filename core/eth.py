#!/usr/bin/env python
# -*- encoding: utf-8 -*-

__author__ = "KennBro"
__copyright__ = "Copyright 2024, Personal Research"
__credits__ = ["KennBro"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "KennBro"
__email__ = "kennbro <at> protonmail <dot> com"
__status__ = "Development"


import time
import json
import logging
import requests
import sqlite3
import traceback
import pandas as pd
from sqlalchemy import text
from datetime import datetime
from flask import jsonify

from termcolor import colored
import coloredlogs, logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
coloredlogs.install(level='DEBUG', fmt=log_format, logger=logger)
logger.propagate = False  # INFO: To prevent duplicates with flask


def insert_with_ignore(table, conn, keys, data_iter):
    # INFO: This escape is for reserved words
    escaped_keys = [f'"{k}"' if (k.lower() == 'from') or (k.lower() == 'to') else k for k in keys]

    columns = ','.join(escaped_keys)
    placeholders = ','.join([f":{k}" for k in keys])
    stmt = f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    data_dicts = (dict(zip(keys, data)) for data in data_iter)
    
    for data in data_dicts:
        conn.execute(stmt, data)


def db_store_wallet_detail(conn, data):    

    conn.execute(f"""INSERT INTO t_address_detail VALUES 
                   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?)
                   """,
         ('eth',  # TODO: Manage multiple blockchain
         data['address'],
         data['blockNumber'],
         data['last_block'],
         data['blockNumber'],
         data['timeStamp'],
         data['hash'],
         data['from'],
         data['to'],
         data['value'],
         data['input'],
         '',
         data['type'],
         '',
         '',
         '',
         '',
         '',
         '',
         '',
         '',
         data['contractAddress'],
         ''))

    conn.commit()


def db_store_address_block(conn, data):    

    conn.execute(f"""INSERT INTO t_blocks VALUES 
                   (?, ?, ?, ?, ?, ?, ?)
                   """,
         ('eth',  # TODO: Manage multiple blockchain
         data['address'],
         data['blockNumber'],
         data['last_block'],
         data['timeStamp'],
         data['timeStamp_to'],
         data['all_data']))

    conn.commit()


def db_store_contracts(conn, datas):

    for data in datas:
        try: 
            conn.execute(f"""INSERT INTO t_contract VALUES 
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?)
                           """,
                ('eth',
                 data['contract'],
                 data['SourceCode'],
                 data['ABI'],
                 data['ContractName'],
                 data['CompilerVersion'],
                 data['OptimizationUsed'],
                 data['Runs'],
                 data['ConstructorArguments'],
                 data['EVMVersion'],
                 data['Library'],
                 data['LicenseType'],
                 data['Proxy'],
                 data['Implementation'],
                 data['SwarmSource']))
        except Exception as e:
            print(e)

    conn.commit()


def event_stream_ether(params):
    logger.info(f"Getting information")
    data = json.dumps({"msg": f"Getting information", "end": False, "error": False, "content": {}})
    yield f"data:{data}\n\n"

    # Checking params
    logger.info(f"Checking params")
    data = json.dumps({"msg": f"Checking params", "end": False, "error": False, "content": {}})
    yield f"data:{data}\n\n"
    if (params.get('address', '') == ''):
        message = f"<strong>Param address is mandatory</strong>"
        logger.error(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": message, "end": True, "error": True, "content": {}})
        yield f"data:{data}\n\n"
    else:
        address = params.get('address').lower()

        # INFO: Get Source
        if (params.get('source', '') == ''):
            source = 'central'
        else:
            source = params.get('source', '')

        # FIX: Eliminate chunks
        if (params.get('chunk', '') == ''):
            chunk = 100000  # TODO: Control the chunk size
        else:
            chunk = params.get('chunk', '')

        if (params.get('block_from', '') == ''):
            block_from = 0
        else:
            block_from = params.get('block_from', '')

        if (params.get('block_to', '') == ''):
            block_to = block_from + chunk
        else:
            block_to = params.get('block_to', '')

        # TODO: Validate if user send trx reference

        # Checking wallet and first trx
        key = params['config']['ethscan']
        dbname = params['config']['dbname']

        connection = sqlite3.connect(dbname)
        cursor = connection.cursor()

        # INFO: Update central address
        query = f"SELECT * FROM t_tags WHERE tag = 'central'"
        cursor.execute(query)
        central = cursor.fetchall()
        logger.debug(f"Central address: {central}")
        if (central):
            update = f"UPDATE t_tags SET blockChain = 'eth', address = '{address}' WHERE tag = 'central'"
            connection.execute(update)
            logger.debug(f"UPDATE Central: {update}")
            connection.commit()
        else:
            insert = f"INSERT INTO t_tags (blockChain, address, tag) VALUES ('eth', '{address}', 'central')"
            connection.execute(insert)
            logger.debug(f"INSERT Central: {insert}")
            connection.commit()
        
        # INFO: Update path address
        insert = f"INSERT OR IGNORE INTO t_tags (blockChain, address, tag) VALUES ('eth', '{address}', 'path')"
        connection.execute(insert)
        logger.debug(f"INSERT Path: {insert}")
        connection.commit()

        # INFO: Verify info of address
        query = f"SELECT * FROM t_address_detail WHERE address = '{address}'"
        cursor.execute(query)
        wallet_detail = cursor.fetchall()
        json_object = []
        json_internals = []
        json_transfers = []
        json_nfts = []
        json_multitokens = []

        # INFO: Get infoamtion 
        if (wallet_detail == []):
            # INFO: Verify if address is contract
            type = 'wallet'
            contract_creation = {}
            try:
                url = f"https://api.etherscan.io/api?module=contract&action=getcontractcreation&contractaddresses={address}&apikey={key}"
                response = requests.get(url)
                contract_creation = response.json()
                json_status = response.json()['status']

                if (json_status == "1"):
                    type = 'contract'
                    message = f"<strong>ADDRESS</strong> - Contract"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"
                else:
                    message = f"<strong>ADDRESS</strong> - Wallet"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

            except Exception:
                traceback.print_exc()
                traceback_text = traceback.format_exc()

                connection.close()
                message = f"<strong>Error...</strong>"
                logger.warning(f"{message}")
                data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                yield f"data:{data}\n\n"

                for line in traceback_text.splitlines():
                    message = f"{line}"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                message = f" "
                logger.warning(f"{message}")
                data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                yield f"data:{data}\n\n"

                # HACK: Here should be a raise?

            # INFO: Bypass contract information from search
            # if (source == "search" and type == "contract"):
            if (type == "contract"):
                # TODO: 
                # - Get creator and trx (X)
                # - Store data in tagging table
                # - Store data in label table (name of contract)

                # INFO: Get contract information
                json_contract = []
                try:
                    url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}"
                    response = requests.get(url)
                    json_contract = response.json()['result']

                    if (len(json_contract) > 0):
                        message = f"<strong>CONTRACT</strong> - Collected information"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>CONTRACT<strong> - <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store data in contract table
                try:
                    message = f"<strong>CONTRACT</strong> - Storing information..."
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # Store contract
                    db_store_contracts(connection, json_contract)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get trx creation
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=1&sort=asc&apikey={key}"
                    response = requests.get(url)
                    json_object = response.json()['result']

                    if (len(json_object) > 0):
                        message = f"<strong>TRANSACTIONS</strong> - Found trx of creation contract"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                        # INFO: Modify the trx creation, necessary to graph
                        json_object[0]['to'] = json_object[0]['contractAddress']
                    else:
                        message = f"<strong>TRANSACTIONS<strong> - Creation contract <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store wallet detail (contract)
                try:
                    message = f"<strong>TRANSACTIONS</strong> - Storing details...Contract..."
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # Get first trx
                    first = json_object[0]
                    logger.debug(f"First trx :\n{first}")
                    # Determine type of address   # TODO: NFT
                    first['type'] = 'contract'
                    if (first['to'] == '' and first['contractAddress'] != ''):
                        first['type'] = 'contract'
                    # Get last block collected
                    first['last_block'] = json_object[-1]['blockNumber']
                    first['timeStamp_to'] = json_object[-1]['timeStamp']
                    # TODO: Do this check more complex for large wallets and contracts, 
                    first['all_data'] = True  # INFO: Because it is a contract
                    # Set address
                    first['address'] = address

                    # PERF: Analyze if wallet_detail and block table can merge
                    
                    # Store detail
                    db_store_wallet_detail(connection, first)
                    # Store blocks
                    db_store_address_block(connection, first)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store transaction of contract creation
                try:
                    message = f"<strong>DATA COLLECTED</strong> - Storing..."
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    tic = time.perf_counter()
                    # db_store_transactions_optimized(connection, json_object)
                    df_trx_store = pd.DataFrame(json_object)
                    df_trx_store['blockChain'] = 'eth'
                    df_trx_store.to_sql('t_transactions', connection, if_exists='append', index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transactions of contract creation...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.error(message.replace('<strong>', '').replace('</strong>', ''))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store internal tagging and labels of contract
                try:
                    tic = time.perf_counter()

                    contract_tagging = []
                    # contract_tagging.append(('blockChain': 'eth', 'address': address, 'tag': 'contract'})
                    contract_tagging.append(('eth', address, 'contract'))
                    # print(f"Contract creation: {contract_creation}")
                    # print(f"Contract creator: {contract_creation['result'][0]['contractCreator']}")
                    # contract_tagging.append({'blockChain': 'eth', 'address': contract_creation['result'][0]['contractCreator'], 'tag': 'contract creator'})
                    contract_tagging.append(('eth', contract_creation['result'][0]['contractCreator'], 'contract creator'))
                    # print(f"Contract tagging: {contract_tagging}")
                    # Insert tags in SQLite
                    cursor = connection.cursor()
                    insert_tag = 'INSERT OR IGNORE INTO t_tags (blockChain, address, tag) VALUES (?, ?, ?)'
                    cursor.executemany(insert_tag, contract_tagging)
                    # cursor.executemany(insert_tag, contract_tagging.to_records(index=False))
                    connection.commit()

                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Tagging...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    tic = time.perf_counter()

                    # INFO: Check if address exist in labels table
                    query = f"SELECT * FROM t_labels WHERE address = '{address}'"
                    cursor.execute(query)
                    label = cursor.fetchall()
                    if (len(label) == 0) and (len(json_contract) > 0):
                        # INFO: Store internal label
                        if (json_contract[0]['ContractName'] == ''):
                            contract_label = [('ethereum', 'internal_label', address, 'Contract without name', '["caution", "noName"]')]
                        else:
                            contract_label = [('ethereum', 'internal_label', address, 
                                               json_contract[0]['ContractName'], 
                                               '["' + json_contract[0]['ContractName'][:10] + '"]')]
                        # Insert tags in SQLite
                        cursor = connection.cursor()
                        insert_label = 'INSERT OR IGNORE INTO t_labels (blockChain, source, address, name, labels) VALUES (?, ?, ?, ?, ?)'
                        # cursor.executemany(insert_tag, contract_label.to_records(index=False))
                        cursor.executemany(insert_label, contract_label)
                        connection.commit()

                        toc = time.perf_counter()
                        message = f"<strong>STORE</strong> - Internal label...<strong>{toc - tic:0.4f}</strong> seconds"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    # INFO: Store contract creation info
                    cursor = connection.cursor()
                    insert_creator = """INSERT OR IGNORE INTO t_founders_creators 
                        (blockChain, blockNumber, type, timeStamp, hash, `from`, `to`, value, input, contractAddress, tokenDecimal, tokenSymbol, tokenName) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                    creator_row = [("eth",
                                    json_object[0]['blockNumber'], 
                                    "creation",
                                    json_object[0]['timeStamp'],
                                    json_object[0]['hash'],
                                    json_object[0]['from'],
                                    json_object[0]['to'],
                                    json_object[0]['value'],
                                    json_object[0]['input'],
                                    json_object[0]['contractAddress'],
                                    "",
                                    "",
                                    "")]
                    cursor.executemany(insert_creator, creator_row)
                    connection.commit()

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.error(message.replace('<strong>', '').replace('</strong>', ''))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

            # INFO: Get wallet info
            elif (type == "wallet"):
                # INFO: Get trx
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={key}"
                    response = requests.get(url)
                    json_object = response.json()['result']

                    if (len(json_object) > 0):
                        message = f"<strong>TRANSACTIONS</strong> - Found first block and trx"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>TRANSACTIONS<strong> - First block and trx <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get internals
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_internals = response.json()['result']

                    if (len(json_internals) > 0):
                        message = f"<strong>INTERNALS</strong> - Found internals"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>INTERNALS<strong> - Internals <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get transfers
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_transfers = response.json()['result']

                    if (len(json_transfers) > 0):
                        message = f"<strong>TRANSFERS</strong> - Found transfers"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>TRANSFERS<strong> - Transfers <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get NFTs (ERC-721)
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=tokennfttx&address={address}&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_nfts = response.json()['result']

                    if (len(json_nfts) > 0):
                        message = f"<strong>NFTs</strong> - Found NFTs"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>NFTs<strong> - Transfers <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get Multitoken trx (ERC-1551)
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=token1155tx&address={address}&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_multitokens = response.json()['result']

                    if (len(json_multitokens) > 0):
                        message = f"<strong>MULTITOKENS</strong> - Found Multi Token Standard"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>MULTITOKENS<strong> - Transfers <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # TODO: If wallet is a contract, get and store contract information

                # INFO: Store wallet detail in DB
                try:
                    message = f"<strong>TRANSACTIONS</strong> - Storing details..."
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # Get first trx
                    first = json_object[0].copy()
                    logger.debug(f"First trx :\n{first}")
                    # Determine type of address   # TODO: NFT
                    first['type'] = 'wallet'

                    if (first['to'] == '' and first['contractAddress'] != ''):
                        first['type'] = 'contract'
                    # Get last block collected
                    first['last_block'] = json_object[-1]['blockNumber']
                    first['timeStamp_to'] = json_object[-1]['timeStamp']
                    # TODO: Do this check more complex for large wallets and contracts, 
                    if (len(json_object) == 10000):
                        first['all_data'] = False
                    else: 
                        first['all_data'] = True
                    # Set address
                    first['address'] = address

                    # PERF: Analyze if wallet_detail and block table can merge
                    
                    # Store detail
                    db_store_wallet_detail(connection, first)
                    # Store blocks
                    db_store_address_block(connection, first)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store transaction, internals, transfers and NFTs in DB
                try:
                    message = f"<strong>DATA COLLECTED</strong> - Storing..."
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # HACK: Here is the crux of it all, because I have to transfer the methodID and funcname to the rest!!!!
                    # TODO: Register in internals, transfers, nfts and multitoken the trxs neccesary to complete methodIs and functionName
                    # INFO: transactions
                    tic = time.perf_counter()
                    # db_store_transactions_optimized(connection, json_object)
                    df_trx_store = pd.DataFrame(json_object)
                    df_trx_store['blockChain'] = 'eth'
                    df_trx_store.to_sql('t_transactions', connection, if_exists='append', index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transactions...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: internals
                    tic = toc
                    # db_store_internals_optimized(connection, json_internals)
                    if (json_internals != []):
                        df_internals_store = pd.DataFrame(json_internals)
                        df_internals_store['blockChain'] = 'eth'
                        df_internals_merged = df_internals_store.merge(df_trx_store[['hash', 'methodId', 'functionName']], on='hash', how='left')
                        df_internals_merged.fillna('', inplace=True)
                        df_internals_merged.to_sql('t_internals', connection, if_exists='append', index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Internals...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: transfers
                    tic = toc
                    # db_store_transfers_optimized(connection, json_transfers)
                    if (json_transfers != []):
                        df_transfers_store = pd.DataFrame(json_transfers)
                        df_transfers_store['blockChain'] = 'eth'
                        df_transfers_merged = df_transfers_store.merge(df_trx_store[['hash', 'methodId', 'functionName']], on='hash', how='left')
                        df_transfers_merged.fillna('', inplace=True)
                        df_transfers_merged.to_sql('t_transfers', connection, if_exists='append', index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transfers ERC20...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: NFTs
                    tic = toc
                    # db_store_nfts_optimized(connection, json_nfts)
                    if (json_nfts != []):
                        df_nfts_store = pd.DataFrame(json_nfts)
                        df_nfts_store['blockChain'] = 'eth'
                        df_nfts_merged = df_nfts_store.merge(df_trx_store[['hash', 'methodId', 'functionName']], on='hash', how='left')
                        df_nfts_merged.fillna('', inplace=True)
                        df_nfts_merged.to_sql('t_nfts', connection, if_exists='append', index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transfers ERC721...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: Multitokens
                    tic = toc
                    # db_store_multitoken_optimized(connection, json_multitokens)
                    if (json_multitokens != []):
                        df_multitoken_store = pd.DataFrame(json_multitokens)
                        df_multitoken_store['blockChain'] = 'eth'
                        df_multitoken_merged = df_multitoken_store.merge(df_trx_store[['hash', 'methodId', 'functionName']], on='hash', how='left')
                        df_multitoken_merged.fillna('', inplace=True)
                        df_multitoken_merged.to_sql('t_multitoken', connection, if_exists='append', index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transfers ERC1155...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.error(message.replace('<strong>', '').replace('</strong>', ''))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Generating internals tags
                tic = time.perf_counter()
                # db_store_tagging(connection, address, json_object, json_transfers, json_internals)
                db_store_tagging_opt(connection, address, json_object, json_transfers, json_internals, json_nfts, json_multitokens)
                toc = time.perf_counter()
                message = f"<strong>STORE</strong> - Tagging...<strong>{toc - tic:0.4f}</strong> seconds"
                logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                yield f"data:{data}\n\n"

        # INFO: Exist information in db
        else:
            # FIX: Do better

            # TODO: Add contract validation

            # Validate if all trxs are collected
            query = f"SELECT all_data FROM t_blocks WHERE address = '{address}';"
            cursor.execute(query)
            all_data_collected = cursor.fetchone()

            # INFO: No more info to collect
            if (not all_data_collected[0]): 

                # Get last block number
                query = f"SELECT rowid, * FROM t_blocks WHERE address = '{address}' ORDER BY block_to DESC LIMIT 1;"
                cursor.execute(query)
                row = cursor.fetchone()
                last_block = int(row[4]) + 1
                rowid = int(row[0])

                logger.debug(f"Last block: {last_block}")
                
                # Get more blocks from last block
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={last_block}&endblock=99999999&sort=asc&apikey={key}"
                    # print(url)
                    response = requests.get(url)
                    json_object = response.json()['result']

                    if (len(json_object) > 0):
                        message = f"<strong>TRANSACTIONS</strong> - Get more transactions"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = f"<strong>TRANSACTIONS<strong> - More transactions <strong>NOT FOUND</strong>"
                        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # Store transaction in DB
                try:
                    message = f"<strong>TRANSACTIONS</strong> - Storing..."
                    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    db_store_transactions(connection, json_object)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = f"<strong>Error...</strong>"
                    logger.error(message.replace('<strong>', '').replace('</strong>', ''))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = f" "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # Get new last block and date
                new_last_block = json_object[-1]['blockNumber']
                new_timeStamp_to = json_object[-1]['timeStamp']

                logger.debug(f"New Last block: {json_object[-1]}")
                logger.debug(f"New Last block number: {new_last_block}")

                # Update last block
                update = f"UPDATE t_blocks SET block_to = {new_last_block}, date_to = {new_timeStamp_to} WHERE rowid = {rowid}"
                cursor.execute(update)
                logger.debug(f"UPDATE: {update}")
                connection.commit()

                # TODO: Store transfer in DB
                # TODO: Store internal in DB
                # TODO: Generating internals tags

        # INFO: Send wallet detail information
        query = f"SELECT * FROM t_address_detail WHERE address = '{address}'"
        cursor.execute(query)
        wallet_detail = cursor.fetchall()

        message = f"<strong>TRANSACTIONS</strong> - Receiving wallet detail information..."
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"wallet_detail": wallet_detail}})
        yield f"data:{data}\n\n"

        # INFO: Get address and path
        query = f"SELECT * FROM t_tags WHERE tag = 'central'"
        cursor.execute(query)
        address = cursor.fetchone()
        # print(f"ADDRESS: {address}")
        type = wallet_detail[0][12]

        query = "SELECT address FROM t_tags WHERE tag = 'path' AND blockChain = 'eth'"
        cursor.execute(query)
        rows = cursor.fetchall()

        # Get addresses in path
        addresses = [row[0] for row in rows]

        message = f"<strong>DATA</strong> - Received central and path addresses cached data..."
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"address": address, "addresses": addresses}})
        yield f"data:{data}\n\n"

        # INFO: Get trxs, internals and transfers
        message = f"<strong>DATA</strong> - Getting collected trxs, internals, transfers..."
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        tic = time.perf_counter()
        # trxs = get_trx_from_address(connection, address)
        trxs = get_trx_from_addresses_opt(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Proccesed...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                           "content": {"graph": trxs['transactions'], "list": trxs['list'], "stat": trxs['stat']}})
        yield f"data:{data}\n\n"

        # INFO: Get Founders and creators
        tic = time.perf_counter()
        founders = get_founders_creators(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Founders and creators...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"founders": founders}})
        yield f"data:{data}\n\n"

        # INFO: Get Balance and Gas
        tic = time.perf_counter()
        balance = get_balance_and_gas(connection, address, type, key)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Balance and Gas...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                           "content": {"balance": balance['balance'], "tokens": balance['tokens'], "gas": balance['gas']}})
        yield f"data:{data}\n\n"

        # INFO: Get tags and labels
        tic = time.perf_counter()
        tags = get_tags_labels(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Tags and Labels...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                           "content": {"tags": tags['tags'], "labels": tags['labels']}})
        yield f"data:{data}\n\n"

        message = f"<strong>End collection</strong>"
        logger.error(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": True, "error": False, "content": {}})
        yield f"data:{data}\n\n"

def test_function_1(params):
    # Checking wallet and first trx
    dbname = params['config']['dbname']
    address = params.get('address')
    connection = sqlite3.connect(dbname)

    address = ('eth', address, 'central')
    # trxs = get_trx_from_address(connection, address)

    tic = time.perf_counter()
    data = get_trx_from_addresses_opt(connection, address)
    # data = get_balance_and_gas(connection, address, 'wallet', '')
    toc = time.perf_counter()
    logger.info(f"Execute test_1 in {toc - tic:0.4f} seconds")

    return data


def test_function_2(params):
    # Checking wallet and first trx
    dbname = params['config']['dbname']
    address = params.get('address')
    connection = sqlite3.connect(dbname)

    address = ('eth', address, 'central')

    tic = time.perf_counter()
    # data = get_trx_from_addresses_opt_bkp(connection, address)
    data = get_founders_creators(connection, address)
    # data = get_tags_labels(connection, address)
    toc = time.perf_counter()
    logger.info(f"Execute test_2 in {toc - tic:0.4f} seconds")

    return data


# TODO: Include type of address and evaluate
# HACK: Merge with labels??
# TODO: Add NFTs and Multitoken
def db_store_tagging_opt(connection, address, trxs, transfers, internals, nfts, multitoken):
    # Create dataframes
    if not trxs:
        df_t = pd.DataFrame(columns=['blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                     'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName'])
    else:
        df_t = pd.DataFrame(trxs)
    if not transfers:
        df_f = pd.DataFrame(columns=['blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                     'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName'])
    else:
        df_f = pd.DataFrame(transfers)
    if not internals:
        df_i = pd.DataFrame(columns=['blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                     'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName'])
    else:
        df_i = pd.DataFrame(internals)
    if not nfts:
        df_n = pd.DataFrame(columns=['blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                     'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName'])
    else:
        df_n = pd.DataFrame(nfts)
    if not multitoken:
        df_m = pd.DataFrame(columns=['blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                     'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName'])
    else:
        df_m = pd.DataFrame(multitoken)

    # Safe concat
    address_columns = ['from', 'to', 'contractAddress']
    # addresses = pd.concat([df[column].dropna().unique() for df in [df_t, df_f, df_i, df_n, df_m] for column in address_columns if column in df.columns])
    addresses = pd.concat([pd.Series(df[column].dropna().unique()) for df in [df_t, df_f, df_i, df_n, df_m] for column in address_columns if column in df.columns])


    df_addresses = pd.DataFrame(addresses.unique(), columns=['address'])
    df_addresses['blockChain'] = 'eth'  # TODO: Multichain
    df_addresses['tag'] = 'wallet'

    # Detect contracts
    # FIX: This should be more complex
    # - If address is contract, determine contract to contract (?)
    contracts = pd.concat([
        df_t.loc[(df_t['input'] != '0x') & (df_t['from'] != address), 'from'],
        df_t.loc[(df_t['input'] != '0x') & (df_t['to'] != address), 'to'],
        # df_t.loc[df_t['input'] != '', 'to'],
        df_f['contractAddress'].dropna(),
        df_n['contractAddress'].dropna(),
        df_m['contractAddress'].dropna()
    ]).unique()

    # TODO: NTFs and Multitoken should be separated and tagged like "contract nft" and "contract multitoken" respectively

    df_addresses.loc[df_addresses['address'].isin(contracts), 'tag'] = 'contract'

    # INFO: Founders or creators
    # TODO: Evaluate type to distinguish Founders and creators
    # df_all = pd.concat([df[['from', 'to', 'timeStamp']] for df in [df_t, df_f, df_i] if 'timeStamp' in df.columns], ignore_index=True).sort_values(by='timeStamp')
    df_all = pd.concat([df[['from', 'to', 'timeStamp']] for df in [df_t, df_f, df_i, df_n, df_m] if 'timeStamp' in df.columns], ignore_index=True).sort_values(by='timeStamp')
    min_timestamp = df_all.loc[df_all['from'] == address, 'timeStamp'].min()
    founders_addresses = df_all.loc[df_all['timeStamp'] < min_timestamp, 'from'].unique()

    df_founders = pd.DataFrame(founders_addresses, columns=['address'])
    df_founders['blockChain'] = 'eth'
    df_founders['tag'] = 'founder'

    # Unifying tags
    df_addresses = pd.concat([df_addresses, df_founders]).drop_duplicates().reset_index(drop=True)

    # Insert tags in SQLite
    cursor = connection.cursor()
    insert_tag = 'INSERT OR IGNORE INTO t_tags (address, blockChain, tag) VALUES (?, ?, ?)'
    cursor.executemany(insert_tag, df_addresses.to_records(index=False))
    connection.commit()

    # Store trxs Founders
    founders = df_founders['address'].tolist()
    df_t_f = df_t.loc[(df_t['to'] == address) & 
                      (df_t['from'].isin(founders)) & 
                      (df_t['timeStamp'] < min_timestamp)].assign(
                              type='transaction').assign(blockChain='eth').assign(tokenDecimal=18).assign(tokenSymbol='ETH').assign(tokenName='Ether')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    df_i_f = df_i.loc[(df_i['to'] == address) & 
                      (df_i['from'].isin(founders)) & 
                      (df_i['timeStamp'] < min_timestamp)].assign(
                              type='internal').assign(blockChain='eth').assign(tokenDecimal=18).assign(tokenSymbol='ETH').assign(tokenName='Ether')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    df_f_f = df_f.loc[(df_f['to'] == address) & 
                      (df_f['from'].isin(founders)) & 
                      (df_f['timeStamp'] < min_timestamp)].assign(
                              type='transfer').assign(blockChain='eth')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    # INFO: Neccesary rename to avoid add columns to founders
    df_n.rename(columns={"tokenID": "value"}, inplace=True)
    df_n_f = df_n.loc[(df_n['to'] == address) & 
                      (df_n['from'].isin(founders)) & 
                      (df_n['timeStamp'] < min_timestamp)].assign(
                              type='nfts').assign(blockChain='eth')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    # INFO: Neccesary rename to avoid add columns to founders
    df_m.rename(columns={"tokenID": "value"}, inplace=True)
    df_m.rename(columns={"tokenValue": "tokenDecimal"}, inplace=True)
    df_m_f = df_m.loc[(df_m['to'] == address) & 
                      (df_m['from'].isin(founders)) & 
                      (df_m['timeStamp'] < min_timestamp)].assign(
                              type='nfts').assign(blockChain='eth')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]

    df_founders_trxs = pd.concat([df_t_f, df_i_f, df_f_f, df_n_f, df_m_f])

    # Insert tags in SQLite
    cursor = connection.cursor()
    insert_founders = """INSERT OR IGNORE INTO t_founders_creators 
        (blockChain, blockNumber, type, timeStamp, hash, `from`, `to`, value, input, contractAddress, tokenDecimal, tokenSymbol, tokenName) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.executemany(insert_founders, df_founders_trxs.to_records(index=False))
    connection.commit()


def get_trx_from_addresses_opt(conn, address_central):

    # INFO: address_central is
    # ('eth', '0x7f3acf451e372f517b45f9d2ee0e42e31bc5e53e', 'central')
    # For future Multichain (?)

    address_central = address_central[1]

    nodes = []
    nodes_list = []
    links = []
    stat_tot = 0
    stat_trx = 0
    stat_int = 0
    stat_tra = 0
    stat_err = 0
    stat_con = 0
    stat_wal = 0
    # stat_coo = 0  # TODO: Contract owned
    # max_qty = 0

    # INFO: Tagging
    df_tags = pd.read_sql_query("SELECT address, tag FROM t_tags", conn)
    tags_grouped = df_tags.groupby('address')['tag'].apply(list).reset_index(name='tags')
    tags_dict = pd.Series(tags_grouped.tags.values,index=tags_grouped.address).to_dict()

    # INFO: Labels
    df_labels = pd.read_sql_query("SELECT * FROM t_labels WHERE blockChain = 'ethereum'", conn)  # TODO: Multichain
    labels_dict = df_labels.set_index('address').apply(lambda row: row.to_dict(), axis=1).to_dict()

    # stat_coo = 
    # stat_con = len(json_tags) - stat_coo

    # TODO: 
    # - Isolate contract creation trx
    # - Join query with labels
    # - More info in links

    # INFO: Get all Trx, Transfers and internals
    query = """
        SELECT blockChain, type, hash, `from`, `to`, value, contractAddress, symbol, name, decimal, valConv, timeStamp, isError
        FROM (
            SELECT blockChain, 'transaction' as 'type', hash, `from`, `to`, value, 
                contractAddress, 'ETH' as 'symbol', 'Ether' as 'name', 18 as 'decimal', value / POWER(10, 18) as valConv, timeStamp, isError
            FROM t_transactions
            UNION ALL
            SELECT blockChain, 'internals', hash, `from`, `to`, value, 
                contractAddress, 'ETH', 'Ether', 18, value / POWER(10, 18), timeStamp, isError
            FROM t_internals
            UNION ALL
            SELECT blockChain, 'transfers', hash, `from`, `to`, value, 
                contractAddress, tokenSymbol, tokenName, tokenDecimal, value / POWER(10, tokenDecimal), timeStamp, 0
            FROM t_transfers
        ) AS combined
        ORDER BY timeStamp ASC
    """
    df_all = pd.read_sql_query(query, conn)

    # INFO: Convert to datetime
    df_all['timeStamp'] = pd.to_datetime(df_all['timeStamp'], unit='s')
    stat_err = len(df_all[df_all['isError'] != 0])
    df_all = df_all[df_all['isError'] == 0]

    nodes = {}
    links = {}

    for _, row in df_all.iterrows():
        from_address = row['from']
        to_address = row['to']
        symbol = row['symbol']
        value = float(row['valConv'])
        type = row['type']

        for address in [from_address, to_address]:
            if address not in nodes:
                tag = tags_dict.get(address, [])  # Get tag
                label = labels_dict.get(address, [])  # Get label
                nodes[address] = {
                    "id": address, 
                    "address": address,
                    # "tag": [tag] if tag else [],
                    "tag": tag,
                    "label": label,
                    "token": "ETH"
                    # "trx_in": {},
                    # "qty_in": 0,
                    # "trx_out": {},
                    # "qty_out": 0
                }

        # HACK: Perhaps we need trx_in, trx_out, qty_in, qty_out for bubbles in the future

        # Update trx_out and qty_out to 'from', trx_in and qty_in to 'to'
        # nodes[from_address]['trx_out'] += float(row['value'])
        # nodes[from_address]['qty_out'] += 1
        # nodes[to_address]['trx_in'] += float(row['value'])
        # nodes[to_address]['qty_in'] += 1

        # Update trx_out
        # if symbol in nodes[from_address]['trx_out']:
        #     nodes[from_address]['trx_out'][symbol] += value
        # else:
        #     nodes[from_address]['trx_out'][symbol] = value
        # nodes[from_address]['qty_out'] += 1
        # Update trx_in
        # if symbol in nodes[to_address]['trx_in']:
        #     nodes[to_address]['trx_in'][symbol] += value
        # else:
        #     nodes[to_address]['trx_in'][symbol] = value
        # nodes[to_address]['qty_in'] += 1

        # # Links
        # link_key = f"{from_address}->{to_address}"
        # if link_key in links:
        #     links[link_key]["value"] += float(row["value"])
        #     links[link_key]["qty"] += 1
        # else:
        #     links[link_key] = {"source": from_address, "target": to_address, "value": float(row["value"]), "qty": 1}

        # Links
        link_key = f"{from_address}->{to_address}"
        if link_key not in links:
            links[link_key] = {"source": from_address, "target": to_address, "detail": {}, "qty": 0}
        if symbol in links[link_key]["detail"]:
            links[link_key]["detail"][symbol]["sum"] += value
            links[link_key]["detail"][symbol]["count"] += 1
        else:
            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
        links[link_key]["qty"] += 1


    nodes_list = list(nodes.values())
    links_list = list(links.values())

    transactions = {"nodes": nodes_list, "links": links_list}

    # list_trans = df_all[(df_all['from'] == address_central) | (df_all['to'] == address_central)].to_json(orient = "records")  # FIX: Add contractAddress ??
    list_trans = df_all.loc[(df_all["from"] == address_central) | (df_all["to"] == address_central)].to_json(orient = "records")

    stat_trx = len(df_all[df_all['type'] == 'transaction'])
    stat_int = len(df_all[df_all['type'] == 'internals'])
    stat_tra = len(df_all[df_all['type'] == 'transfers'])
    stat_tot = len(df_all)
    stat_wal = len(df_tags[df_tags['tag'] == 'wallet'])
    stat_con = len(df_tags[df_tags['tag'] == 'contract'])
    # TODO: Stat of wallets and contracts

    stat = {"stat_trx": int(stat_trx), "stat_int": int(stat_int), 
            "stat_tra": int(stat_tra), "stat_wal": int(stat_wal),
            "stat_tot": int(stat_tot), "stat_con": int(stat_con), 
            "stat_err": int(stat_err), "stat_coo": int(stat_con)}  # FIX: repeating stat_con in stat_coo

    return {"transactions": transactions, "list": list_trans, "stat": stat}


# TODO: REMOVE
# def get_trx_from_addresses_opt_bkp(conn, address_central):

#     # INFO: address_central is
#     # ('eth', '0x7f3acf451e372f517b45f9d2ee0e42e31bc5e53e', 'central')
#     # For future Multichain (?)

#     address_central = address_central[1]

#     nodes = []
#     nodes_list = []
#     links = []
#     stat_tot = 0
#     stat_trx = 0
#     stat_int = 0
#     stat_tra = 0
#     stat_err = 0
#     stat_con = 0
#     stat_wal = 0
#     # stat_coo = 0  # TODO: Contract owned
#     # max_qty = 0

#     # INFO: Tagging
#     df_tags = pd.read_sql_query("SELECT address, tag FROM t_tags", conn)
#     tags_grouped = df_tags.groupby('address')['tag'].apply(list).reset_index(name='tags')
#     tags_dict = pd.Series(tags_grouped.tags.values,index=tags_grouped.address).to_dict()

#     # INFO: Labels
#     df_labels = pd.read_sql_query("SELECT * FROM t_labels WHERE blockChain = 'ethereum'", conn)  # TODO: Multichain
#     labels_dict = df_labels.set_index('address').apply(lambda row: row.to_dict(), axis=1).to_dict()

#     # stat_coo = 
#     # stat_con = len(json_tags) - stat_coo

#     # TODO: 
#     # - Isolate contract creation trx
#     # - Join query with labels
#     # - More info in links

#     # INFO: Get all Trx, Transfers and internals
#     query = """
#         SELECT blockChain, type, hash, `from`, `to`, value, contractAddress, symbol, name, decimal, valConv, timeStamp, isError
#         FROM (
#             SELECT blockChain, 'transaction' as 'type', hash, `from`, `to`, value, 
#                 contractAddress, 'ETH' as 'symbol', 'Ether' as 'name', 18 as 'decimal', value / POWER(10, 18) as valConv, timeStamp, isError
#             FROM t_transactions
#             UNION ALL
#             SELECT blockChain, 'internals', hash, `from`, `to`, value, 
#                 contractAddress, 'ETH', 'Ether', 18, value / POWER(10, 18), timeStamp, isError
#             FROM t_internals
#             UNION ALL
#             SELECT blockChain, 'transfers', hash, `from`, `to`, value, 
#                 contractAddress, tokenSymbol, tokenName, tokenDecimal, value / POWER(10, tokenDecimal), timeStamp, 0
#             FROM t_transfers
#         ) AS combined
#         ORDER BY timeStamp ASC
#     """
#     df_all = pd.read_sql_query(query, conn)

#     # INFO: Convert to datetime
#     df_all['timeStamp'] = pd.to_datetime(df_all['timeStamp'], unit='s')
#     stat_err = len(df_all[df_all['isError'] != 0])
#     df_all = df_all[df_all['isError'] == 0]

#     nodes = {}
#     links = {}

#     for _, row in df_all.iterrows():
#         from_address = row['from']
#         to_address = row['to']

#         for address in [from_address, to_address]:
#             if address not in nodes:
#                 tag = tags_dict.get(address, [])  # Get tag
#                 label = labels_dict.get(address, [])  # Get label
#                 nodes[address] = {
#                     "id": address, 
#                     "address": address,
#                     # "tag": [tag] if tag else [],
#                     "tag": tag,
#                     "label": label,
#                     "token": "ETH",  # TODO: Multichain
#                     "trx_in": 0,
#                     "qty_in": 0,
#                     "trx_out": 0,
#                     "qty_out": 0
#                 }

#         # Update trx_out and qty_out to 'from', trx_in and qty_in to 'to'
#         nodes[from_address]['trx_out'] += float(row['value'])
#         nodes[from_address]['qty_out'] += 1
#         nodes[to_address]['trx_in'] += float(row['value'])
#         nodes[to_address]['qty_in'] += 1

#         # Links
#         link_key = f"{from_address}->{to_address}"
#         if link_key in links:
#             links[link_key]["value"] += float(row["value"])
#             links[link_key]["qty"] += 1
#         else:
#             links[link_key] = {"source": from_address, "target": to_address, "value": float(row["value"]), "qty": 1}

#     nodes_list = list(nodes.values())
#     links_list = list(links.values())

#     transactions = {"nodes": nodes_list, "links": links_list}

#     # print(df_all.info())
#     # print(df_all)
#     # print("=================================================================")
#     # print(address_central)
#     # list_trans = df_all[(df_all['from'] == address_central) | (df_all['to'] == address_central)].to_json(orient = "records")  # FIX: Add contractAddress ??
#     list_trans = df_all.loc[(df_all["from"] == address_central) | (df_all["to"] == address_central)].to_json(orient = "records")
#     # print(list_trans)

#     stat_trx = len(df_all[df_all['type'] == 'transaction'])
#     stat_int = len(df_all[df_all['type'] == 'internals'])
#     stat_tra = len(df_all[df_all['type'] == 'transfers'])
#     stat_tot = len(df_all)
#     stat_wal = len(df_tags[df_tags['tag'] == 'wallet'])
#     stat_con = len(df_tags[df_tags['tag'] == 'contract'])
#     # TODO: Stat of wallets and contracts

#     stat = {"stat_trx": int(stat_trx), "stat_int": int(stat_int), 
#             "stat_tra": int(stat_tra), "stat_wal": int(stat_wal),
#             "stat_tot": int(stat_tot), "stat_con": int(stat_con), 
#             "stat_err": int(stat_err), "stat_coo": int(stat_con)}  # FIX: repeating stat_con in stat_coo

#     return {"transactions": transactions, "list": list_trans, "stat": stat}


def get_balance_and_gas(conn, address_central, type, key):
    # INFO: address_central is
    # ('eth', '0x7f3acf451e372f517b45f9d2ee0e42e31bc5e53e', 'central')
    # For future Multichain (?)

    address_central = address_central[1]

    if (type == 'wallet'):
        # INFO: Get all Trx, Transfers and internals balance
        query = f"""
            SELECT 
                blockChain,
                '{address_central}',
                token,
                tokenName,
                SUM(
                    CASE 
                        WHEN "From" = '{address_central}' THEN -value / POWER(10, SymbolDecimal)
                        WHEN "To" = '{address_central}' THEN value / POWER(10, SymbolDecimal)
                        ELSE 0
                    END
                ) AS balance
            FROM 
                (
                SELECT blockChain, 'transaction', hash, `from`, `to`, value, contractAddress, 'ETH' as 'token', 'Ether' as 'tokenName', 18 as 'SymbolDecimal', timeStamp, isError
                FROM t_transactions
                UNION 
                SELECT blockChain, 'internals', hash, `from`, `to`, value, contractAddress, 'ETH',  'Ether', 18 as 'SymbolDecimal', timeStamp, isError
                FROM t_internals
                UNION 
                SELECT blockChain, 'transfers', hash, `from`, `to`, value, contractAddress, tokenSymbol, tokenName, tokenDecimal, timeStamp, 0
                FROM t_transfers
                ) AS balance_temp
            WHERE 
                "isError" = 0 AND
                ("From" = '{address_central}' OR "To" = '{address_central}')
            GROUP BY 
                token;
        """
        # df_balance = pd.read_sql_query(query, conn).to_json(orient = "records")
        df_balance = pd.read_sql_query(query, conn)

        # INFO: Get Gas
        query = f"""
            SELECT 
                SUM(gasPrice * gasUsed * 1e-18) as "Gas"
            FROM t_transactions
            WHERE 
                "isError" = 0 AND
                "From" = '{address_central}'
        """
        cursor = conn.cursor()
        cursor.execute(query)
        gas = cursor.fetchone()[0]

        # Get index of ETH row
        index = df_balance.index[df_balance['token'] == 'ETH'].tolist()
        balance = df_balance[df_balance['token'] == 'ETH']

        if (balance.loc[index[0], 'balance'] - gas) >= 0:
            balance.loc[index[0], 'balance'] -= gas
        else:
            balance.loc[index[0], 'balance'] = 0

        # balance.loc[index, 'balance'] -= gas
        # if (balance.loc[index, 'balance'] < 0):
        #     balance.loc[index, 'balance'] = 0.0
        balance = json.loads(balance[balance['token'] == 'ETH'].to_json(orient = "records"))

        # Remove row from tokens
        df_balance.drop(index, inplace=True)
        tokens = json.loads(df_balance.to_json(orient = "records"))

        return {"balance": balance, "tokens": tokens, "gas": gas}

    else:
        # INFO: Get balance of contract
        url = f"https://api.etherscan.io/api?module=account&action=balance&address={address_central}&tag=latest&apikey={key}"
        # print(f"KEY: {key}")
        response = requests.get(url)
        json_object = response.json()['result']
        # print(f"BALANCE: {json_object}")
        balance  = [{"blockChain": "eth", "balance": int(json_object) / 1e18, "token": "ETH", "tokenName": "Ether"}]
        # TODO: Use scrapping to get all of tokens balance

        return {"balance": balance, "tokens": [], "gas": []}


def get_founders_creators(conn, address_central):
    address_central = address_central[1]

    # INFO: Get Founders
    query = f"SELECT * FROM t_founders_creators WHERE `to` = '{address_central}';"
    df_founders_creators = pd.read_sql_query(query, conn).to_json(orient = "records")

    return {"founders": df_founders_creators}


def get_tags_labels(conn, address_central):
    address_central = address_central[1]

    # PERF: I think that need to remove the blockchain condition

    # INFO: Get Tags
    query = f"SELECT * FROM t_tags WHERE blockChain = 'eth' and address = '{address_central}';"
    df_tags = json.loads(pd.read_sql_query(query, conn).to_json(orient = "records"))

    # INFO: Get Labels
    query = f"SELECT * FROM t_labels WHERE blockChain = 'ethereum' and address = '{address_central}';"
    df_labels = json.loads(pd.read_sql_query(query, conn).to_json(orient = "records"))

    return {"tags": df_tags, "labels": df_labels}
