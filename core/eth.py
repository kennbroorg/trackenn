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
# from sqlalchemy import text
from datetime import datetime
from flask import jsonify

from termcolor import colored
import coloredlogs, logging

logger = logging.getLogger(__name__)
# logger.propagate = False  # INFO: To prevent duplicates with flask


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

    # INFO: Config Log Level
    log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
    coloredlogs.install(level=params['config']['level'], fmt=log_format, logger=logger)
    logger.propagate = False  # INFO: To prevent duplicates with flask

    logger.info(f"Getting information")
    data = json.dumps({"msg": f"Getting information", "end": False, "error": False, "content": {}})
    yield f"data:{data}\n\n"

    message = f"<strong>Blockchain</strong> - Ethereum"
    logger.info(message.replace('<strong>', '').replace('</strong>', ''))
    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
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

        # FIX: Eliminate chunks and block_to
        # # INFO: Get Source
        # if (params.get('source', '') == ''):
        #     source = 'central'
        # else:
        #     source = params.get('source', '')

        # FIX: Eliminate chunks and block_to
        # if (params.get('chunk', '') == ''):
        #     chunk = 100000  # TODO: Control the chunk size
        # else:
        #     chunk = params.get('chunk', '')

        # if (params.get('block_from', '') == ''):
        #     block_from = 0
        # else:
        #     block_from = params.get('block_from', '')

        # if (params.get('block_to', '') == ''):
        #     block_to = block_from + chunk
        # else:
        #     block_to = params.get('block_to', '')

        # TODO: Validate if user send trx reference

        # Checking wallet and first trx
        key = params['config']['ethscan']
        dbname = params['config']['dbname']

        connection = sqlite3.connect(dbname)
        cursor = connection.cursor()

        # INFO: Get blockchain param
        if (params.get('network', '') == '') or (params.get('network', '') == 'undefined'):
            # INFO: ERROR
            blockchain = ''
            connection.close()
            message = f"<strong>Error...</strong>"
            logger.error(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            message = f"<strong>Blockchain must be informed</strong>"
            logger.error(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            message = f" "
            logger.error(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
            yield f"data:{data}\n\n"
            raise Exception("Blockchain must be informed")
        else:
            blockchain = params['network']
        logger.debug(f"Blockchain: {blockchain}")

        # INFO: Warning message about API
        if (params['config']['ethscan'] == '') or (params['config']['ethscan'] == 'XXX'):
            message = f"<strong>Etherscan.io api key possibly unconfigured</strong>"
            logger.error(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

        # INFO: Update central address
        query = f"SELECT * FROM t_tags WHERE tag = 'central'"
        cursor.execute(query)
        central = cursor.fetchall()
        logger.debug(f"Central address: {central}")
        if (central):
            update = f"UPDATE t_tags SET blockChain = '{blockchain}', address = '{address}' WHERE tag = 'central'"
            connection.execute(update)
            logger.debug(f"UPDATE Central: {update}")
            connection.commit()
        else:
            insert = f"INSERT INTO t_tags (blockChain, address, tag) VALUES ('{blockchain}', '{address}', 'central')"
            connection.execute(insert)
            logger.debug(f"INSERT Central: {insert}")
            connection.commit()
        
        # INFO: Update path address
        insert = f"INSERT OR IGNORE INTO t_tags (blockChain, address, tag) VALUES ('{blockchain}', '{address}', 'path')"
        connection.execute(insert)
        logger.debug(f"INSERT Path: {insert}")
        connection.commit()

        # INFO: Verify info of address
        query = f"SELECT * FROM t_address_detail WHERE address = '{address}' AND blockChain = '{blockchain}'"
        logger.debug(f"SELECT address_detail: {query}")
        cursor.execute(query)
        wallet_detail = cursor.fetchone()
        json_object = []
        json_internals = []
        json_transfers = []
        json_nfts = []
        json_multitokens = []

        logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")
        logger.debug(f" Wallet detail: {wallet_detail}")
        logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")

        # INFO: Get information 
        if (wallet_detail == []) or (wallet_detail == None):
            # INFO: Verify if address is contract
            type = 'wallet'
            contract_creation = {}
            try:
                url = f"https://api.etherscan.io/api?module=contract&action=getcontractcreation&contractaddresses={address}&apikey={key}"
                response = requests.get(url)
                contract_creation = response.json()
                json_status = response.json()['status']
                json_message = response.json()['message']
                json_result = response.json()['result']

                if (json_message == "NOTOK"):
                    message = f"<strong>Error...</strong>"
                    logger.error(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    message = f"<strong>{json_result}</strong>"
                    logger.error(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    message = f" "
                    logger.error(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"
                    raise Exception(json_result)
                elif (json_status == "1"):
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
                    first = json_object[0].copy()
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
                    df_trx_store['blockChain'] = blockchain
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
                    contract_tagging.append((blockchain, address, 'contract'))
                    # print(f"Contract creation: {contract_creation}")
                    # print(f"Contract creator: {contract_creation['result'][0]['contractCreator']}")
                    # contract_tagging.append({'blockChain': 'eth', 'address': contract_creation['result'][0]['contractCreator'], 'tag': 'contract creator'})
                    contract_tagging.append((blockchain, contract_creation['result'][0]['contractCreator'], 'contract creator'))
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
                    insert_creator = """INSERT OR IGNORE INTO t_funders_creators 
                        (blockChain, blockNumber, type, timeStamp, hash, `from`, `to`, value, input, contractAddress, tokenDecimal, tokenSymbol, tokenName) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                    creator_row = [(blockchain,
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
                    df_trx_store['blockChain'] = blockchain
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
                        df_internals_store['blockChain'] = blockchain
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
                        df_transfers_store['blockChain'] = blockchain
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
                        df_nfts_store['blockChain'] = blockchain
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
                        df_multitoken_store['blockChain'] = blockchain
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
            # HACK: This is not a good place to continue collecting information if it is not complete
            logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logger.debug(f"+ Already collected                                +")
            logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")

            # TODO: Add contract validation (Read the HACK comment)

            # Validate if all trxs are collected
            query = f"SELECT all_data FROM t_blocks WHERE address = '{address}';"
            cursor.execute(query)
            all_data_collected = cursor.fetchone()

            # INFO: No more info to collect

            # if (not all_data_collected[0]): 

            #     # Get last block number
            #     query = f"SELECT rowid, * FROM t_blocks WHERE address = '{address}' ORDER BY block_to DESC LIMIT 1;"
            #     cursor.execute(query)
            #     row = cursor.fetchone()
            #     last_block = int(row[4]) + 1
            #     rowid = int(row[0])

            #     logger.debug(f"Last block: {last_block}")
            #     
            #     # Get more blocks from last block
            #     try:
            #         url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock={last_block}&endblock=99999999&sort=asc&apikey={key}"
            #         # print(url)
            #         response = requests.get(url)
            #         json_object = response.json()['result']

            #         if (len(json_object) > 0):
            #             message = f"<strong>TRANSACTIONS</strong> - Get more transactions"
            #             logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            #             data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
            #             yield f"data:{data}\n\n"
            #         else:
            #             message = f"<strong>TRANSACTIONS<strong> - More transactions <strong>NOT FOUND</strong>"
            #             logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            #             data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
            #             yield f"data:{data}\n\n"

            #     except Exception:
            #         traceback.print_exc()
            #         traceback_text = traceback.format_exc()

            #         connection.close()
            #         message = f"<strong>Error...</strong>"
            #         logger.warning(f"{message}")
            #         data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            #         yield f"data:{data}\n\n"

            #         for line in traceback_text.splitlines():
            #             message = f"{line}"
            #             logger.warning(f"{message}")
            #             data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            #             yield f"data:{data}\n\n"

            #         message = f" "
            #         logger.warning(f"{message}")
            #         data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
            #         yield f"data:{data}\n\n"

            #     # Store transaction in DB
            #     try:
            #         message = f"<strong>TRANSACTIONS</strong> - Storing..."
            #         logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            #         data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
            #         yield f"data:{data}\n\n"

            #         db_store_transactions(connection, json_object)
            #     except Exception:
            #         traceback.print_exc()
            #         traceback_text = traceback.format_exc()

            #         connection.close()
            #         message = f"<strong>Error...</strong>"
            #         logger.error(message.replace('<strong>', '').replace('</strong>', ''))
            #         logger.warning(f"{message}")
            #         data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            #         yield f"data:{data}\n\n"

            #         for line in traceback_text.splitlines():
            #             message = f"{line}"
            #             logger.warning(f"{message}")
            #             data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            #             yield f"data:{data}\n\n"

            #         message = f" "
            #         logger.warning(f"{message}")
            #         data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
            #         yield f"data:{data}\n\n"

            #     # Get new last block and date
            #     new_last_block = json_object[-1]['blockNumber']
            #     new_timeStamp_to = json_object[-1]['timeStamp']

            #     logger.debug(f"New Last block: {json_object[-1]}")
            #     logger.debug(f"New Last block number: {new_last_block}")

            #     # Update last block
            #     update = f"UPDATE t_blocks SET block_to = {new_last_block}, date_to = {new_timeStamp_to} WHERE rowid = {rowid}"
            #     cursor.execute(update)
            #     logger.debug(f"UPDATE: {update}")
            #     connection.commit()

            #     # TODO: Store transfer in DB
            #     # TODO: Store internal in DB
            #     # TODO: Generating internals tags

        # INFO: Send wallet detail information
        query = f"SELECT * FROM t_address_detail WHERE address = '{address}' AND blockChain = '{blockchain}'"
        cursor.execute(query)
        wallet_detail = cursor.fetchall()

        logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")
        logger.debug(f" !!!!!! Wallet detail: {wallet_detail}")
        logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")

        message = f"<strong>TRANSACTIONS</strong> - Receiving wallet detail information..."
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"wallet_detail": wallet_detail}})
        yield f"data:{data}\n\n"

        # INFO: Get address and path
        query = f"SELECT * FROM t_tags WHERE tag = 'central'" # INFO: Here no need blockchain, only one central must exist
        cursor.execute(query)
        address = cursor.fetchone()
        # print(f"ADDRESS: {address}")
        type = wallet_detail[0][12]

        # TODO: Path must support multichain in the future
        query = f"SELECT address FROM t_tags WHERE tag = 'path' AND blockChain = '{blockchain}'"
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
        if (params['graph'] == "Complex"):
            trxs = get_trx_from_addresses_experimental(connection, address, params)
        else:
            trxs = get_trx_from_addresses_opt(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Proccesed...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                           "content": {"graph": trxs['transactions'], "list": trxs['list'], "stat": trxs['stat']}})
        yield f"data:{data}\n\n"

        # INFO: Get Funders and creators
        tic = time.perf_counter()
        funders = get_funders_creators(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Funders and creators...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"funders": funders}})
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
    data = get_trx_from_addresses_experimental(connection, address, params=params)
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
    # data = get_funders_creators(connection, address)
    data = get_trx_from_addresses_research(connection, address, params=params)
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
    df_addresses['blockChain'] = 'eth'
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

    # INFO: Funders or creators
    # TODO: Evaluate type to distinguish Funders and creators
    # df_all = pd.concat([df[['from', 'to', 'timeStamp']] for df in [df_t, df_f, df_i] if 'timeStamp' in df.columns], ignore_index=True).sort_values(by='timeStamp')
    df_all = pd.concat([df[['from', 'to', 'timeStamp']] for df in [df_t, df_f, df_i, df_n, df_m] if 'timeStamp' in df.columns], ignore_index=True).sort_values(by='timeStamp')
    min_timestamp = df_all.loc[df_all['from'] == address, 'timeStamp'].min()
    funders_addresses = df_all.loc[df_all['timeStamp'] < min_timestamp, 'from'].unique()

    df_funders = pd.DataFrame(funders_addresses, columns=['address'])
    df_funders['blockChain'] = 'eth'
    df_funders['tag'] = 'funder'

    # Unifying tags
    df_addresses = pd.concat([df_addresses, df_funders]).drop_duplicates().reset_index(drop=True)

    # Insert tags in SQLite
    cursor = connection.cursor()
    insert_tag = 'INSERT OR IGNORE INTO t_tags (address, blockChain, tag) VALUES (?, ?, ?)'
    cursor.executemany(insert_tag, df_addresses.to_records(index=False))
    connection.commit()

    # Store trxs Funders
    funders = df_funders['address'].tolist()
    df_t_f = df_t.loc[(df_t['to'] == address) & 
                      (df_t['from'].isin(funders)) & 
                      (df_t['timeStamp'] < min_timestamp)].assign(
                              type='transaction').assign(blockChain='eth').assign(tokenDecimal=18).assign(tokenSymbol='ETH').assign(tokenName='Ether')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    df_i_f = df_i.loc[(df_i['to'] == address) & 
                      (df_i['from'].isin(funders)) & 
                      (df_i['timeStamp'] < min_timestamp)].assign(
                              type='internal').assign(blockChain='eth').assign(tokenDecimal=18).assign(tokenSymbol='ETH').assign(tokenName='Ether')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    df_f_f = df_f.loc[(df_f['to'] == address) & 
                      (df_f['from'].isin(funders)) & 
                      (df_f['timeStamp'] < min_timestamp)].assign(
                              type='transfer').assign(blockChain='eth')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    # INFO: Neccesary rename to avoid add columns to funders
    df_n.rename(columns={"tokenID": "value"}, inplace=True)
    df_n_f = df_n.loc[(df_n['to'] == address) & 
                      (df_n['from'].isin(funders)) & 
                      (df_n['timeStamp'] < min_timestamp)].assign(
                              type='nfts').assign(blockChain='eth')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]
    # INFO: Neccesary rename to avoid add columns to funders
    df_m.rename(columns={"tokenID": "value"}, inplace=True)
    df_m.rename(columns={"tokenValue": "tokenDecimal"}, inplace=True)
    df_m_f = df_m.loc[(df_m['to'] == address) & 
                      (df_m['from'].isin(funders)) & 
                      (df_m['timeStamp'] < min_timestamp)].assign(
                              type='nfts').assign(blockChain='eth')[[
                                  'blockChain', 'blockNumber', 'type', 'timeStamp','hash', 'from', 'to', 
                                  'value', 'input', 'contractAddress', 'tokenDecimal', 'tokenSymbol', 'tokenName']]

    df_funders_trxs = pd.concat([df_t_f, df_i_f, df_f_f, df_n_f, df_m_f])

    # Insert tags in SQLite
    cursor = connection.cursor()
    insert_funders = """INSERT OR IGNORE INTO t_funders_creators 
        (blockChain, blockNumber, type, timeStamp, hash, `from`, `to`, value, input, contractAddress, tokenDecimal, tokenSymbol, tokenName) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
    cursor.executemany(insert_funders, df_funders_trxs.to_records(index=False))
    connection.commit()


def get_trx_from_addresses_opt(conn, address_central):

    # INFO: address_central is
    # ('eth', '0x7f3acf451e372f517b45f9d2ee0e42e31bc5e53e', 'central')
    # For future Multichain (?)

    logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug(f"+ Address: {address_central}")
    logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")

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
                stat_wal += 'wallet' in tag
                stat_con += 'contract' in tag
                label = labels_dict.get(address, [])  # Get label
                nodes[address] = {
                    "id": address, 
                    "address": address,
                    # "tag": [tag] if tag else [],
                    "tag": tag,
                    "label": label,
                    "blockchain": "eth",
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
    # stat_wal = len(df_tags[df_tags['tag'] == 'wallet'])
    # stat_con = len(df_tags[df_tags['tag'] == 'contract'])
    # TODO: Stat of wallets and contracts

    stat = {"stat_trx": int(stat_trx), "stat_int": int(stat_int), 
            "stat_tra": int(stat_tra), "stat_wal": int(stat_wal),
            "stat_tot": int(stat_tot), "stat_con": int(stat_con), 
            "stat_err": int(stat_err), "stat_coo": int(stat_con)}  # FIX: repeating stat_con in stat_coo

    return {"transactions": transactions, "list": list_trans, "stat": stat}


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


def get_funders_creators(conn, address_central):
    address_central = address_central[1]

    # INFO: Get Funders
    query = f"SELECT * FROM t_funders_creators WHERE `to` = '{address_central}';"
    df_funders_creators = pd.read_sql_query(query, conn).to_json(orient = "records")

    return {"funders": df_funders_creators}


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


# TODO: 
# - [X] Group transaction, internals, transfers, nfts, multitokens by hash
# - [X] Separate complete trxs from incomplete one
# - [X] Count isError
# - [X] Count different type
# - [ ] Collect functionName and store in nodes in form of list
#         In node or link???
# - [ ] Implement incomplete transactions
# - [ ] Do we show the token contract? Or do we embed it in the link as a url?
# - [ ] Normalize all debug messages
# - [ ] Document each case (draw)
# - [ ] Port this function to bsc.py

# PERF:
# - [ ] After documenting and drawing each case, seek to unify cases
# - [ ] Move node and link creation to the part following store
#         Pay special attention not to store duplicates
# - [ ] Store preprocessed information in temp tables

def get_trx_from_addresses_experimental(conn, address_central, params=[]):

    # INFO: Config Log Level
    if params:
        log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
        coloredlogs.install(level=params['config']['level'], fmt=log_format, logger=logger)
        logger.propagate = False  # INFO: To prevent duplicates with flask

    logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug(f"+ Address: {address_central}")
    logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")

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

    # INFO: Tagging: Here exclude wallets and contracts
    df_tags = pd.read_sql_query("SELECT address, tag FROM t_tags WHERE tag NOT IN ('wallet', 'contract')", conn)
    tags_grouped = df_tags.groupby('address')['tag'].apply(list).reset_index(name='tags')
    tags_dict = pd.Series(tags_grouped.tags.values,index=tags_grouped.address).to_dict()

    # INFO: Labels
    df_labels = pd.read_sql_query("SELECT * FROM t_labels WHERE blockChain = 'ethereum'", conn)
    labels_dict = df_labels.set_index('address').apply(lambda row: row.to_dict(), axis=1).to_dict()

    # INFO: Get all Trx, Transfers, internals, nfts and multitoken
    query = """
        SELECT blockChain, type, hash, `from`, `to`, value, contractAddress, symbol, name, decimal, valConv, timeStamp, isError, methodId, functionName
        FROM (
            SELECT blockChain, 'transaction' as 'type', hash, `from`, `to`, value, contractAddress, 'ETH' as 'symbol', 
			'Ether' as 'name', 18 as 'decimal', value / POWER(10, 18) as valConv, timeStamp, isError, methodId, functionName
            FROM t_transactions
            UNION ALL
            SELECT blockChain, 'internals', hash, `from`, `to`, value, 
                contractAddress, 'ETH', 'Ether', 18, value / POWER(10, 18), timeStamp, isError, '0x', ''
            FROM t_internals
            UNION ALL
            SELECT blockChain, 'transfers', hash, `from`, `to`, value,
                contractAddress, tokenSymbol, tokenName, tokenDecimal, value / POWER(10, tokenDecimal), timeStamp, 0, '0x', ''
            FROM t_transfers
            UNION ALL
            SELECT blockChain, 'nfts', hash, `from`, `to`, tokenID,
                contractAddress, tokenSymbol, tokenName, tokenDecimal, tokenID, timeStamp, 0, '0x', ''
            FROM t_nfts
            UNION ALL
            SELECT blockChain, 'multitoken', hash, `from`, `to`, tokenID,
                contractAddress, tokenSymbol, tokenName, tokenValue, tokenID, timeStamp, 0, '0x', ''
            FROM t_multitoken
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

    # for index, row in df_all.iterrows():
    grouped = df_all.groupby('hash')

    count = 0
    for hash, group in grouped:

        group_size = len(group)
        has_transaction = 'transaction' in group['type'].values

        # FIX: Remove after research
        # INFO: Piece of code for classification
        # print(f"TYPE: {type}")
        # print(f"XFROM: {xfrom_address}")
        # print(f"XTO: {xto_address}")
        # print(f"XVALUE: {xvalue}")
        # print(f"XSYMBOL: {xsymbol}")
        # print(f"FROM: {from_address}")
        # print(f"TO: {to_address}")
        # print(f"CONTRACT ADDRESS: {row['contractAddress']}")
        # print(f"VALUE: {value}")
        # print(f"SYMBOL: {symbol}")

        # INFO: Complete transaction and pure =======================================
        if (group_size == 1) and (has_transaction):
            logger.debug(f"== SIMPLE TRANSACTION ============================")
            # TODO: Verify that transaction with functionName is complete

            for _, row in group.iterrows():
                from_address = row['from']
                to_address = row['to']
                symbol = row['symbol']
                value = float(row['valConv'])
                # Nodes
                if from_address not in nodes:
                    stat_wal += 1
                    # PERF: Improve
                    tag = tags_dict.get(from_address, [])  # Get tag
                    tag.append('wallet')
                    label = labels_dict.get(from_address, [])  # Get label
                    nodes[from_address] = {
                        "id": from_address, 
                        "address": from_address,
                        "tag": tag,
                        "label": label,
                    }

                if to_address not in nodes:
                    # PERF: Improve
                    tag = tags_dict.get(to_address, [])  # Get tag
                    if (row['functionName'] != ''):
                        tag.append('contract')
                        stat_con += 1
                    else: 
                        tag.append('wallet')
                        stat_wal += 1

                    label = labels_dict.get(to_address, [])  # Get label
                    nodes[to_address] = {
                        "id": to_address, 
                        "address": to_address,
                        "tag": tag,
                        "label": label,
                    }

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

        # INFO: Complete transaction with one transfer (ERC20, nft, Multitoken) ======
        elif (group_size == 2) and (has_transaction):
            count += 1
            logger.debug(f"== COMPLETE GROUP SIZE 2 =========================")
            print(f"Hash: {hash}")
            xfrom_address = xto_address = xsymbol = ''
            xvalue = 0
            for _, row in group.iterrows():
                # INFO: x_from and z_from are wallets and the same 
                type = row['type']
                from_address = row['from']
                to_address = row['to']
                symbol = row['symbol']
                value = float(row['valConv'])
                # print(f"TYPE: {type}")
                # print(f"FROM: {from_address}")
                # print(f"TO: {to_address}")
                # print(f"CONTRACT ADDRESS: {row['contractAddress']}")
                # print(f"VALUE: {value}")
                # print(f"SYMBOL: {symbol}")
                # Nodes
                # INFO: Transaction
                if (type == 'transaction'):
                    xfrom_address = from_address
                    xto_address = to_address
                    xvalue = value
                    xsymbol = symbol
                    if from_address not in nodes:
                        stat_wal += 1
                        # PERF: Improve
                        tag = tags_dict.get(from_address, [])  # Get tag
                        tag.append('wallet')
                        label = labels_dict.get(from_address, [])  # Get label
                        nodes[from_address] = {
                            "id": from_address, 
                            "address": from_address,
                            "tag": tag,
                            "label": label,
                        }
                    if to_address not in nodes:
                        stat_con += 1
                        # PERF: Improve
                        tag = tags_dict.get(to_address, [])  # Get tag
                        tag.append('contract')
                        label = labels_dict.get(to_address, [])  # Get label
                        nodes[to_address] = {
                            "id": to_address, 
                            "address": to_address,
                            "tag": tag,
                            "label": label,
                        }

                # INFO: Internals
                if (type == 'internals'):
                    contract_address = row['contractAddress']

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (xfrom_address == to_address): # REF: hash: 0xfe45d513dc4fc8fb844f7a4b4375b15e3e7ac0a1923fb8e9cf70b6428968a408
                        logger.debug(f"INTERNAL FORM 74 - INTERNAL RECEIVE")
                        if from_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(from_address, [])  # Get label
                            nodes[from_address] = {
                                "id": from_address, 
                                "address": from_address,
                                "tag": tag,
                                "label": label,
                            }

                        # Links for internal receive
                        link_key = f"{from_address}->{to_address}"
                        logger.warning(f"INTERNAL LINK - RECEIVE INTERNAL ETHER {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": from_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"INTERNALS NOT TYPIFIED")

                # INFO: Transfers
                if (type == 'transfers'):
                    contract_address = row['contractAddress']

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (from_address == xfrom_address) and (to_address == xto_address): # REF: hash : 0x19fb9697dd7f42ba531d7ef9c07c7bf2a859111ce2915512bfadeb3c4badc344 deposit to contract
                        logger.debug(f"FORM 3 (from_address == xfrom_address) and (to_address == xto_address)")
                        if contract_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(contract_address, [])  # Get label
                            nodes[contract_address] = {
                                "id": contract_address, 
                                "address": contract_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links to contract
                        link_key = f"{xfrom_address}->{contract_address}"
                        # logger.warning(f"LINK TRANSACTION {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": contract_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1
                        # Links from contract
                        link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"LINK TRANSFER {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    elif (from_address == xfrom_address) and (contract_address == xto_address): # REF: hash: 0xf01325b1b4c10b4cbe86c94cf65e459dde587b86bcdcbe50beaa8fa94df4b7e8 
                        logger.debug(f"FORM 4 (from_address == xfrom_address) and (contract_address == xto_address) - Transfer ERC20 to wallet") 
                        if to_address not in nodes:
                            stat_wal += 1
                            # PERF: Improve
                            tag = tags_dict.get(to_address, [])  # Get tag
                            tag.append('wallet')
                            label = labels_dict.get(to_address, [])  # Get label
                            nodes[to_address] = {
                                "id": to_address, 
                                "address": to_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links for transaction xfrom and xto (contract)
                        link_key = f"{xfrom_address}->{xto_address}"
                        # logger.warning(f"LINK TRANSACTION {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links from contract to wallet
                        link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"LINK TRANSFER {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    elif (xfrom_address == to_address) and (xto_address == from_address): # REF: hash: 0xa54c6cacc1661abef5adf16d4227e5f5827519ed1038043882b0cf11e4299085
                        logger.debug(f" FORM 5 (xfrom_address == to_address) and (xto_address == from_address)")
                        if contract_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(contract_address, [])  # Get label
                            nodes[contract_address] = {
                                "id": contract_address, 
                                "address": contract_address,
                                "tag": tag,
                                "label": label,
                            }

                        # Links for transaction xfrom and xto
                        link_key = f"{from_address}->{contract_address}"
                        # logger.warning(f"LINK 1 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": from_address, "target": contract_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for FORM trx xfrom == zfrom and one transfer
                        link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"LINK 2 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    # FIXME: Review
                    elif (xfrom_address == to_address) and (xto_address != from_address != contract_address):  # REF: hash: 0xb4aba176a2feebbc7f892a434b9ddb23fc074fc15cbba1fe8b13ca210f816335
                        logger.debug(f" FORM 6 (xfrom_address == to_address) and (xto_address != from_address != contract_address) - Buy token ERC20 through ETH")
                        for address in [from_address, contract_address]:
                            if address not in nodes:
                                stat_con += 1
                                # PERF: Improve
                                tag = tags_dict.get(address, [])  # Get tag
                                tag.append('contract')
                                label = labels_dict.get(address, [])  # Get label
                                nodes[address] = {
                                    "id": address, 
                                    "address": address,
                                    "tag": tag,
                                    "label": label,
                                }

                        # Links for transaction xfrom and xto
                        link_key = f"{xfrom_address}->{xto_address}"
                        # logger.warning(f"LINK 1 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                        if xsymbol in links[link_key]["detail"]:
                            links[link_key]["detail"][xsymbol]["sum"] += xvalue
                            links[link_key]["detail"][xsymbol]["count"] += 1
                        else:
                            links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for transfer xto and from_address
                        link_key = f"{xto_address}->{from_address}"
                        # logger.warning(f"LINK 2 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                        if xsymbol in links[link_key]["detail"]:
                            links[link_key]["detail"][xsymbol]["sum"] += xvalue
                            links[link_key]["detail"][xsymbol]["count"] += 1
                        else:
                            links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                        links[link_key]["qty"] += 1

                        # # Links for transfer from_address and contract_address 
                        # link_key = f"{from_address}->{contract_address}" # xto_address is a contract
                        # # logger.warning(f"LINK 3 {link_key}")
                        # if link_key not in links:
                        #     links[link_key] = {"source": from_address, "target": contract_address, "detail": {}, "qty": 0}
                        # if symbol in links[link_key]["detail"]:
                        #     links[link_key]["detail"][symbol]["sum"] += value
                        #     links[link_key]["detail"][symbol]["count"] += 1
                        # else:
                        #     links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        # links[link_key]["qty"] += 1

                        # Links for transfer contract_address and to_address 
                        link_key = f"{from_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"LINK 4 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"ERC20 TRANSFER NOT TYPIFIED")

                # INFO: nfts
                if (type == 'nfts'):
                    contract_address = row['contractAddress']

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (xfrom_address == to_address) and (xto_address == contract_address) and \
                         (from_address == "0x0000000000000000000000000000000000000000"): # REF: hash : 0xf758e2b6a9ed20f3431ef36f57493cf2d6b81e4bc6e2e21921c690bc81b907dd
                        logger.debug(f"NFTS FORM 3 (xfrom_address == to_address) and (xto_address == contract_address) and (from_address == 0x00..) - Mint of NFT")
                        # if contract_address not in nodes:  # INFO: This isn't neccesary
                        #     stat_con += 1
                        #     tag = tags_dict.get(contract_address, [])  # Get tag
                        #     tag.append('contract')
                        #     label = labels_dict.get(contract_address, [])  # Get label
                        #     nodes[contract_address] = {
                        #         "id": contract_address, 
                        #         "address": contract_address,
                        #         "tag": tag,
                        #         "label": label,
                        #     }
                        # Links from contract to to_address (This is a mint of NFT)
                        link_key = f"{contract_address}->{to_address}"
                        # logger.warning(f"LINK NFT MINT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (from_address == xfrom_address) and (contract_address == xto_address): # REF: hash: 0xed609d5d6ccbd68138d8733da0c2711c102265fb8d47c9e4408139e794072913
                        logger.debug(f"NFTS FORM 4 (from_address == xfrom_address) and (contract_address == xto_address) - Transfer NFT ->") 
                        if to_address not in nodes:
                            stat_wal += 1
                            # PERF: Improve
                            tag = tags_dict.get(to_address, [])  # Get tag
                            tag.append('wallet')
                            label = labels_dict.get(to_address, [])  # Get label
                            nodes[to_address] = {
                                "id": to_address, 
                                "address": to_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links from wallet (xfrom_address to contract (or xto_address)
                        link_key = f"{xfrom_address}->{xto_address}"
                        # logger.warning(f"LINK TRANSFER NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                        # Links from contract (xto_address) to wallet to_address
                        link_key = f"{contract_address}->{to_address}"
                        # logger.warning(f"LINK TRANSFER NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (xfrom_address == to_address) and (xto_address != from_address != contract_address):  # REF: hash: 0xea91b02882a88fe588054fc2c2dbdaff8c24a5a9d30588512595ad8016aa91bd
                        logger.debug(f"NFTS FORM 5 (xfrom_address == to_address) and (xto_address != from_address != contract_address) <- Buy NFT to another wallet")
                        if from_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(from_address, [])  # Get tag
                            tag.append('wallet')
                            label = labels_dict.get(from_address, [])  # Get label
                            nodes[from_address] = {
                                "id": from_address, 
                                "address": from_address,
                                "tag": tag,
                                "label": label,
                            }
                        if contract_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(contract_address, [])  # Get label
                            nodes[contract_address] = {
                                "id": contract_address, 
                                "address": contract_address,
                                "tag": tag,
                                "label": label,
                            }

                        # Links wallet buy NFT from DEX
                        link_key = f"{xfrom_address}->{xto_address}"
                        # logger.warning(f"NFT LINK - BUY NFT FROM DEX {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                        if xsymbol in links[link_key]["detail"]:
                            links[link_key]["detail"][xsymbol]["sum"] += xvalue
                            links[link_key]["detail"][xsymbol]["count"] += 1
                        else:
                            links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for transfer xto and from_address
                        link_key = f"{xto_address}->{from_address}"
                        # logger.warning(f"NFT LINK - DEX TRANSFER ETH TO WALLET {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                        if xsymbol in links[link_key]["detail"]:
                            links[link_key]["detail"][xsymbol]["sum"] += xvalue
                            links[link_key]["detail"][xsymbol]["count"] += 1
                        else:
                            links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for transfer from_address and contract_address  # HACK: This could be hide in a url inside the merge this with the next link
                        link_key = f"{from_address}->{contract_address}"
                        # logger.warning(f"NFT LINK - from wallet through contract {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                        # Links for transfer contract_address and to_address 
                        link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"NFT LINK - FROM CONTRACT TRANSFER NFT TO WALLET {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (from_address == xfrom_address) and (contract_address != xto_address): # REF: hash: 0x6beb31076d54d69f11fa2bf69faf50c6679a43c6e46932406e914d3995b509ae
                        logger.debug(f"NFTS FORM 104 (from_address == xfrom_address) and (contract_address != xto_address) - Transfer NFT ->") 
                        if to_address not in nodes:
                            stat_wal += 1
                            # PERF: Improve
                            tag = tags_dict.get(to_address, [])  # Get tag
                            tag.append('wallet')
                            label = labels_dict.get(to_address, [])  # Get label
                            nodes[to_address] = {
                                "id": to_address, 
                                "address": to_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links from wallet to wallet of NFT
                        link_key = f"{xfrom_address}->{xto_address}"
                        logger.warning(f"LINK TRANSFER NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                        # Links from contract (xto_address) to wallet to_address
                        link_key = f"{contract_address}->{to_address}"
                        # logger.warning(f"LINK TRANSFER NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"NFTS TRANSFER NOT TYPIFIED")

                # INFO: Multitoken
                if (type == 'multitoken'):
                    contract_address = row['contractAddress']
                    # print(f"TYPE: {type}")
                    # print(f"XFROM: {xfrom_address}")
                    # print(f"XTO: {xto_address}")
                    # print(f"XVALUE: {xvalue}")
                    # print(f"XSYMBOL: {xsymbol}")
                    # print(f"FROM: {from_address}")
                    # print(f"TO: {to_address}")
                    # print(f"CONTRACT ADDRESS: {row['contractAddress']}")
                    # print(f"VALUE: {value}")
                    # print(f"SYMBOL: {symbol}")

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (xfrom_address == to_address) and (xto_address == contract_address) and \
                         (from_address == "0x0000000000000000000000000000000000000000"): # REF: hash : 0xc86c7f7fa143321e574bfbd99b07681dde16e003042982c9f12ee918a0776930
                        logger.debug(f"NFTS FORM 3 (xfrom_address == to_address) and (xto_address == contract_address) and (from_address == 0x00..) - Mint of NFT in multitoken")
                        # if contract_address not in nodes:  # INFO: This isn't neccesary
                        #     stat_con += 1
                        #     tag = tags_dict.get(contract_address, [])  # Get tag
                        #     tag.append('contract')
                        #     label = labels_dict.get(contract_address, [])  # Get label
                        #     nodes[contract_address] = {
                        #         "id": contract_address, 
                        #         "address": contract_address,
                        #         "tag": tag,
                        #         "label": label,
                        #     }
                        # Links from contract to to_address (This is a mint of NFT)
                        link_key = f"{contract_address}->{to_address}"
                        # logger.warning(f"LINK MULTITOKEN - MINT NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "multitoken"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (xfrom_address == from_address) and (xto_address == contract_address) and \
                         (to_address == "0x0000000000000000000000000000000000000000"): # REF: hash : 0x8bbf660270e17ecc774be01d65d63e68a6480f8a102e684bf928e1d3c88d5570
                        logger.debug(f"NFTS FORM 4 (xfrom_address == from_address) and (xto_address == contract_address) and (to_address == 0x00..) - Burn of NFT in multitoken")
                        # if contract_address not in nodes:  # INFO: This isn't neccesary
                        #     stat_con += 1
                        #     tag = tags_dict.get(contract_address, [])  # Get tag
                        #     tag.append('contract')
                        #     label = labels_dict.get(contract_address, [])  # Get label
                        #     nodes[contract_address] = {
                        #         "id": contract_address, 
                        #         "address": contract_address,
                        #         "tag": tag,
                        #         "label": label,
                        #     }
                        # Links from contract to to_address (This is a mint of NFT)
                        link_key = f"{from_address}->{contract_address}"
                        # logger.warning(f"LINK MULTITOKEN - BURN NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": from_address, "target": contract_address, "detail": {"type": "multitoken"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (xfrom_address == to_address) and (xto_address != from_address != contract_address):  # REF: hash: 0x7dcef56e5ea86475bc24ae59387c278b56ed3432ca39d68d67f150e61266bd6d
                        logger.debug(f"NFTS FORM 5 (xfrom_address == to_address) and (xto_address != from_address != contract_address) <- Buy NFT to another wallet")
                        if from_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(from_address, [])  # Get tag
                            tag.append('wallet')
                            label = labels_dict.get(from_address, [])  # Get label
                            nodes[from_address] = {
                                "id": from_address, 
                                "address": from_address,
                                "tag": tag,
                                "label": label,
                            }
                        if contract_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(contract_address, [])  # Get label
                            nodes[contract_address] = {
                                "id": contract_address, 
                                "address": contract_address,
                                "tag": tag,
                                "label": label,
                            }

                        # Links wallet buy NFT from DEX
                        link_key = f"{xfrom_address}->{xto_address}"
                        # logger.warning(f"MULTITOKEN LINK - BUY NFT FROM DEX {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                        if xsymbol in links[link_key]["detail"]:
                            links[link_key]["detail"][xsymbol]["sum"] += xvalue
                            links[link_key]["detail"][xsymbol]["count"] += 1
                        else:
                            links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for transfer xto and from_address
                        link_key = f"{xto_address}->{from_address}"
                        # logger.warning(f"MULTITOKEN LINK - DEX TRANSFER ETH TO WALLET {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                        if xsymbol in links[link_key]["detail"]:
                            links[link_key]["detail"][xsymbol]["sum"] += xvalue
                            links[link_key]["detail"][xsymbol]["count"] += 1
                        else:
                            links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for transfer from_address and contract_address  # HACK: This could be hide in a url inside the merge this with the next link
                        link_key = f"{from_address}->{contract_address}"
                        # logger.warning(f"MULTITOKEN LINK - from wallet through contract {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "multitoken"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                        # Links for transfer contract_address and to_address 
                        link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"MULTITOKEN LINK - FROM CONTRACT TRANSFER NFT TO WALLET {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "multitoken"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"MULTITOKEN TRANSFER NOT TYPIFIED")

        elif (group_size > 2) and (has_transaction):
            logger.warning(f"== COMPLETE GROUP SIZE > 2 =========================")
            print(f"Hash: {hash}")
            xfrom_address = xto_address = xsymbol = ''
            xvalue = 0
            qty_row = len(group)
            print(f"QTY: {qty_row}")
            for _, row in group.iterrows():
                # INFO: x_from and z_from are wallets and the same 
                type = row['type']
                from_address = row['from']
                to_address = row['to']
                symbol = row['symbol']
                value = float(row['valConv'])
                print(f"TYPE: {type}")
                print(f"FROM: {from_address}")
                print(f"TO: {to_address}")
                print(f"CONTRACT ADDRESS: {row['contractAddress']}")
                print(f"VALUE: {value}")
                print(f"SYMBOL: {symbol}")
                # Nodes
                # INFO: Transaction
                if (type == 'transaction'):
                    xfrom_address = from_address
                    xto_address = to_address
                    xvalue = value
                    xsymbol = symbol
                    if from_address not in nodes:
                        stat_wal += 1
                        # PERF: Improve
                        tag = tags_dict.get(from_address, [])  # Get tag
                        tag.append('wallet')
                        label = labels_dict.get(from_address, [])  # Get label
                        nodes[from_address] = {
                            "id": from_address, 
                            "address": from_address,
                            "tag": tag,
                            "label": label,
                        }
                    if to_address not in nodes:
                        stat_con += 1
                        # PERF: Improve
                        tag = tags_dict.get(to_address, [])  # Get tag
                        tag.append('contract')
                        label = labels_dict.get(to_address, [])  # Get label
                        nodes[to_address] = {
                            "id": to_address, 
                            "address": to_address,
                            "tag": tag,
                            "label": label,
                        }

                # INFO: Internals
                if (type == 'internals'):
                    contract_address = row['contractAddress']

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (xfrom_address == to_address): # REF: hash: 0xfe45d513dc4fc8fb844f7a4b4375b15e3e7ac0a1923fb8e9cf70b6428968a408
                        logger.debug(f"INTERNAL FORM 74 - INTERNAL RECEIVE")
                        if from_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(from_address, [])  # Get label
                            nodes[from_address] = {
                                "id": from_address, 
                                "address": from_address,
                                "tag": tag,
                                "label": label,
                            }

                        # Links for internal receive
                        link_key = f"{from_address}->{to_address}"
                        logger.warning(f"INTERNAL LINK - RECEIVE INTERNAL ETHER {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": from_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"INTERNALS NOT TYPIFIED")

                # INFO: Transfers
                if (type == 'transfers'):
                    contract_address = row['contractAddress']

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif ((qty_row == 3) and # Quantity in group
                         ((group.iloc[1]['type'] == "transfers") and (group.iloc[2]['type'] == "transfers")) and   # Validate types
                         (xfrom_address == group.iloc[1]['from']) and
                         (xfrom_address == group.iloc[2]['to'])): # REF: hash: 0xfff2a20407ec45aa55974a967da2fbb33d1a9590062570835f4afdcdf49ed52e

                        logger.debug(f"FORM 33 - Token exchange")
                        if (xfrom_address == from_address): # row 2
                            if to_address not in nodes:
                                stat_con += 1
                                # PERF: Improve
                                tag = tags_dict.get(to_address, [])  # Get tag
                                tag.append('contract')
                                label = labels_dict.get(to_address, [])  # Get label
                                nodes[to_address] = {
                                    "id": to_address, 
                                    "address": to_address,
                                    "tag": tag,
                                    "label": label,
                                }
                            # Links from to token
                            link_key = f"{from_address}->{to_address}"
                            logger.warning(f"TRANSFER LINK 1 PAY WITH TOKEN {link_key}")
                            if link_key not in links:
                                links[link_key] = {"source": from_address, "target": to_address, "detail": {}, "qty": 0}
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1
                            # Links to exchange
                            link_key = f"{to_address}->{xto_address}"
                            logger.warning(f"TRANSFER LINK TO EXCHANGE {link_key}")
                            if link_key not in links:
                                links[link_key] = {"source": to_address, "target": xto_address, "detail": {}, "qty": 0}
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1
                        if (xfrom_address == to_address): # row 3
                            if from_address not in nodes:
                                stat_con += 1
                                # PERF: Improve
                                tag = tags_dict.get(from_address, [])  # Get tag
                                tag.append('contract')
                                label = labels_dict.get(from_address, [])  # Get label
                                nodes[from_address] = {
                                    "id": from_address, 
                                    "address": from_address,
                                    "tag": tag,
                                    "label": label,
                                }
                            # Links from exchange to token
                            link_key = f"{xto_address}->{from_address}"
                            logger.warning(f"TRANSFER LINK 3 EXCHANGE TO NEW TOKEN {link_key}")
                            if link_key not in links:
                                links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1
                            # Links from new token to target
                            link_key = f"{from_address}->{to_address}"
                            logger.warning(f"TRANSFER LINK 4 NEW TOKEN TO TARGET WALLET {link_key}")
                            if link_key not in links:
                                links[link_key] = {"source": from_address, "target": to_address, "detail": {}, "qty": 0}
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1

                    elif (xfrom_address == to_address) and (xto_address == from_address): # REF: hash: 0xfe45d513dc4fc8fb844f7a4b4375b15e3e7ac0a1923fb8e9cf70b6428968a408
                        logger.debug(f" FORM 34 - FROM TOKEN CONTRACT TO WALLET")
                        if contract_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(contract_address, [])  # Get label
                            nodes[contract_address] = {
                                "id": contract_address, 
                                "address": contract_address,
                                "tag": tag,
                                "label": label,
                            }

                        # Links for transfer token 
                        link_key = f"{xto_address}->{contract_address}"
                        logger.warning(f" TRANSFER LINK - TRRANSFER TOKEN TO WALLET {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xto_address, "target": contract_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                        # Links for transfer token to wallet
                        link_key = f"{contract_address}->{xfrom_address}" # xto_address is a contract
                        # logger.warning(f"LINK 2 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": xfrom_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                #     elif (from_address == xfrom_address) and (contract_address == xto_address): # REF: hash: 0xf01325b1b4c10b4cbe86c94cf65e459dde587b86bcdcbe50beaa8fa94df4b7e8 
                #         logger.debug(f"FORM 4 (from_address == xfrom_address) and (contract_address == xto_address) - Transfer ERC20 to wallet") 
                #         if to_address not in nodes:
                #             stat_wal += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(to_address, [])  # Get tag
                #             tag.append('wallet')
                #             label = labels_dict.get(to_address, [])  # Get label
                #             nodes[to_address] = {
                #                 "id": to_address, 
                #                 "address": to_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }
                #         # Links for transaction xfrom and xto (contract)
                #         link_key = f"{xfrom_address}->{xto_address}"
                #         # logger.warning(f"LINK TRANSACTION {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][symbol]["sum"] += value
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                #         links[link_key]["qty"] += 1

                #         # Links from contract to wallet
                #         link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                #         # logger.warning(f"LINK TRANSFER {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][symbol]["sum"] += value
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                #         links[link_key]["qty"] += 1

                #     elif (xfrom_address == to_address) and (xto_address == from_address): # REF: hash: 0xa54c6cacc1661abef5adf16d4227e5f5827519ed1038043882b0cf11e4299085
                #         logger.debug(f" FORM 5 (xfrom_address == to_address) and (xto_address == from_address)")
                #         if contract_address not in nodes:
                #             stat_con += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(contract_address, [])  # Get tag
                #             tag.append('contract')
                #             label = labels_dict.get(contract_address, [])  # Get label
                #             nodes[contract_address] = {
                #                 "id": contract_address, 
                #                 "address": contract_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }

                #         # Links for transaction xfrom and xto
                #         link_key = f"{from_address}->{contract_address}"
                #         # logger.warning(f"LINK 1 {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": from_address, "target": contract_address, "detail": {}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][symbol]["sum"] += value
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                #         links[link_key]["qty"] += 1

                #         # Links for FORM trx xfrom == zfrom and one transfer
                #         link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                #         # logger.warning(f"LINK 2 {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][symbol]["sum"] += value
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                #         links[link_key]["qty"] += 1

                    elif (xfrom_address == to_address) and (xto_address != from_address != contract_address):  # REF: hash: 0x6beb31076d54d69f11fa2bf69faf50c6679a43c6e46932406e914d3995b509ae
                        logger.debug(f"TRANSFER FORM 46 (xfrom_address == to_address) and (xto_address != from_address != contract_address) - Buy token ERC20 through ETH")
                        for address in [from_address]:
                            if address not in nodes:
                                stat_wal += 1
                                # PERF: Improve
                                tag = tags_dict.get(address, [])  # Get tag
                                tag.append('wallet')
                                label = labels_dict.get(address, [])  # Get label
                                nodes[address] = {
                                    "id": address, 
                                    "address": address,
                                    "tag": tag,
                                    "label": label,
                                }

                        # Links for transaction xfrom and xto
                        link_key = f"{xfrom_address}->{xto_address}"
                        logger.warning(f"TRANSFER LINK 101 CALL TO EXCHANGE - {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                        if (xvalue != 0):
                            if xsymbol in links[link_key]["detail"]:
                                links[link_key]["detail"][xsymbol]["sum"] += xvalue
                                links[link_key]["detail"][xsymbol]["count"] += 1
                            else:
                                links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                            links[link_key]["qty"] += 1
                        else:
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1

                        # Links for transfer xto and from_address
                        link_key = f"{xto_address}->{from_address}"
                        logger.warning(f"TRANSFER LINK 102 CALL TO EXCHANGE - {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                        if (xvalue != 0):
                            if xsymbol in links[link_key]["detail"]:
                                links[link_key]["detail"][xsymbol]["sum"] += xvalue
                                links[link_key]["detail"][xsymbol]["count"] += 1
                            else:
                                links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                            links[link_key]["qty"] += 1
                        else:
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1

                        # # Links for transfer from_address and contract_address 
                        # link_key = f"{from_address}->{contract_address}" # xto_address is a contract
                        # # logger.warning(f"LINK 3 {link_key}")
                        # if link_key not in links:
                        #     links[link_key] = {"source": from_address, "target": contract_address, "detail": {}, "qty": 0}
                        # if symbol in links[link_key]["detail"]:
                        #     links[link_key]["detail"][symbol]["sum"] += value
                        #     links[link_key]["detail"][symbol]["count"] += 1
                        # else:
                        #     links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        # links[link_key]["qty"] += 1

                        # Links for transfer contract_address and to_address 
                        link_key = f"{from_address}->{to_address}" # xto_address is a contract
                        # logger.warning(f"LINK 4 {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": from_address, "target": to_address, "detail": {}, "qty": 0}
                        if (xvalue != 0):
                            if xsymbol in links[link_key]["detail"]:
                                links[link_key]["detail"][xsymbol]["sum"] += xvalue
                                links[link_key]["detail"][xsymbol]["count"] += 1
                            else:
                                links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                            links[link_key]["qty"] += 1
                        else:
                            if symbol in links[link_key]["detail"]:
                                links[link_key]["detail"][symbol]["sum"] += value
                                links[link_key]["detail"][symbol]["count"] += 1
                            else:
                                links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                            links[link_key]["qty"] += 1

                    elif (from_address == xfrom_address): # REF: hash: 0x26bae55868fed567c6f865259156ff1c56891f2c2bb87ba5cdfa1903d3823d18
                        logger.debug(f"TRANSFER FORM 44 - Swap ERC20 to token or ETHER") 
                        if to_address not in nodes:
                            stat_con += 1
                            # PERF: Improve
                            tag = tags_dict.get(to_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(to_address, [])  # Get label
                            nodes[to_address] = {
                                "id": to_address, 
                                "address": to_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links for start to SWAP through DEX
                        link_key = f"{xfrom_address}->{xto_address}"
                        logger.warning(f"LINK TRANSFER - SWAP TOKEN ERC20 DEX {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1
                        # Links for start to SWAP from DEX to token ERC20
                        link_key = f"{xto_address}->{to_address}"
                        logger.warning(f"LINK TRANSFER - SWAP TOKEN ERC20 CONTRACT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": xto_address, "target": to_address, "detail": {}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            links[link_key]["detail"][symbol]["sum"] += value
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"sum": value, "count": 1}
                        links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"ERC20 TRANSFER NOT TYPIFIED")

                # INFO: nfts
                if (type == 'nfts'):
                    contract_address = row['contractAddress']

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (xfrom_address == to_address) and \
                         (from_address == "0x0000000000000000000000000000000000000000"): # REF: hash : 0xf758e2b6a9ed20f3431ef36f57493cf2d6b81e4bc6e2e21921c690bc81b907dd
                        logger.debug(f"NFTS FORM 3 (xfrom_address == to_address) and (xto_address == contract_address) and (from_address == 0x00..) - Mint of NFT")
                        if contract_address not in nodes:  # INFO: This isn't neccesary
                            stat_con += 1
                            tag = tags_dict.get(contract_address, [])  # Get tag
                            tag.append('contract')
                            label = labels_dict.get(contract_address, [])  # Get label
                            nodes[contract_address] = {
                                "id": contract_address, 
                                "address": contract_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links from contract to to_address (This is a mint of NFT)
                        link_key = f"{contract_address}->{to_address}"
                        logger.warning(f"LINK NFT MINT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (from_address == xfrom_address) and (contract_address != xto_address != to_address): # REF: hash: 0x6beb31076d54d69f11fa2bf69faf50c6679a43c6e46932406e914d3995b509ae
                        logger.debug(f"NFTS FORM 400 - Transfer NFT ->") 
                        if to_address not in nodes:
                            stat_wal += 1
                            # PERF: Improve
                            tag = tags_dict.get(to_address, [])  # Get tag
                            tag.append('wallet')
                            label = labels_dict.get(to_address, [])  # Get label
                            nodes[to_address] = {
                                "id": to_address, 
                                "address": to_address,
                                "tag": tag,
                                "label": label,
                            }
                        # Links from wallet (xfrom_address to contract (or xto_address)
                        link_key = f"{xfrom_address}->{xto_address}"
                        logger.warning(f"LINK TRANSFER NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                        # Links from contract (xto_address) to wallet to_address
                        link_key = f"{xto_address}->{to_address}"
                        logger.warning(f"LINK TRANSFER NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                            links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                #     elif (from_address == xfrom_address) and (contract_address == xto_address): # REF: hash: 0xed609d5d6ccbd68138d8733da0c2711c102265fb8d47c9e4408139e794072913
                #         logger.debug(f"NFTS FORM 4 (from_address == xfrom_address) and (contract_address == xto_address) - Transfer NFT ->") 
                #         if to_address not in nodes:
                #             stat_wal += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(to_address, [])  # Get tag
                #             tag.append('wallet')
                #             label = labels_dict.get(to_address, [])  # Get label
                #             nodes[to_address] = {
                #                 "id": to_address, 
                #                 "address": to_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }
                #         # Links from wallet (xfrom_address to contract (or xto_address)
                #         link_key = f"{xfrom_address}->{xto_address}"
                #         # logger.warning(f"LINK TRANSFER NFT {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             # TODO: Add name?
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #         links[link_key]["qty"] += 1

                #         # Links from contract (xto_address) to wallet to_address
                #         link_key = f"{contract_address}->{to_address}"
                #         # logger.warning(f"LINK TRANSFER NFT {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             # TODO: Add name?
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #         links[link_key]["qty"] += 1

                #     elif (xfrom_address == to_address) and (xto_address != from_address != contract_address):  # REF: hash: 0xea91b02882a88fe588054fc2c2dbdaff8c24a5a9d30588512595ad8016aa91bd
                #         logger.debug(f"NFTS FORM 5 (xfrom_address == to_address) and (xto_address != from_address != contract_address) <- Buy NFT to another wallet")
                #         if from_address not in nodes:
                #             stat_con += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(from_address, [])  # Get tag
                #             tag.append('wallet')
                #             label = labels_dict.get(from_address, [])  # Get label
                #             nodes[from_address] = {
                #                 "id": from_address, 
                #                 "address": from_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }
                #         if contract_address not in nodes:
                #             stat_con += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(contract_address, [])  # Get tag
                #             tag.append('contract')
                #             label = labels_dict.get(contract_address, [])  # Get label
                #             nodes[contract_address] = {
                #                 "id": contract_address, 
                #                 "address": contract_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }

                #         # Links wallet buy NFT from DEX
                #         link_key = f"{xfrom_address}->{xto_address}"
                #         # logger.warning(f"NFT LINK - BUY NFT FROM DEX {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                #         if xsymbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][xsymbol]["sum"] += xvalue
                #             links[link_key]["detail"][xsymbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                #         links[link_key]["qty"] += 1

                #         # Links for transfer xto and from_address
                #         link_key = f"{xto_address}->{from_address}"
                #         # logger.warning(f"NFT LINK - DEX TRANSFER ETH TO WALLET {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                #         if xsymbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][xsymbol]["sum"] += xvalue
                #             links[link_key]["detail"][xsymbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                #         links[link_key]["qty"] += 1

                #         # Links for transfer from_address and contract_address  # HACK: This could be hide in a url inside the merge this with the next link
                #         link_key = f"{from_address}->{contract_address}"
                #         # logger.warning(f"NFT LINK - from wallet through contract {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             # TODO: Add name?
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #         links[link_key]["qty"] += 1

                #         # Links for transfer contract_address and to_address 
                #         link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                #         # logger.warning(f"NFT LINK - FROM CONTRACT TRANSFER NFT TO WALLET {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "nft"}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             # TODO: Add name?
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"nft": [], "count": 1}
                #             links[link_key]["detail"][symbol]["nft"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #         links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"NFTS TRANSFER NOT TYPIFIED")

                # INFO: Multitoken
                if (type == 'multitoken'):
                    contract_address = row['contractAddress']
                    # print(f"TYPE: {type}")
                    # print(f"XFROM: {xfrom_address}")
                    # print(f"XTO: {xto_address}")
                    # print(f"XVALUE: {xvalue}")
                    # print(f"XSYMBOL: {xsymbol}")
                    # print(f"FROM: {from_address}")
                    # print(f"TO: {to_address}")
                    # print(f"CONTRACT ADDRESS: {row['contractAddress']}")
                    # print(f"VALUE: {value}")
                    # print(f"SYMBOL: {symbol}")

                    if (xfrom_address == ''):
                        logger.error(f"XFROM ADDRESS is empty")

                    elif (xto_address == ''):
                        logger.error(f"XTO ADDRESS is empty")

                    elif (xfrom_address == to_address) and \
                         (from_address == "0x0000000000000000000000000000000000000000"): # REF: hash : 0xc86c7f7fa143321e574bfbd99b07681dde16e003042982c9f12ee918a0776930
                        logger.debug(f"MULTITOKEN FORM 3 (xfrom_address == to_address) and (xto_address == contract_address) and (from_address == 0x00..) - Mint of NFT in multitoken")
                        # if contract_address not in nodes:  # INFO: This isn't neccesary
                        #     stat_con += 1
                        #     tag = tags_dict.get(contract_address, [])  # Get tag
                        #     tag.append('contract')
                        #     label = labels_dict.get(contract_address, [])  # Get label
                        #     nodes[contract_address] = {
                        #         "id": contract_address, 
                        #         "address": contract_address,
                        #         "tag": tag,
                        #         "label": label,
                        #     }
                        # Links from contract to to_address (This is a mint of NFT)
                        link_key = f"{contract_address}->{to_address}"
                        logger.warning(f"LINK MULTITOKEN - MINT NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "multitoken"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                    elif (xfrom_address == from_address) and \
                         (to_address == "0x0000000000000000000000000000000000000000"): # REF: hash : 0xd68d6af297161460a477f56e76d2fdfba2978e2d10de2853dd2d693a21720e06 (First multitoken trx)
                        logger.debug(f"NFTS FORM 4 (xfrom_address == from_address) and (xto_address == contract_address) and (to_address == 0x00..) - Burn of NFT in multitoken")
                        # if contract_address not in nodes:  # INFO: This isn't neccesary
                        #     stat_con += 1
                        #     tag = tags_dict.get(contract_address, [])  # Get tag
                        #     tag.append('contract')
                        #     label = labels_dict.get(contract_address, [])  # Get label
                        #     nodes[contract_address] = {
                        #         "id": contract_address, 
                        #         "address": contract_address,
                        #         "tag": tag,
                        #         "label": label,
                        #     }
                        # Links for burn NFT
                        link_key = f"{from_address}->{contract_address}"
                        logger.warning(f"LINK MULTITOKEN - BURN NFT {link_key}")
                        if link_key not in links:
                            links[link_key] = {"source": from_address, "target": contract_address, "detail": {"type": "multitoken"}, "qty": 0}
                        if symbol in links[link_key]["detail"]:
                            # TODO: Add name?
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                            links[link_key]["detail"][symbol]["count"] += 1
                        else:
                            links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                            links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                        links[link_key]["qty"] += 1

                #     elif (xfrom_address == to_address) and (xto_address != from_address != contract_address):  # REF: hash: 0x7dcef56e5ea86475bc24ae59387c278b56ed3432ca39d68d67f150e61266bd6d
                #         logger.debug(f"NFTS FORM 5 (xfrom_address == to_address) and (xto_address != from_address != contract_address) <- Buy NFT to another wallet")
                #         if from_address not in nodes:
                #             stat_con += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(from_address, [])  # Get tag
                #             tag.append('wallet')
                #             label = labels_dict.get(from_address, [])  # Get label
                #             nodes[from_address] = {
                #                 "id": from_address, 
                #                 "address": from_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }
                #         if contract_address not in nodes:
                #             stat_con += 1
                #             # PERF: Improve
                #             tag = tags_dict.get(contract_address, [])  # Get tag
                #             tag.append('contract')
                #             label = labels_dict.get(contract_address, [])  # Get label
                #             nodes[contract_address] = {
                #                 "id": contract_address, 
                #                 "address": contract_address,
                #                 "tag": tag,
                #                 "label": label,
                #             }

                #         # Links wallet buy NFT from DEX
                #         link_key = f"{xfrom_address}->{xto_address}"
                #         # logger.warning(f"MULTITOKEN LINK - BUY NFT FROM DEX {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": xfrom_address, "target": xto_address, "detail": {}, "qty": 0}
                #         if xsymbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][xsymbol]["sum"] += xvalue
                #             links[link_key]["detail"][xsymbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                #         links[link_key]["qty"] += 1

                #         # Links for transfer xto and from_address
                #         link_key = f"{xto_address}->{from_address}"
                #         # logger.warning(f"MULTITOKEN LINK - DEX TRANSFER ETH TO WALLET {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": xto_address, "target": from_address, "detail": {}, "qty": 0}
                #         if xsymbol in links[link_key]["detail"]:
                #             links[link_key]["detail"][xsymbol]["sum"] += xvalue
                #             links[link_key]["detail"][xsymbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][xsymbol] = {"sum": xvalue, "count": 1}
                #         links[link_key]["qty"] += 1

                #         # Links for transfer from_address and contract_address  # HACK: This could be hide in a url inside the merge this with the next link
                #         link_key = f"{from_address}->{contract_address}"
                #         # logger.warning(f"MULTITOKEN LINK - from wallet through contract {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "multitoken"}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             # TODO: Add name?
                #             links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                #             links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #         links[link_key]["qty"] += 1

                #         # Links for transfer contract_address and to_address 
                #         link_key = f"{contract_address}->{to_address}" # xto_address is a contract
                #         # logger.warning(f"MULTITOKEN LINK - FROM CONTRACT TRANSFER NFT TO WALLET {link_key}")
                #         if link_key not in links:
                #             links[link_key] = {"source": contract_address, "target": to_address, "detail": {"type": "multitoken"}, "qty": 0}
                #         if symbol in links[link_key]["detail"]:
                #             # TODO: Add name?
                #             links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #             links[link_key]["detail"][symbol]["count"] += 1
                #         else:
                #             links[link_key]["detail"][symbol] = {"multitoken": [], "count": 1}
                #             links[link_key]["detail"][symbol]["multitoken"].append({"id": int(value), "url": f"https://etherscan.io/nft/{contract_address}/{int(value)}"})
                #         links[link_key]["qty"] += 1

                    else: 
                        logger.error(f"MULTITOKEN TRANSFER NOT TYPIFIED")

        # else:
        #     print(f"== INCOMPLETE ====================================")
        #     print(f"Hash: {hash}")
        #     print(group)
        #     print("\n")

    nodes_list = list(nodes.values())
    logger.debug(f"NODES QTY: {len(nodes_list)}")
    logger.debug(f"COMPLETE: {count}")
    links_list = list(links.values())

    transactions = {"nodes": nodes_list, "links": links_list}

    # list_trans = df_all[(df_all['from'] == address_central) | (df_all['to'] == address_central)].to_json(orient = "records")  # FIX: Add contractAddress ??
    list_trans = df_all.loc[(df_all["from"] == address_central) | (df_all["to"] == address_central)].to_json(orient = "records")

    type_counts = df_all['type'].value_counts()
    stat_trx = type_counts.get('transaction', 0)
    stat_int = type_counts.get('internals', 0)
    stat_tra = type_counts.get('transfers', 0)
    # TODO: Add nfts and multitoken
    stat_tot = stat_trx + \
               stat_int + \
               stat_tra + \
               stat_err

    stat = {"stat_trx": int(stat_trx), "stat_int": int(stat_int), 
            "stat_tra": int(stat_tra), "stat_wal": int(stat_wal),
            "stat_tot": int(stat_tot), "stat_con": int(stat_con), 
            "stat_err": int(stat_err), "stat_coo": int(stat_con)}  # FIX: repeating stat_con in stat_coo

    return {"transactions": transactions, "list": list_trans, "stat": stat}








def get_trx_from_addresses_research(conn, address_central, params=[]):

    # INFO: Config Log Level
    if params:
        log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
        coloredlogs.install(level=params['config']['level'], fmt=log_format, logger=logger)
        logger.propagate = False  # INFO: To prevent duplicates with flask

    logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug(f"+ Address: {address_central}")
    logger.debug(f"++++++++++++++++++++++++++++++++++++++++++++++++++++")

    address_central = address_central[1]

    # INFO: Get all Trx, Transfers, internals, nfts and multitoken
    query = """
        SELECT blockChain, type, hash, `from`, `to`, value, contractAddress, symbol, name, decimal, valConv, timeStamp, isError, methodId, functionName
        FROM (
            SELECT blockChain, 'transaction' as 'type', hash, `from`, `to`, value, contractAddress, 'ETH' as 'symbol', 
			'Ether' as 'name', 18 as 'decimal', value / POWER(10, 18) as valConv, timeStamp, isError, methodId, functionName
            FROM t_transactions
            UNION ALL
            SELECT blockChain, 'internals', hash, `from`, `to`, value, 
                contractAddress, 'ETH', 'Ether', 18, value / POWER(10, 18), timeStamp, isError, '0x', ''
            FROM t_internals
            UNION ALL
            SELECT blockChain, 'transfers', hash, `from`, `to`, value,
                contractAddress, tokenSymbol, tokenName, tokenDecimal, value / POWER(10, tokenDecimal), timeStamp, 0, '0x', ''
            FROM t_transfers
            UNION ALL
            SELECT blockChain, 'nfts', hash, `from`, `to`, tokenID,
                contractAddress, tokenSymbol, tokenName, tokenDecimal, tokenID, timeStamp, 0, '0x', ''
            FROM t_nfts
            UNION ALL
            SELECT blockChain, 'multitoken', hash, `from`, `to`, tokenID,
                contractAddress, tokenSymbol, tokenName, tokenValue, tokenID, timeStamp, 0, '0x', ''
            FROM t_multitoken
        ) AS combined
        ORDER BY timeStamp ASC
    """
    df_all = pd.read_sql_query(query, conn)

    # INFO: Convert to datetime
    df_all['timeStamp'] = pd.to_datetime(df_all['timeStamp'], unit='s')
    df_all = df_all[df_all['isError'] == 0]

    # for index, row in df_all.iterrows():
    grouped = df_all.groupby('hash')

    for hash, group in grouped:

        # PERF: Process each row independently but try process like group in the future, if it's possible
        group_size = len(group)
        has_transaction = 'transaction' in group['type'].values
        # print(f"+++++++++ {grouped[0]}")

        # print(f"TYPE: {type}")
        # print(f"XFROM: {xfrom_address}")
        # print(f"XTO: {xto_address}")
        # print(f"XVALUE: {xvalue}")
        # print(f"XSYMBOL: {xsymbol}")
        # print(f"FROM: {from_address}")
        # print(f"TO: {to_address}")
        # print(f"CONTRACT ADDRESS: {row['contractAddress']}")
        # print(f"VALUE: {value}")
        # print(f"SYMBOL: {symbol}")

        # INFO: Complete transaction and pure =======================================
        if (group_size == 1) and (has_transaction):
            pass
            # logger.debug(f"\n== SIMPLE TRANSACTION ============================")
            # for _, row in group.iterrows():
            #     print(f"TYPE: {row['type']} - HASH: {hash}")
            #     print(f"XFROM: {row['from']} -> XTO: {row['to']} <--> CONTRACT: {row['contractAddress']}")
            #     print(f"XVALUE: {row['valConv']} - XSYMBOL: {row['symbol']} - XMETHOD: {row['methodId']} - XFUNC: {row['functionName']}") 
            #     if (float(row['value']) != 0.0) and (row['functionName'] == ''):
            #         logger.warning(f"   ETHER MOVE ====================================")
            #     elif (float(row['value']) == 0.0) and (row['functionName'] != ''):
            #         logger.warning(f"   CONTRACT EXECUTION ============================")
            #     elif (float(row['value']) != 0.0) and ('deposit' in row['functionName']):
            #         logger.warning(f"   DEPOSIT =======================================")
            #     elif (row['from'] == row['to']):
            #         logger.warning(f"   SELF-DEPOSIT ==================================")
            #     else:
            #         logger.error(f"   NOT DETECTED ==================================")

        # INFO: OTHERS
        elif (group_size > 1) and (has_transaction):
            pass
            print(" ")
            logger.debug(f"== NOT SIMPLE ====================================")
            xfrom_address = xto_address = xsymbol = xfunc = ''
            xvalue = 0
            for _, row in group.iterrows():
                if (row['type'] == 'transaction'):
                    xfrom_address = row['from']
                    xto_address = row['to']
                    xvalue = row['valConv']
                    xsymbol = row['symbol']
                    xfunc = row['functionName'].split('(')[0]
                    xparam = row['functionName'].split('(')[1]
                    print(f"TYPE: {row['type']} - HASH: {hash}")
                    print(f"XFROM: {xfrom_address} -> XTO: {xto_address}")
                    print(f"XVALUE: {xvalue} - XSYMBOL: {xsymbol} - XFUNC: {xfunc}-{xparam}") 
                else:
                    logger.debug(f"   ERC - {row['type']} =============================")

                    if (row['type'] == 'transfers') and (float(xvalue) == 0.0) and ("swap" in xfunc) and (group.iloc[1]['from'] == group.iloc[-1]['to'] == address_central):
                        # WARN: Perhaps I must verify type of all ERC row
                        # print(f"GROUP")
                        # print(f"{group[['from', 'to', 'value', 'contractAddress']]}")
                        # logger.warning(f"   SWAP TOKEN ====================================")
                        break
                    elif (row['type'] == 'transfers') and (float(xvalue) != 0.0) and ("swap" in xfunc) and (xfrom_address == row['to'] == address_central) and (xto_address != row['from'] != row['contractAddress']):
                        pass
                        # logger.warning(f"   SWAP ETHER TO TK ==============================")
                    elif (row['type'] == 'transfers') and (float(xvalue) == 0.0) and ("deposit" in xfunc) and (xfrom_address == row['from'] == address_central) and (xto_address == row['to'] != row['contractAddress']):
                        pass
                        logger.warning(f"   DEPOSIT TOKEN =================================")
                    # elif (row['type'] == 'nfts') and (xfrom_address == row['to'] == address_central) and (xto_address == row['contractAddress']) and (row['from'] == '0x0000000000000000000000000000000000000000'):
                    #     pass
                    #     logger.warning(f"   MINT NFT ======================================")
                    elif (row['type'] == 'nfts') and (xfrom_address == row['to'] == address_central) and (row['from'] == '0x0000000000000000000000000000000000000000'):
                        pass
                        # logger.warning(f"   MINT NFT ======================================")
                    elif (row['type'] == 'transfers') and (xfrom_address == row['from'] == address_central) and (xto_address == row['contractAddress']) and (row['to'] != '0x0000000000000000000000000000000000000000'):
                        pass
                        # logger.warning(f"   TRANSFER TK FROM WA ===========================")
                    elif (row['type'] == 'nfts') and (xfrom_address == row['from'] == address_central) and (xto_address == row['contractAddress']) and (row['to'] != '0x0000000000000000000000000000000000000000'):
                        pass
                        # logger.warning(f"   TRANSFER NFT FROM WA ===========================")
                    elif (row['type'] == 'nfts') and (xfrom_address == row['to'] == address_central) and (xto_address != row['to'] != row['contractAddress']) and (row['from'] != '0x0000000000000000000000000000000000000000'):
                        pass
                        # logger.warning(f"   BUY NFT ========================================")
                    elif (row['type'] == 'transfers') and ("withdraw" in xfunc) and (xfrom_address == row['to'] == address_central) and (xto_address == row['from']):
                        pass
                        # logger.warning(f"   WITHDRAW TOKEN ================================")
                    elif (row['type'] == 'internals') and ("withdraw" in xfunc) and (xfrom_address == row['to'] == address_central) and (xto_address == row['from']):
                        pass
                        # logger.warning(f"   UNWRAP INTERNAL ===============================")
                    elif (row['type'] == 'multitoken') and (xfrom_address == row['to'] == address_central) and (row['from'] == '0x0000000000000000000000000000000000000000'):
                        pass
                        # logger.warning(f"   MULTITOKEN MINT NFT ===========================")
                    elif (row['type'] == 'multitoken') and (xfrom_address == row['from'] == address_central) and (row['to'] == '0x0000000000000000000000000000000000000000'):
                        pass
                        # logger.warning(f"   MULTITOKEN BURN NFT ===========================")
                    elif (row['type'] == 'transfers') and (xfrom_address == row['to'] == address_central) and (xto_address == row['from']):
                        pass
                        # logger.warning(f"   TRANSFER TK TO WA ===========================")
                    elif (row['type'] == 'multitoken') and (xfrom_address == row['to'] == address_central) and (float(xvalue) > 0.0) and (xto_address != row['from'] != row['contractAddress']):
                        # INFO: Implement mechanism to determine if it's transfer NFT or token
                        # Perhaps you need to use Decimal fields
                        pass
                        # logger.warning(f"   MULTITOKEN BUY NFT ============================")
                    elif (row['type'] == 'transfers') and (float(xvalue) == 0.0) and ("atomicMatch" in xfunc) and (group.iloc[1]['from'] == group.iloc[-1]['to']) and ((group['type'] == 'nfts').any()):
                        # print(f"GROUP")
                        # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                        # logger.warning(f"   TRANSFER TOKEN BY NFT =========================")
                        break
                    elif (float(xvalue) == 0.0) and ("swap" in xfunc) and (group.iloc[1]['from'] == group.iloc[-1]['to']) and ((group['type'] == 'internals').any()) and ((group['type'] == 'transfers').any()):
                        # print(f"GROUP")
                        # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                        # logger.warning(f"   BUY TOKEN WITH ETHER ==========================")
                        break
                    else:
                        logger.error(f"   NOT DETECTED ==================================")
                    print(f"FROM: {row['from']} -> TO: {row['to']} <-> CON: {row['contractAddress']}")
                    print(f"VALUE: {row['valConv']} - SYMBOL: {row['symbol']} - FUNC: {row['functionName']}") 

        elif (not has_transaction):
            pass
            # logger.debug(f"== INCOMPLETE ====================================")
            # for _, row in group.iterrows():
            #     print(f"TYPE: {row['type']} - HASH: {hash}")
            #     print(f"XFROM: {row['from']}")
            #     print(f"XTO: {row['to']}")
            #     print(f"XVALUE: {row['valueConv']}")
            #     print(f"XSYMBOL: {row['symbol']}")
            #     print(f"CONTRACT ADDRESS: {row['contractAddress']}")
        else:
            pass
            # logger.debug(f"== NOT CLASSIFIED ================================")
            # for _, row in group.iterrows():
            #     print(f"TYPE: {row['type']} - HASH: {hash}")
            #     print(f"XFROM: {row['from']}")
            #     print(f"XTO: {row['to']}")
            #     print(f"XVALUE: {row['valueConv']}")
            #     print(f"XSYMBOL: {row['symbol']}")
            #     print(f"CONTRACT ADDRESS: {row['contractAddress']}")

    return {"transactions": "ok"}
