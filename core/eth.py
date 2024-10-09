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
import pandas as pd  # pyright: ignore
import pandasql as psql  # pyright: ignore
from collections import defaultdict
import yaml

# from sqlalchemy import text
# from datetime import datetime
# from flask import jsonify

from termcolor import colored  # pyright: ignore
import coloredlogs  # pyright: ignore

from core import misc

logger = logging.getLogger(__name__)
# logger.propagate = False  # INFO: To prevent duplicates with flask


def insert_with_ignore(table, conn, keys, data_iter):
    # INFO: This escape is for reserved words
    escaped_keys = [f'"{k}"' if (k.lower() == "from") or (k.lower() == "to") else k for k in keys]

    columns = ",".join(escaped_keys)
    placeholders = ",".join([f":{k}" for k in keys])
    stmt = f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
    data_dicts = (dict(zip(keys, data)) for data in data_iter)

    for data in data_dicts:
        conn.execute(stmt, data)


def db_store_wallet_detail(conn, data):
    conn.execute(
        """INSERT INTO t_address_detail VALUES 
                   (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                   ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                ?, ?, ?)
                   """,
        (
            "eth",  # TODO: Manage multiple blockchain
            data["address"],
            data["blockNumber"],
            data["last_block"],
            data["blockNumber"],
            data["timeStamp"],
            data["hash"],
            data["from"],
            data["to"],
            data["value"],
            data["input"],
            "",
            data["type"],
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            data["contractAddress"],
            "",
        ),
    )

    conn.commit()


def db_store_address_block(conn, data):
    conn.execute(
        """INSERT INTO t_blocks VALUES 
                   (?, ?, ?, ?, ?, ?, ?)
                   """,
        (
            "eth",  # TODO: Manage multiple blockchain
            data["address"],
            data["blockNumber"],
            data["last_block"],
            data["timeStamp"],
            data["timeStamp_to"],
            data["all_data"],
        ),
    )

    conn.commit()


def db_store_contracts(conn, datas):
    for data in datas:
        try:
            conn.execute(
                """INSERT INTO t_contract VALUES 
                           (?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                           ?, ?, ?, ?, ?)
                           """,
                (
                    "eth",
                    data["contract"],
                    data["SourceCode"],
                    data["ABI"],
                    data["ContractName"],
                    data["CompilerVersion"],
                    data["OptimizationUsed"],
                    data["Runs"],
                    data["ConstructorArguments"],
                    data["EVMVersion"],
                    data["Library"],
                    data["LicenseType"],
                    data["Proxy"],
                    data["Implementation"],
                    data["SwarmSource"],
                ),
            )
        except Exception as e:
            print(e)

    conn.commit()


def event_stream_ether(params):
    # INFO: Config Log Level
    log_format = "%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s"
    coloredlogs.install(level=params["config"]["level"], fmt=log_format, logger=logger)
    logger.propagate = False  # INFO: To prevent duplicates with flask

    logger.info("Getting information")
    data = json.dumps({"msg": "Getting information", "end": False, "error": False, "content": {}})
    yield f"data:{data}\n\n"

    message = "<strong>Blockchain</strong> - Ethereum"
    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
    yield f"data:{data}\n\n"

    # Checking params
    logger.info("Checking params")
    data = json.dumps({"msg": "Checking params", "end": False, "error": False, "content": {}})
    yield f"data:{data}\n\n"
    if params.get("address", "") == "":
        message = "<strong>Param address is mandatory</strong>"
        logger.error(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": message, "end": True, "error": True, "content": {}})
        yield f"data:{data}\n\n"
    else:
        address = params.get("address").lower()

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
        key = params["config"]["ethscan"]
        dbname = params["config"]["dbname"]

        connection = sqlite3.connect(dbname)
        cursor = connection.cursor()

        # INFO: Get blockchain param
        if (params.get("network", "") == "") or (params.get("network", "") == "undefined"):
            # INFO: ERROR
            blockchain = ""
            connection.close()
            message = "<strong>Error...</strong>"
            logger.error(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            message = "<strong>Blockchain must be informed</strong>"
            logger.error(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            message = " "
            logger.error(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
            yield f"data:{data}\n\n"
            raise Exception("Blockchain must be informed")
        else:
            blockchain = params["network"]
        logger.debug(f"Blockchain: {blockchain}")

        # INFO: Warning message about API
        if (params["config"]["ethscan"] == "") or (params["config"]["ethscan"] == "XXX"):
            message = "<strong>Etherscan.io api key possibly unconfigured</strong>"
            logger.error(message.replace("<strong>", "").replace("</strong>", ""))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

        # INFO: Update central address
        query = "SELECT * FROM t_tags WHERE tag = 'central'"
        cursor.execute(query)
        central = cursor.fetchall()
        logger.debug(f"Central address: {central}")
        if central:
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

        # Inicialize DataFrames
        df_trx_store = pd.DataFrame(
            columns=["blockChain", "hash", "from", "to", "value", "contractAddress", "timeStamp", "isError", "methodId", "functionName"]
        )
        df_internals_store = pd.DataFrame(columns=["blockChain", "hash", "from", "to", "value", "contractAddress", "timeStamp", "isError"])
        df_transfers_store = pd.DataFrame(
            columns=["blockChain", "hash", "from", "to", "value", "contractAddress", "tokenSymbol", "tokenName", "tokenDecimal", "timeStamp"]
        )
        df_nfts_store = pd.DataFrame(
            columns=["blockChain", "hash", "from", "to", "tokenID", "contractAddress", "tokenSymbol", "tokenName", "tokenDecimal", "timeStamp"]
        )
        df_multitoken_store = pd.DataFrame(
            columns=["blockChain", "hash", "from", "to", "tokenID", "contractAddress", "tokenSymbol", "tokenName", "tokenValue", "timeStamp"]
        )

        logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")
        logger.debug(f" Wallet detail: {wallet_detail}")
        logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")

        # INFO: Get information
        if (wallet_detail == []) or (wallet_detail is None):
            # INFO: Verify if address is contract
            type = "wallet"
            contract_creation = {}
            try:
                url = f"https://api.etherscan.io/api?module=contract&action=getcontractcreation&contractaddresses={address}&apikey={key}"
                response = requests.get(url)
                contract_creation = response.json()
                json_status = response.json()["status"]
                json_message = response.json()["message"]
                json_result = response.json()["result"]

                if json_message == "NOTOK":
                    message = "<strong>Error...</strong>"
                    logger.error(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    message = f"<strong>{json_result}</strong>"
                    logger.error(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    message = " "
                    logger.error(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"
                    raise Exception(json_result)
                elif json_status == "1":
                    type = "contract"
                    message = "<strong>ADDRESS</strong> - Contract"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"
                else:
                    message = "<strong>ADDRESS</strong> - Wallet"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

            except Exception:
                traceback.print_exc()
                traceback_text = traceback.format_exc()

                connection.close()
                message = "<strong>Error...</strong>"
                logger.warning(f"{message}")
                data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                yield f"data:{data}\n\n"

                for line in traceback_text.splitlines():
                    message = f"{line}"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                message = " "
                logger.warning(f"{message}")
                data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                yield f"data:{data}\n\n"

                # HACK: Here should be a raise?

            # INFO: Bypass contract information from search
            # if (source == "search" and type == "contract"):
            if type == "contract":
                # TODO:
                # - Get creator and trx (X)
                # - Store data in tagging table
                # - Store data in label table (name of contract)

                # INFO: Get contract information
                json_contract = []
                try:
                    url = f"https://api.etherscan.io/api?module=contract&action=getsourcecode&address={address}&apikey={key}"
                    response = requests.get(url)
                    json_contract = response.json()["result"]

                    if len(json_contract) > 0:
                        message = "<strong>CONTRACT</strong> - Collected information"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = "<strong>CONTRACT<strong> - <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store data in contract table
                try:
                    message = "<strong>CONTRACT</strong> - Storing information..."
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # Store contract
                    db_store_contracts(connection, json_contract)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get trx creation
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&page=1&offset=1&sort=asc&apikey={key}"
                    response = requests.get(url)
                    json_object = response.json()["result"]

                    if len(json_object) > 0:
                        message = "<strong>TRANSACTIONS</strong> - Found trx of creation contract"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                        # INFO: Modify the trx creation, necessary to graph
                        json_object[0]["to"] = json_object[0]["contractAddress"]
                    else:
                        message = "<strong>TRANSACTIONS<strong> - Creation contract <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store wallet detail (contract)
                try:
                    message = "<strong>TRANSACTIONS</strong> - Storing details...Contract..."
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # Get first trx
                    first = json_object[0].copy()
                    logger.debug(f"First trx :\n{first}")
                    # Determine type of address   # TODO: NFT
                    first["type"] = "contract"
                    if first["to"] == "" and first["contractAddress"] != "":
                        first["type"] = "contract"
                    # Get last block collected
                    first["last_block"] = json_object[-1]["blockNumber"]
                    first["timeStamp_to"] = json_object[-1]["timeStamp"]
                    # TODO: Do this check more complex for large wallets and contracts,
                    first["all_data"] = True  # INFO: Because it is a contract
                    # Set address
                    first["address"] = address

                    # PERF: Analyze if wallet_detail and block table can merge

                    # Store detail
                    db_store_wallet_detail(connection, first)
                    # Store blocks
                    db_store_address_block(connection, first)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store transaction of contract creation
                try:
                    message = "<strong>DATA COLLECTED</strong> - Storing..."
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    tic = time.perf_counter()
                    # db_store_transactions_optimized(connection, json_object)
                    df_trx_store = pd.DataFrame(json_object)
                    df_trx_store["blockChain"] = blockchain
                    df_trx_store.to_sql("t_transactions", connection, if_exists="append", index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transactions of contract creation...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.error(message.replace("<strong>", "").replace("</strong>", ""))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store internal tagging and labels of contract
                try:
                    tic = time.perf_counter()

                    contract_tagging = []
                    # contract_tagging.append(('blockChain': 'eth', 'address': address, 'tag': 'contract'})
                    contract_tagging.append((blockchain, address, "contract"))
                    # print(f"Contract creation: {contract_creation}")
                    # print(f"Contract creator: {contract_creation['result'][0]['contractCreator']}")
                    # contract_tagging.append({'blockChain': 'eth', 'address': contract_creation['result'][0]['contractCreator'], 'tag': 'contract creator'})
                    contract_tagging.append((blockchain, contract_creation["result"][0]["contractCreator"], "contract creator"))
                    # print(f"Contract tagging: {contract_tagging}")
                    # Insert tags in SQLite
                    cursor = connection.cursor()
                    insert_tag = "INSERT OR IGNORE INTO t_tags (blockChain, address, tag) VALUES (?, ?, ?)"
                    cursor.executemany(insert_tag, contract_tagging)
                    # cursor.executemany(insert_tag, contract_tagging.to_records(index=False))
                    connection.commit()

                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Tagging...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    tic = time.perf_counter()

                    # INFO: Check if address exist in labels table
                    query = f"SELECT * FROM t_labels WHERE address = '{address}'"
                    cursor.execute(query)
                    label = cursor.fetchall()
                    if (len(label) == 0) and (len(json_contract) > 0):
                        # INFO: Store internal label
                        if json_contract[0]["ContractName"] == "":
                            contract_label = [("ethereum", "internal_label", address, "Contract without name", '["caution", "noName"]')]
                        else:
                            contract_label = [
                                (
                                    "ethereum",
                                    "internal_label",
                                    address,
                                    json_contract[0]["ContractName"],
                                    '["' + json_contract[0]["ContractName"][:10] + '"]',
                                )
                            ]
                        # Insert tags in SQLite
                        cursor = connection.cursor()
                        insert_label = "INSERT OR IGNORE INTO t_labels (blockChain, source, address, name, labels) VALUES (?, ?, ?, ?, ?)"
                        # cursor.executemany(insert_tag, contract_label.to_records(index=False))
                        cursor.executemany(insert_label, contract_label)
                        connection.commit()

                        toc = time.perf_counter()
                        message = f"<strong>STORE</strong> - Internal label...<strong>{toc - tic:0.4f}</strong> seconds"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    # INFO: Store contract creation info
                    cursor = connection.cursor()
                    insert_creator = """INSERT OR IGNORE INTO t_funders_creators 
                        (blockChain, blockNumber, type, timeStamp, hash, `from`, `to`, value, input, contractAddress, tokenDecimal, tokenSymbol, tokenName) 
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                    creator_row = [
                        (
                            blockchain,
                            json_object[0]["blockNumber"],
                            "creation",
                            json_object[0]["timeStamp"],
                            json_object[0]["hash"],
                            json_object[0]["from"],
                            json_object[0]["to"],
                            json_object[0]["value"],
                            json_object[0]["input"],
                            json_object[0]["contractAddress"],
                            "",
                            "",
                            "",
                        )
                    ]
                    cursor.executemany(insert_creator, creator_row)
                    connection.commit()

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.error(message.replace("<strong>", "").replace("</strong>", ""))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

            # INFO: Get wallet info
            elif type == "wallet":
                # INFO: Get trx
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlist&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={key}"
                    response = requests.get(url)
                    json_object = response.json()["result"]

                    if len(json_object) > 0:
                        message = "<strong>TRANSACTIONS</strong> - Found first block and trx"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = "<strong>TRANSACTIONS<strong> - First block and trx <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get internals
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=txlistinternal&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_internals = response.json()["result"]

                    if len(json_internals) > 0:
                        message = "<strong>INTERNALS</strong> - Found internals"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = "<strong>INTERNALS<strong> - Internals <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get transfers
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=tokentx&address={address}&startblock=0&endblock=99999999&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_transfers = response.json()["result"]

                    if len(json_transfers) > 0:
                        message = "<strong>TRANSFERS</strong> - Found transfers"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = "<strong>TRANSFERS<strong> - Transfers <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get NFTs (ERC-721)
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=tokennfttx&address={address}&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_nfts = response.json()["result"]

                    if len(json_nfts) > 0:
                        message = "<strong>NFTs</strong> - Found NFTs"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = "<strong>NFTs<strong> - Transfers <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Get Multitoken trx (ERC-1551)
                try:
                    url = f"https://api.etherscan.io/api?module=account&action=token1155tx&address={address}&sort=asc&apikey={key}"

                    response = requests.get(url)
                    json_multitokens = response.json()["result"]

                    if len(json_multitokens) > 0:
                        message = "<strong>MULTITOKENS</strong> - Found Multi Token Standard"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"
                    else:
                        message = "<strong>MULTITOKENS<strong> - Transfers <strong>NOT FOUND</strong>"
                        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                        data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # TODO: If wallet is a contract, get and store contract information

                # INFO: Store wallet detail in DB
                try:
                    message = "<strong>TRANSACTIONS</strong> - Storing details..."
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # Get first trx
                    first = json_object[0].copy()
                    logger.debug(f"First trx :\n{first}")
                    # Determine type of address   # TODO: NFT
                    first["type"] = "wallet"

                    if first["to"] == "" and first["contractAddress"] != "":
                        first["type"] = "contract"
                    # Get last block collected
                    first["last_block"] = json_object[-1]["blockNumber"]
                    first["timeStamp_to"] = json_object[-1]["timeStamp"]
                    # TODO: Do this check more complex for large wallets and contracts,
                    if len(json_object) == 10000:
                        first["all_data"] = False
                    else:
                        first["all_data"] = True
                    # Set address
                    first["address"] = address

                    # PERF: Analyze if wallet_detail and block table can merge

                    # Store detail
                    db_store_wallet_detail(connection, first)
                    # Store blocks
                    db_store_address_block(connection, first)
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Store transaction, internals, transfers and NFTs in DB
                try:
                    message = "<strong>DATA COLLECTED</strong> - Storing..."
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # HACK: Here is the crux of it all, because I have to transfer the methodID and funcname to the rest!!!!
                    # TODO: Register in internals, transfers, nfts and multitoken the trxs neccesary to complete methodIs and functionName
                    # INFO: transactions
                    tic = time.perf_counter()
                    # db_store_transactions_optimized(connection, json_object)
                    df_trx_store = pd.DataFrame(json_object)
                    df_trx_store["blockChain"] = blockchain
                    df_trx_store.to_sql("t_transactions", connection, if_exists="append", index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transactions...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: internals
                    tic = toc
                    # db_store_internals_optimized(connection, json_internals)
                    if json_internals != []:
                        df_internals_store = pd.DataFrame(json_internals)
                        df_internals_store["blockChain"] = blockchain
                        df_internals_merged = df_internals_store.merge(df_trx_store[["hash", "methodId", "functionName"]], on="hash", how="left")
                        df_internals_merged.fillna("", inplace=True)
                        df_internals_merged.to_sql("t_internals", connection, if_exists="append", index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Internals...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: transfers
                    tic = toc
                    # db_store_transfers_optimized(connection, json_transfers)
                    if json_transfers != []:
                        df_transfers_store = pd.DataFrame(json_transfers)
                        df_transfers_store["blockChain"] = blockchain
                        df_transfers_merged = df_transfers_store.merge(df_trx_store[["hash", "methodId", "functionName"]], on="hash", how="left")
                        df_transfers_merged.fillna("", inplace=True)
                        df_transfers_merged.to_sql("t_transfers", connection, if_exists="append", index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transfers ERC20...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: NFTs
                    tic = toc
                    # db_store_nfts_optimized(connection, json_nfts)
                    if json_nfts != []:
                        df_nfts_store = pd.DataFrame(json_nfts)
                        df_nfts_store["blockChain"] = blockchain
                        df_nfts_merged = df_nfts_store.merge(df_trx_store[["hash", "methodId", "functionName"]], on="hash", how="left")
                        df_nfts_merged.fillna("", inplace=True)
                        df_nfts_merged.to_sql("t_nfts", connection, if_exists="append", index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transfers ERC721...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    # INFO: Multitokens
                    tic = toc
                    # db_store_multitoken_optimized(connection, json_multitokens)
                    if json_multitokens != []:
                        df_multitoken_store = pd.DataFrame(json_multitokens)
                        df_multitoken_store["blockChain"] = blockchain
                        df_multitoken_merged = df_multitoken_store.merge(df_trx_store[["hash", "methodId", "functionName"]], on="hash", how="left")
                        df_multitoken_merged.fillna("", inplace=True)
                        df_multitoken_merged.to_sql("t_multitoken", connection, if_exists="append", index=False, method=insert_with_ignore)
                    toc = time.perf_counter()
                    message = f"<strong>STORE</strong> - Transfers ERC1155...<strong>{toc - tic:0.4f}</strong> seconds"
                    logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                    data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"
                except Exception:
                    traceback.print_exc()
                    traceback_text = traceback.format_exc()

                    connection.close()
                    message = "<strong>Error...</strong>"
                    logger.error(message.replace("<strong>", "").replace("</strong>", ""))
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                    yield f"data:{data}\n\n"

                    for line in traceback_text.splitlines():
                        message = f"{line}"
                        logger.warning(f"{message}")
                        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
                        yield f"data:{data}\n\n"

                    message = " "
                    logger.warning(f"{message}")
                    data = json.dumps({"msg": f"{message}", "end": True, "error": True, "content": {}})
                    yield f"data:{data}\n\n"

                # INFO: Generating internals tags
                tic = time.perf_counter()
                # db_store_tagging(connection, address, json_object, json_transfers, json_internals)
                db_store_tagging_opt(connection, address, json_object, json_transfers, json_internals, json_nfts, json_multitokens)
                toc = time.perf_counter()
                message = f"<strong>STORE</strong> - Tagging...<strong>{toc - tic:0.4f}</strong> seconds"
                logger.info(message.replace("<strong>", "").replace("</strong>", ""))
                data = json.dumps({"msg": message, "end": False, "error": False, "content": {}})
                yield f"data:{data}\n\n"

                # INFO: KKK - Store nodes and links for classification
                trxs = store_nodes_links_db(
                    connection, address, params, df_trx_store, df_internals_store, df_transfers_store, df_nfts_store, df_multitoken_store
                )
                trxs = get_nodes_links_bd(connection, address, params)

        # INFO: Exist information in db
        else:
            # HACK: This is not a good place to continue collecting information if it is not complete
            logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")
            logger.debug("+ Already collected                                +")
            logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")

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

        logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")
        logger.debug(f" !!!!!! Wallet detail: {wallet_detail}")
        logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")

        message = "<strong>TRANSACTIONS</strong> - Receiving wallet detail information..."
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"wallet_detail": wallet_detail}})
        yield f"data:{data}\n\n"

        # INFO: Get address and path
        query = "SELECT * FROM t_tags WHERE tag = 'central'"  # INFO: Here no need blockchain, only one central must exist
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

        message = "<strong>DATA</strong> - Received central and path addresses cached data..."
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"address": address, "addresses": addresses}})
        yield f"data:{data}\n\n"

        # INFO: Get trxs, internals and transfers
        message = "<strong>DATA</strong> - Getting collected trxs, internals, transfers..."
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        tic = time.perf_counter()
        if params["graph"] == "Complex":
            # trxs = get_trx_from_addresses_experimental(connection, address, params)
            trxs = get_nodes_links_bd(connection, address, params)
        else:
            trxs = get_trx_from_addresses_opt(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Proccesed...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({
            "msg": f"{message}",
            "end": False,
            "error": False,
            "content": {"graph": trxs["transactions"], "list": trxs["list"], "stat": trxs["stat"]},
        })
        yield f"data:{data}\n\n"

        # INFO: Get Funders and creators
        tic = time.perf_counter()
        funders = get_funders_creators(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Funders and creators...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"funders": funders}})
        yield f"data:{data}\n\n"

        # INFO: Get Balance and Gas
        tic = time.perf_counter()
        balance = get_balance_and_gas(connection, address, type, key)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Balance and Gas...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({
            "msg": f"{message}",
            "end": False,
            "error": False,
            "content": {"balance": balance["balance"], "tokens": balance["tokens"], "gas": balance["gas"]},
        })
        yield f"data:{data}\n\n"

        # INFO: Get tags and labels
        tic = time.perf_counter()
        tags = get_tags_labels(connection, address)
        toc = time.perf_counter()
        message = f"<strong>DATA</strong> - Tags and Labels...<strong>{toc - tic:0.4f}</strong> seconds"
        logger.info(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"tags": tags["tags"], "labels": tags["labels"]}})
        yield f"data:{data}\n\n"

        message = "<strong>End collection</strong>"
        logger.error(message.replace("<strong>", "").replace("</strong>", ""))
        data = json.dumps({"msg": f"{message}", "end": True, "error": False, "content": {}})
        yield f"data:{data}\n\n"


def recreate_db(params):
    # Checking wallet and first trx
    dbname = params["config"]["dbname"]
    address = params.get("address")
    connection = sqlite3.connect(dbname)

    address = ("eth", address, "central")
    # trxs = get_trx_from_address(connection, address)

    tic = time.perf_counter()
    # data = get_trx_from_addresses_experimental(connection, address, params=params)
    # data = get_balance_and_gas(connection, address, "wallet", "")

    # NOTE:
    # Simulate graphic gathering from initial db
    with open(r"./config.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    config["action"] = "reset"
    print(config)
    data = misc.event_stream_checking(config)  # WARN: Recreate db

    return data


def test_function_1(params):
    # Checking wallet and first trx
    dbname = params["config"]["dbname"]
    address = params.get("address")
    connection = sqlite3.connect(dbname)

    address = ("eth", address, "central")
    # trxs = get_trx_from_address(connection, address)

    tic = time.perf_counter()
    # data = get_trx_from_addresses_experimental(connection, address, params=params)
    # data = get_balance_and_gas(connection, address, "wallet", "")

    # NOTE:
    # Simulate graphic gathering from initial db
    with open(r"./config.yaml") as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    params["graph"] = "complex"
    params["network"] = "eth"
    params["config"] = config
    params["address"] = address[1]
    print(params)
    data = event_stream_ether(params)

    toc = time.perf_counter()
    logger.info(f"Execute test_1 in {toc - tic:0.4f} seconds")

    return data


def test_function_2(params):
    # Checking wallet and first trx
    dbname = params["config"]["dbname"]
    address = params.get("address")
    connection = sqlite3.connect(dbname)

    address = ("eth", address, "central")

    tic = time.perf_counter()
    # data = get_trx_from_addresses_opt_bkp(connection, address)
    # data = get_funders_creators(connection, address)
    # data = get_trx_from_addresses_research(connection, address, params=params)
    data = get_nodes_links_bd(connection, address, params=params)
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
        df_t = pd.DataFrame(
            columns=[
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        )
    else:
        df_t = pd.DataFrame(trxs)
    if not transfers:
        df_f = pd.DataFrame(
            columns=[
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        )
    else:
        df_f = pd.DataFrame(transfers)
    if not internals:
        df_i = pd.DataFrame(
            columns=[
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        )
    else:
        df_i = pd.DataFrame(internals)
    if not nfts:
        df_n = pd.DataFrame(
            columns=[
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        )
    else:
        df_n = pd.DataFrame(nfts)
    if not multitoken:
        df_m = pd.DataFrame(
            columns=[
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        )
    else:
        df_m = pd.DataFrame(multitoken)

    # Safe concat
    address_columns = ["from", "to", "contractAddress"]
    # addresses = pd.concat([df[column].dropna().unique() for df in [df_t, df_f, df_i, df_n, df_m] for column in address_columns if column in df.columns])
    addresses = pd.concat([
        pd.Series(df[column].dropna().unique()) for df in [df_t, df_f, df_i, df_n, df_m] for column in address_columns if column in df.columns
    ])

    df_addresses = pd.DataFrame(addresses.unique(), columns=["address"])
    df_addresses["blockChain"] = "eth"
    df_addresses["tag"] = "wallet"

    # Detect contracts
    # FIX: This should be more complex
    # - If address is contract, determine contract to contract (?)
    contracts = pd.concat([
        df_t.loc[(df_t["input"] != "0x") & (df_t["from"] != address), "from"],
        df_t.loc[(df_t["input"] != "0x") & (df_t["to"] != address), "to"],
        # df_t.loc[df_t['input'] != '', 'to'],
        df_f["contractAddress"].dropna(),
        df_n["contractAddress"].dropna(),
        df_m["contractAddress"].dropna(),
    ]).unique()

    # TODO: NTFs and Multitoken should be separated and tagged like "contract nft" and "contract multitoken" respectively

    df_addresses.loc[df_addresses["address"].isin(contracts), "tag"] = "contract"

    # INFO: Funders or creators
    # TODO: Evaluate type to distinguish Funders and creators
    # df_all = pd.concat([df[['from', 'to', 'timeStamp']] for df in [df_t, df_f, df_i] if 'timeStamp' in df.columns], ignore_index=True).sort_values(by='timeStamp')
    df_all = pd.concat(
        [df[["from", "to", "timeStamp"]] for df in [df_t, df_f, df_i, df_n, df_m] if "timeStamp" in df.columns], ignore_index=True
    ).sort_values(by="timeStamp")
    min_timestamp = df_all.loc[df_all["from"] == address, "timeStamp"].min()
    funders_addresses = df_all.loc[df_all["timeStamp"] < min_timestamp, "from"].unique()

    df_funders = pd.DataFrame(funders_addresses, columns=["address"])
    df_funders["blockChain"] = "eth"
    df_funders["tag"] = "funder"

    # Unifying tags
    df_addresses = pd.concat([df_addresses, df_funders]).drop_duplicates().reset_index(drop=True)

    # Insert tags in SQLite
    cursor = connection.cursor()
    insert_tag = "INSERT OR IGNORE INTO t_tags (address, blockChain, tag) VALUES (?, ?, ?)"
    cursor.executemany(insert_tag, df_addresses.to_records(index=False))
    connection.commit()

    # Store trxs Funders
    funders = df_funders["address"].tolist()
    df_t_f = (
        df_t.loc[(df_t["to"] == address) & (df_t["from"].isin(funders)) & (df_t["timeStamp"] < min_timestamp)]
        .assign(type="transaction")
        .assign(blockChain="eth")
        .assign(tokenDecimal=18)
        .assign(tokenSymbol="ETH")
        .assign(tokenName="Ether")[
            [
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        ]
    )
    df_i_f = (
        df_i.loc[(df_i["to"] == address) & (df_i["from"].isin(funders)) & (df_i["timeStamp"] < min_timestamp)]
        .assign(type="internal")
        .assign(blockChain="eth")
        .assign(tokenDecimal=18)
        .assign(tokenSymbol="ETH")
        .assign(tokenName="Ether")[
            [
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        ]
    )
    df_f_f = (
        df_f.loc[(df_f["to"] == address) & (df_f["from"].isin(funders)) & (df_f["timeStamp"] < min_timestamp)]
        .assign(type="transfer")
        .assign(blockChain="eth")[
            [
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        ]
    )
    # INFO: Neccesary rename to avoid add columns to funders
    df_n.rename(columns={"tokenID": "value"}, inplace=True)
    df_n_f = (
        df_n.loc[(df_n["to"] == address) & (df_n["from"].isin(funders)) & (df_n["timeStamp"] < min_timestamp)]
        .assign(type="nfts")
        .assign(blockChain="eth")[
            [
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        ]
    )
    # INFO: Neccesary rename to avoid add columns to funders
    df_m.rename(columns={"tokenID": "value"}, inplace=True)
    df_m.rename(columns={"tokenValue": "tokenDecimal"}, inplace=True)
    df_m_f = (
        df_m.loc[(df_m["to"] == address) & (df_m["from"].isin(funders)) & (df_m["timeStamp"] < min_timestamp)]
        .assign(type="nfts")
        .assign(blockChain="eth")[
            [
                "blockChain",
                "blockNumber",
                "type",
                "timeStamp",
                "hash",
                "from",
                "to",
                "value",
                "input",
                "contractAddress",
                "tokenDecimal",
                "tokenSymbol",
                "tokenName",
            ]
        ]
    )

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

    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug(f"+ Address: {address_central}")
    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")

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
    tags_grouped = df_tags.groupby("address")["tag"].apply(list).reset_index(name="tags")
    tags_dict = pd.Series(tags_grouped.tags.values, index=tags_grouped.address).to_dict()

    # INFO: Labels
    df_labels = pd.read_sql_query("SELECT * FROM t_labels WHERE blockChain = 'ethereum'", conn)  # TODO: Multichain
    labels_dict = df_labels.set_index("address").apply(lambda row: row.to_dict(), axis=1).to_dict()

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
    df_all["timeStamp"] = pd.to_datetime(df_all["timeStamp"], unit="s")
    stat_err = len(df_all[df_all["isError"] != 0])
    df_all = df_all[df_all["isError"] == 0]

    nodes = {}
    links = {}

    for _, row in df_all.iterrows():
        from_address = row["from"]
        to_address = row["to"]
        symbol = row["symbol"]
        value = float(row["valConv"])
        type = row["type"]

        for address in [from_address, to_address]:
            if address not in nodes:
                tag = tags_dict.get(address, [])  # Get tag
                stat_wal += "wallet" in tag
                stat_con += "contract" in tag
                label = labels_dict.get(address, [])  # Get label
                nodes[address] = {
                    "id": address,
                    "address": address,
                    # "tag": [tag] if tag else [],
                    "tag": tag,
                    "label": label,
                    "blockchain": "eth",
                    "token": "ETH",
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
    list_trans = df_all.loc[(df_all["from"] == address_central) | (df_all["to"] == address_central)].to_json(orient="records")

    stat_trx = len(df_all[df_all["type"] == "transaction"])
    stat_int = len(df_all[df_all["type"] == "internals"])
    stat_tra = len(df_all[df_all["type"] == "transfers"])
    stat_tot = len(df_all)
    # stat_wal = len(df_tags[df_tags['tag'] == 'wallet'])
    # stat_con = len(df_tags[df_tags['tag'] == 'contract'])
    # TODO: Stat of wallets and contracts

    stat = {
        "stat_trx": int(stat_trx),
        "stat_int": int(stat_int),
        "stat_tra": int(stat_tra),
        "stat_wal": int(stat_wal),
        "stat_tot": int(stat_tot),
        "stat_con": int(stat_con),
        "stat_err": int(stat_err),
        "stat_coo": int(stat_con),
    }  # FIX: repeating stat_con in stat_coo

    return {"transactions": transactions, "list": list_trans, "stat": stat}


def get_balance_and_gas(conn, address_central, type, key):
    # INFO: address_central is
    # ('eth', '0x7f3acf451e372f517b45f9d2ee0e42e31bc5e53e', 'central')
    # For future Multichain (?)

    address_central = address_central[1]

    if type == "wallet":
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
        index = df_balance.index[df_balance["token"] == "ETH"].tolist()
        balance = df_balance[df_balance["token"] == "ETH"]

        if (balance.loc[index[0], "balance"] - gas) >= 0:
            balance.loc[index[0], "balance"] -= gas
        else:
            balance.loc[index[0], "balance"] = 0

        # balance.loc[index, 'balance'] -= gas
        # if (balance.loc[index, 'balance'] < 0):
        #     balance.loc[index, 'balance'] = 0.0
        balance = json.loads(balance[balance["token"] == "ETH"].to_json(orient="records"))

        # Remove row from tokens
        df_balance.drop(index, inplace=True)
        tokens = json.loads(df_balance.to_json(orient="records"))

        return {"balance": balance, "tokens": tokens, "gas": gas}

    else:
        # INFO: Get balance of contract
        url = f"https://api.etherscan.io/api?module=account&action=balance&address={address_central}&tag=latest&apikey={key}"
        # print(f"KEY: {key}")
        response = requests.get(url)
        json_object = response.json()["result"]
        # print(f"BALANCE: {json_object}")
        balance = [{"blockChain": "eth", "balance": int(json_object) / 1e18, "token": "ETH", "tokenName": "Ether"}]
        # TODO: Use scrapping to get all of tokens balance

        return {"balance": balance, "tokens": [], "gas": []}


def get_funders_creators(conn, address_central):
    address_central = address_central[1]

    # INFO: Get Funders
    query = f"SELECT * FROM t_funders_creators WHERE `to` = '{address_central}';"
    df_funders_creators = pd.read_sql_query(query, conn).to_json(orient="records")

    return {"funders": df_funders_creators}


def get_tags_labels(conn, address_central):
    address_central = address_central[1]

    # PERF: I think that need to remove the blockchain condition

    # INFO: Get Tags
    query = f"SELECT * FROM t_tags WHERE blockChain = 'eth' and address = '{address_central}';"
    df_tags = json.loads(pd.read_sql_query(query, conn).to_json(orient="records"))

    # INFO: Get Labels
    query = f"SELECT * FROM t_labels WHERE blockChain = 'ethereum' and address = '{address_central}';"
    df_labels = json.loads(pd.read_sql_query(query, conn).to_json(orient="records"))

    return {"tags": df_tags, "labels": df_labels}


def store_nodes_links_db(conn, address_central, params=[], df_trx=[], df_int=[], df_trf=[], df_nft=[], df_mul=[]):
    address_central = address_central.lower()
    nodes = {}
    links = {}
    stat_tot = 0
    stat_trx = 0
    stat_int = 0
    stat_tra = 0
    # stat_err = 0
    stat_con = 0
    stat_wal = 0
    # stat_coo = 0  # TODO: Contract owned
    # max_qty = 0

    # NOTE: Get nodes and links in db
    cursor = conn.cursor()
    cursor.execute("SELECT address FROM t_nodes_classification")
    nodes_sql = cursor.fetchall()
    nodes_db = [address[0] for address in nodes_sql]
    # print(nodes_db)
    cursor.execute("SELECT link_key FROM t_links_classification")
    links_sql = cursor.fetchall()
    links_db = [lk[0] for lk in links_sql]
    # print(links_db)

    # INFO: Auxiliar function to add nodes to dict
    def add_nodes(address, tag, label, contract=False):
        nonlocal nodes  # HACK: It is not local to this function
        nonlocal nodes_db  # HACK: It is not local to this function
        nonlocal stat_wal  # HACK: It is not local to this function
        nonlocal stat_con  # HACK: It is not local to this function

        if address not in nodes:
            if contract:
                tag.append("contract")
                stat_con += 1
            else:
                tag.append("wallet")
                stat_wal += 1

            nodes[address] = {"id": address, "address": address, "tag": tag, "label": label}
        return True

    # INFO: Auxiliar function to add links to dict
    def add_link(from_address, to_address, symbol, name, contract, value, action, type, node_create=True):
        nonlocal links  # HACK: It is not local to this function
        nonlocal links_db  # HACK: It is not local to this function
        nonlocal address_central  # HACK: It is not local to this function

        if node_create:
            # Nodes
            for node_address in [from_address, to_address]:
                if (node_address not in nodes) and (node_address not in nodes_db):
                    tag = tags_dict.get(node_address, [])  # Get tag
                    label = labels_dict.get(node_address, [])  # Get label
                    if node_address == address_central:
                        resp = add_nodes(node_address, tag, label, contract=False)
                    else:
                        resp = add_nodes(node_address, tag, label, contract=True)

        if type == "nfts":  # NOTE: Add nfts condition
            key = f"{from_address}->{to_address}-{value}"
            # if action == "mint nft":
            #     __import__('pdb').set_trace()
            # print(key)
        elif type == "multitokens":  # TODO: Add multitoken nfts condition. How?
            key = f"{from_address}->{to_address}-{symbol}"
        else:
            key = f"{from_address}->{to_address}-{symbol}"

        # key = (from_address, to_address, symbol)
        # if (key not in links) and (key not in links_db):
        if key not in links:
            links[key] = {}
            links[key]["link_key"] = key
            links[key]["source"] = from_address
            links[key]["target"] = to_address
            links[key]["symbol"] = symbol
            links[key]["name"] = name
            links[key]["contract"] = contract
            links[key]["count"] = 1
            links[key]["sum"] = value
            links[key]["action"] = [action]
            links[key]["type"] = type
        else:
            links[key]["count"] += 1
            links[key]["sum"] += value
            if action not in links[key]["action"]:
                links[key]["action"].append(action)
        return True

    tic = time.perf_counter()

    # INFO: Config Log Level
    if params:
        log_format = "%(asctime)s [%(filename)s:%(lineno)d] %(levelname)s %(message)s"
        coloredlogs.install(level=params["config"]["level"], fmt=log_format, logger=logger)
        logger.propagate = False  # INFO: To prevent duplicates with flask
        address_central = params["address"].lower()

    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug(f"+ Address: {address_central}")
    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")

    data_frames = {"df_trx": df_trx, "df_int": df_int, "df_trf": df_trf, "df_nft": df_nft, "df_mul": df_mul}

    # INFO: Tagging: Here exclude wallets and contracts
    df_tags = pd.read_sql_query("SELECT address, tag FROM t_tags WHERE tag NOT IN ('wallet', 'contract')", conn)
    tags_grouped = df_tags.groupby("address")["tag"].apply(list).reset_index(name="tags")
    tags_dict = pd.Series(tags_grouped.tags.values, index=tags_grouped.address).to_dict()

    # INFO: Labels
    df_labels = pd.read_sql_query("SELECT * FROM t_labels WHERE blockChain = 'ethereum'", conn)
    labels_dict = df_labels.set_index("address").apply(lambda row: row.to_dict(), axis=1).to_dict()

    # INFO: Get all Trx, Transfers, internals, nfts and multitoken
    query = """
        SELECT blockChain, type, hash, `from`, `to`, value, contractAddress, symbol, name, decimal, valConv, timeStamp, isError, methodId, functionName
        FROM (
            SELECT df_trx.blockChain, 'transaction' as 'type', df_trx.hash, df_trx.`from`, df_trx.`to`, df_trx.value, df_trx.contractAddress, 
            'ETH' as 'symbol', 'Ether' as 'name', 18 as 'decimal', df_trx.value / POWER(10, 18) as valConv, df_trx.timeStamp, df_trx.isError, 
            df_trx.methodId, df_trx.functionName
            FROM df_trx
            UNION ALL
            SELECT df_int.blockChain, 'internals', df_int.hash, df_int.`from`, df_int.`to`, df_int.value, 
                df_int.contractAddress, 'ETH', 'Ether', 18, df_int.value / POWER(10, 18), df_int.timeStamp, df_int.isError, '0x', ''
            FROM df_int
            UNION ALL
            SELECT df_trf.blockChain, 'transfers', df_trf.hash, df_trf.`from`, df_trf.`to`, df_trf.value,
                df_trf.contractAddress, df_trf.tokenSymbol, df_trf.tokenName, df_trf.tokenDecimal, df_trf.value / POWER(10, df_trf.tokenDecimal), 
                df_trf.timeStamp, 0, '0x', ''
            FROM df_trf
            UNION ALL
            SELECT df_nft.blockChain, 'nfts', df_nft.hash, df_nft.`from`, df_nft.`to`, df_nft.tokenID,
                df_nft.contractAddress, df_nft.tokenSymbol, df_nft.tokenName, df_nft.tokenDecimal, df_nft.tokenID, df_nft.timeStamp, 0, '0x', ''
            FROM df_nft
            UNION ALL
            SELECT df_mul.blockChain, 'multitoken', df_mul.hash, df_mul.`from`, df_mul.`to`, df_mul.tokenID,
                df_mul.contractAddress, df_mul.tokenSymbol, df_mul.tokenName, df_mul.tokenValue, df_mul.tokenID, df_mul.timeStamp, 0, '0x', ''
            FROM df_mul
        ) AS combined
        ORDER BY timeStamp ASC
    """

    # Ejecutar la consulta
    df_all = psql.sqldf(query, data_frames)

    # INFO: Convert to datetime
    df_all["decimal"] = df_all["decimal"].astype("int64")
    df_all["timeStamp"] = df_all["decimal"].astype("int64")
    df_all["isError"] = df_all["isError"].astype("int64")
    df_all["timeStamp"] = pd.to_datetime(df_all["timeStamp"], unit="s")
    stat_err = len(df_all[df_all["isError"] != 0])
    df_all = df_all[df_all["isError"] == 0]

    # for index, row in df_all.iterrows():
    grouped = df_all.groupby("hash")

    inc = 0

    for hash, group in grouped:
        group_size = len(group)
        has_transaction = "transaction" in group["type"].values

        # INFO: Complete transaction and pure =======================================
        if (group_size == 1) and (has_transaction):
            # logger.info(colored(f"== SIMPLE ========================================", 'blue'))
            for _, row in group.iterrows():
                from_address = row["from"]
                to_address = row["to"]
                symbol = row["symbol"]
                name = row["name"]
                value = float(row["valConv"])
                function = row["functionName"]

                #     logger.debug(colored(f"== DETAIL\nTYPE: {row['type']} - HASH: {hash}\nXFROM: {row['from']} -> XTO: {row['to']} " +
                #                          f"<--> CONTRACT: {row['contractAddress']}\nXVALUE: {row['valConv']} - XSYMBOL: {row['symbol']} " +
                #                          f"- XMETHOD: {row['methodId']} - XFUNC: {row['functionName']}", 'blue'))
                action = ""
                if (float(row["value"]) != 0.0) and (row["functionName"] == ""):
                    # logger.info(colored(f"++ ETHER MOVE ====================================", 'blue'))   # NOTE: Checked
                    action = "ether move"
                elif row["from"] == row["to"]:
                    # logger.info(colored(f"++ SELF-DEPOSIT ==================================", 'blue'))   # NOTE: Checked
                    action = "self deposit"
                elif (float(row["value"]) != 0.0) and ("deposit" in row["functionName"]):
                    # logger.info(colored(f"++ DEPOSIT =======================================", 'blue'))   # NOTE: Checked
                    action = "deposit"
                # elif (float(row['value']) == 0.0) and (row['functionName'] != ''):
                elif row["functionName"] != "":  # 0xfb09c6e73cccf49307bb0f81428dc9ebbbb93699c5834c5a71e5aa8a5220229d
                    # TODO: Nest to distinguish differents functions (?)
                    # logger.info(colored(f"++ CONTRACT EXECUTION ============================", 'blue'))   # NOTE: Checked
                    action = "contract execution"
                elif (float(row["value"]) == 0.0) and (
                    row["functionName"] == ""
                ):  # 0x9ea05e6effbd5deea270a5296fd3e88e2653cefdff177296a487efe71b6fd728
                    # logger.info(colored(f"++ DO NOTHING ====================================", 'blue'))   # TODO: Checked
                    action = "do nothing"
                else:
                    logger.error(f"++ NOT DETECTED = {row['type']} ==================")

                # Nodes
                node_address = from_address
                if (node_address not in nodes) and (node_address not in nodes_db):
                    tag = tags_dict.get(node_address, [])  # Get tag
                    label = labels_dict.get(node_address, [])  # Get label
                    resp = add_nodes(node_address, tag, label, contract=False)

                node_address = to_address
                if (node_address not in nodes) and (node_address not in nodes_db):
                    tag = tags_dict.get(node_address, [])  # Get tag
                    label = labels_dict.get(node_address, [])  # Get label
                    if function != "":
                        resp = add_nodes(node_address, tag, label, contract=True)
                    else:
                        resp = add_nodes(node_address, tag, label, contract=False)

                # Links
                add_link(from_address, to_address, symbol, name, "", value, action, row["type"], node_create=False)

            # # logger.info(colored(f"==================================================\n", 'black'))

        # INFO: OTHERS
        elif (group_size > 1) and (has_transaction):
            logger.debug(colored("== COMPLEX =======================================", "cyan"))
            xfrom_address = xto_address = xsymbol = xname = xfunc = xtype = xparam = ""
            xvalue = 0
            for _, row in group.iterrows():
                if row["type"] == "transaction":
                    xtype = row["type"]
                    xfrom_address = row["from"]
                    xto_address = row["to"]
                    xcontract = row["contractAddress"]
                    xvalue = row["valConv"]
                    xsymbol = row["symbol"]
                    xname = row["name"]
                    if row["functionName"]:
                        xfunc = row["functionName"].split("(")[0]
                        xparam = row["functionName"].split("(")[1]
                    # logger.debug(
                    #     colored(
                    #         f"== DETAIL\nTYPE: {row['type']} - HASH: {hash}\nXFROM: {xfrom_address} -> XTO: {xto_address} "
                    #         + f"<--> CONTRACT: {xcontract}\nXVALUE: {xvalue} - XSYMBOL: {xsymbol} "
                    #         + f"- XMETHOD: {row['methodId']} - XFUNC: {xfunc}-{xparam}",
                    #         "cyan",
                    #     )
                    # )
                else:
                    # logger.debug(colored(f"   ERC - {row['type']} =============================== \n" +
                    #                      f"FROM: {row['from']} -> TO: {row['to']} <-> CON: {row['contractAddress']}\n" +
                    #                      f"VALUE: {row['valConv']} - SYMBOL: {row['symbol']} - FUNC: {row['functionName']}", 'cyan'))
                    from_address = row["from"]
                    to_address = row["to"]
                    contract = row["contractAddress"]
                    value = row["valConv"]
                    symbol = row["symbol"]
                    name = row["name"]

                    # INFO: Internals
                    if row["type"] == "internals":
                        if ("withdraw" in xfunc) and (xfrom_address == row["to"] == address_central) and (xto_address == row["from"]):
                            # 0x344bc8fcc078e736944f728d29f1a5a04303588c793417143e8f5852e5e04b22
                            # logger.debug(colored(f"++ WITHDRAW INTERNAL =(Do more research)==========", 'light_cyan'))  # NOTE: Checked
                            action = "withdraw internal (unwrap)"

                            # Links
                            add_link(xfrom_address, xto_address, xsymbol, xname, "", xvalue, action, xtype, node_create=True)
                            add_link(xto_address, xfrom_address, symbol, name, "", value, action, row["type"], node_create=False)

                        elif (
                            ("swap" in xfunc)
                            and (group.iloc[1]["from"] == group.iloc[-1]["to"])
                            and ((group["type"] == "internals").any())
                            and ((group["type"] == "transfers").any())
                        ):
                            # 0x26bae55868fed567c6f865259156ff1c56891f2c2bb87ba5cdfa1903d3823d18
                            # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            # NOTE: Group processed
                            action = "swap ether by token" if group.iloc[1]["from"] == address_central else "swap token by ether"
                            # logger.debug(colored(f"++ BUY TOKEN WITH ETHER ==========================", 'light_cyan'))  # NOTE: Checked

                            # Links
                            add_link(
                                group.iloc[1]["from"],
                                group.iloc[1]["to"],
                                group.iloc[1]["symbol"],
                                group.iloc[1]["name"],
                                group.iloc[1]["contractAddress"],
                                group.iloc[1]["valConv"],
                                action,
                                group.iloc[1]["type"],
                                node_create=True,
                            )
                            add_link(
                                group.iloc[-1]["from"],
                                group.iloc[-1]["to"],
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=False,
                            )
                            break

                        # WARN: REVIEW
                        elif (
                            ("swap" in xfunc or "multicall" in xfunc)
                            and (group.iloc[1]["to"] == group.iloc[-1]["to"])
                            and ((group["type"] == "internals").any())
                            and ((group["type"] == "transfers").any())
                            and (xvalue != 0)
                        ):  # 0xf9358c40ad6b71c12d33139504c462c73d822ff58aaf968374858e139da0740b
                            # 0x4a7e69831f25c741b0837ba008daf16d6a4782573729ccbe1ce9486c420c54f7
                            # print(f"GROUP")
                            # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            # NOTE: Group processed
                            # NOTE: There is a record “transfers” that is not shown but is in the group, due to the “break”.
                            action = "swap ether for token"
                            # logger.debug(colored(f"++ SWAP ETHER FOR TOKEN ==========================", 'light_cyan'))  # NOTE: Checked

                            # Links
                            add_link(xfrom_address, xto_address, xsymbol, xname, "", value, action, "transaction", node_create=True)
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=False,
                            )
                            break

                        elif (
                            ((group["type"] == "internals").any())
                            and ((group["type"] == "transfers").any())
                            and (
                                group.loc[group["type"] == "transaction", "from"].values[0]
                                == group.loc[group["type"] == "internals", "to"].values[0]
                                == group.loc[group["type"] == "transfers", "from"].values[0]
                            )
                        ):
                            #  0xf8f8a9326e6e6f7bcdaca207e4ece32998cb9022dfd61235939693d892c836d9
                            # print(f"{group}")
                            # print(f"{group[['type', 'from', 'to', 'value']]}")
                            # NOTE: group processed
                            # NOTE: there is a record “transfers” that is not shown but is in the group, due to the “break”.
                            action = "swap token by ether"
                            # logger.debug(colored(f"++ SWAP TOKEN BY ETHER ===========================", 'light_cyan'))  # NOTE: checked

                            # Links
                            add_link(xfrom_address, xto_address, xsymbol, xname, "", value, action, "transaction", node_create=True)
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=False,
                            )
                            break

                        elif (
                            (xfrom_address == row["to"] == address_central)
                            and (xvalue == 0)
                            and (group["type"] == "nfts").any()
                            and (group.iloc[-1]["from"] == address_central)
                        ):
                            # 0x11eca391dfa1b0ce3ab75a96de135234b752e89e9289919ca1d3b9922a0ae256
                            # logger.debug(colored(f"++ SWAP NFT FOR TOKEN INTERNAL =================", 'light_cyan'))  # TODO: Checked
                            action = "bid" if "bid" in xfunc.lower() else "not detected"

                            # Links
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[1]["symbol"],
                                group.iloc[1]["name"],
                                group.iloc[1]["contractAddress"],
                                group.iloc[1]["valConv"],
                                action,
                                group.iloc[1]["type"],
                                node_create=True,
                            )

                            # Node
                            node_address = group.iloc[-1]["to"]
                            if (node_address not in nodes) and (node_address not in nodes_db):
                                tag = tags_dict.get(node_address, [])  # Get tag
                                label = labels_dict.get(node_address, [])  # Get label
                                add_nodes(node_address, tag, label, contract=False)  # TODO: Verify that always is a wallet

                            add_link(
                                group.iloc[-1]["from"],
                                group.iloc[-1]["to"],
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                "nfts",
                                node_create=True,
                            )
                            break

                        elif (
                            ("purchase" in xfunc)
                            and (xfrom_address == row["to"] == address_central)
                            and (xvalue != 0)
                            and (group["type"] == "nfts").any()
                            and (group.iloc[-1]["from"] == "0x0000000000000000000000000000000000000000")
                        ):
                            # 0xe7bd55ddf0b6cd170b59f724ce2297a5c28d5837e7968a0885300970a4c8e7a7
                            # print(f"{group}")
                            # print(f"{group[['type', 'from', 'to', 'value']]}")
                            # NOTE: group processed
                            # NOTE: there is a record “nfts” that is not shown but is in the group, due to the “break”.
                            action = "purchase nft with ether"
                            # logger.debug(colored(f"++ PURCHASE NFT WITH ETHER =======================", 'light_cyan'))  # NOTE: Checked

                            # Links
                            add_link(xfrom_address, xto_address, xsymbol, xname, "", xvalue - value, action, "transaction", node_create=True)
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                "nfts",
                                node_create=False,
                            )
                            break

                        elif ("exit" in xfunc) and (xfrom_address == row["to"]) and (xvalue == 0):
                            # 0xef233f6abc71024c9894f3b83cb03c94a06efc1b3f7befac95017509a907b6f4
                            action = "bridging in"
                            # logger.debug(colored(f"++ BRIDGING (WITHDRAW) =(?)=======================", 'light_cyan'))  # NOTE: Checked - Bridging

                            # Links
                            add_link(row["from"], row["to"], row["symbol"], row["name"], "", row["valConv"], action, row["type"], node_create=True)

                        elif ("purchase" in xfunc) and (xfrom_address == row["to"]) and (xvalue != 0):  # NOTE: Generic
                            # ?
                            logger.debug(colored(f"++ {hash}", "red"))  # FIX: Checked
                            logger.debug(colored("++ PURCHASE WITH ETHER ===========================", "light_cyan"))  # TODO: Checked

                        elif xfrom_address == row["to"] == address_central:
                            # 0xc5d30d442ed9899304b6234230797cee8b0c0066407a5294a9531d758a2732c5
                            # WARN: Super generic
                            action = "Transfer ether to wa"
                            # logger.debug(colored(f"++ TRANSFER ETHER TO WA ===(Generic)============", 'light_cyan'))  # NOTE: Checked

                            # Links
                            add_link(row["from"], row["to"], row["symbol"], row["name"], "", row["valConv"], action, row["type"], node_create=True)

                        else:
                            logger.error(f"++ NOT DETECTED = {row['type']} ==================")
                            logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            break

                    # INFO: Transfers
                    elif row["type"] == "transfers":
                        # TODO: Exclude nfts and multitoken
                        if (
                            (float(xvalue) == 0.0)
                            and ("swap" in xfunc)
                            and (group.iloc[1]["from"] == group.iloc[-1]["to"] == address_central)
                            and (not (group["type"] == "multitoken").any() and (not (group["type"] == "nfts").any()))
                        ):
                            # 0xfff2a20407ec45aa55974a967da2fbb33d1a9590062570835f4afdcdf49ed52e
                            # WARN: Group processed
                            # print(f"{group[['from', 'to', 'value', 'contractAddress']]}")
                            action = "swap tokens"
                            # logger.debug(colored("++ SWAP TOKEN ====================================", "light_cyan"))  # NOTE: Checked

                            # # Nodes
                            # node_address = xfrom_address
                            # if (node_address not in nodes) and (node_address not in nodes_db):
                            #     tag = tags_dict.get(node_address, [])  # Get tag
                            #     label = labels_dict.get(node_address, [])  # Get label
                            #     if (node_address == address_central):
                            #         stat_con, stat_wal = add_nodes(node_address, tag, label, stat_con, stat_wal, contract=False)
                            #     else:
                            #         stat_con, stat_wal = add_nodes(node_address, tag, label, stat_con, stat_wal, contract=True)

                            # node_address = xto_address
                            # if (node_address not in nodes) and (node_address not in nodes_db):
                            #     tag = tags_dict.get(node_address, [])  # Get tag
                            #     label = labels_dict.get(node_address, [])  # Get label
                            #     if (node_address == address_central):
                            #         stat_con, stat_wal = add_nodes(node_address, tag, label, stat_con, stat_wal, contract=False)
                            #     else:
                            #         stat_con, stat_wal = add_nodes(node_address, tag, label, stat_con, stat_wal, contract=True)

                            # Links
                            add_link(
                                xfrom_address,
                                xto_address,
                                group.iloc[1]["symbol"],
                                group.iloc[1]["name"],
                                group.iloc[1]["contractAddress"],
                                group.iloc[1]["valConv"],
                                action,
                                group.iloc[1]["type"],
                                node_create=True,
                            )
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=False,
                            )
                            break
                        # elif (float(xvalue) == 0.0) and ("atomicMatch" in xfunc) and (group.iloc[1]['from'] == group.iloc[-1]['to']) and ((group['type'] == 'nfts').any()):
                        #     print(f"GROUP")
                        #     print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                        #     # WARN: Group processed
                        #     logger.debug(colored(f"++ TRANSFER TOKEN BY NFT =========================", 'light_cyan'))  # TODO: Determine TOKEN BY NFT or NFT BY TOKEN
                        #     break
                        elif (
                            (float(xvalue) == 0.0)
                            and (group.iloc[1]["from"] == group.iloc[-1]["to"])
                            and ((group["type"] == "nfts").any())
                            and (row["to"] == address_central)
                            and (group.iloc[-1]["from"] != "0x0000000000000000000000000000000000000000")
                        ):
                            # 0xf1b8c703a12b2f3f9c582720d2b7cb0052c042743f74a912e625ec4208c75ce4
                            # print(f"GROUP")
                            # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            # WARN: Group processed
                            action = "sell nft to wallet"
                            # logger.debug(colored("++ SELL NFT TO WALLET =(G)========================", "light_cyan"))  # NOTE: Checked

                            # Links
                            add_link(
                                group.iloc[1]["from"],
                                group.iloc[1]["to"],
                                group.iloc[1]["symbol"],
                                group.iloc[1]["name"],
                                group.iloc[1]["contractAddress"],
                                group.iloc[1]["valConv"],
                                action,
                                group.iloc[1]["type"],
                                node_create=True,
                            )
                            add_link(
                                group.iloc[-1]["from"],
                                group.iloc[-1]["to"],
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                "nfts",
                                # group.iloc[-1]["type"],
                                node_create=False,
                            )
                            break

                        elif (float(xvalue) == 0.0) and ("borrow" in xfunc) and (xfrom_address == group.iloc[-1]["to"]):
                            # 0xfcf7f2cfc1add7e3837c8d1ee1de783f5f792d08d26bf6403ba1e17c8e906d1c
                            # WARN: Group processed
                            action = "borrow"
                            # logger.debug(colored(f"++ BORROW TOKEN ==================================", "light_cyan"))  # NOTE: Checked

                            # Links
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=True,
                            )
                            break
                        # elif ((float(xvalue) == 0.0) and ("swap" in xfunc) and (group.iloc[1]["from"] == group.iloc[-1]["to"]) and ((group["type"] == "internals").any()) and ((group["type"] == "transfers").any())):  # NOTE: Checked
                        #     # print(f"GROUP")
                        #     # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                        #     # WARN: Group processed
                        #     # WARN: There is a record “internals” that is not shown but is in the group, due to the “break”.
                        #     logger.debug(colored(f"++ BUY TOKEN WITH ETHER ==========================", "light_cyan"))  # NOTE: Checked
                        #     break
                        elif ((group["type"] == "multitoken").any()) and (  # NOTE: NEW (Extend condition excluding nft (?))
                            (group["type"] == "transfers").any()
                        ):
                            # 0xf7557d9bf453561b48e523eba9ef8a364ea194e4b197c5cd50176ef108e5e06c
                            swap_token_by_multitoken = False
                            # print(f"GROUP")
                            # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            # WARN: Group processed
                            filter_df = group[group["type"].isin(["transfers", "multitoken"])].copy()
                            filter_df.loc[:, "group_key"] = filter_df.apply(lambda row: frozenset([row["from"], row["to"]]), axis=1)
                            grouped_dfs = {name: group.drop(columns="group_key") for name, group in filter_df.groupby("group_key")}
                            # print(grouped_dfs)
                            for key, grouped_df in grouped_dfs.items():
                                # print(f"\n\n{grouped_df[['type', 'from', 'to', 'value', 'contractAddress']]}\n\n")
                                if len(grouped_df) > 1:  # NOTE: swap transfer multitoken
                                    swap_token_by_multitoken = True
                                    action = "swap token by multitoken"

                                    # Links
                                    add_link(
                                        grouped_df.iloc[0]["from"],
                                        grouped_df.iloc[0]["to"],
                                        grouped_df.iloc[0]["symbol"],
                                        grouped_df.iloc[0]["name"],
                                        grouped_df.iloc[0]["contractAddress"],
                                        grouped_df.iloc[0]["valConv"],
                                        action,
                                        grouped_df.iloc[0]["type"],
                                        node_create=True,
                                    )
                                    add_link(
                                        grouped_df.iloc[-1]["from"],
                                        grouped_df.iloc[-1]["to"],
                                        grouped_df.iloc[-1]["symbol"],
                                        grouped_df.iloc[-1]["name"],
                                        grouped_df.iloc[-1]["contractAddress"],
                                        grouped_df.iloc[-1]["valConv"],
                                        action,
                                        grouped_df.iloc[-1]["type"],
                                        node_create=False,
                                    )
                                    # logger.debug(colored(f"++ SWAP TRANSFER MULTITOKEN ======================", "light_cyan"))  # TODO: Checked
                            if swap_token_by_multitoken:
                                pass
                                # logger.debug(colored(f"++ SWAP TRANSFER MULTITOKEN ======================", "light_cyan"))  # TODO: Checked
                            else:
                                logger.error(f"++ NOT DETECTED = {row['type']} = Multitoken =====")
                            break
                        elif (
                            (float(xvalue) != 0.0)
                            and ("swap" in xfunc)
                            and (xfrom_address == row["to"] == address_central)
                            and (xto_address != row["from"] != row["contractAddress"])
                        ):
                            # 0xe3553ba5a5c4d40d578bb3eaf5e04f0de935528f7ff24d302470e962ce342de1
                            # logger.debug(colored(f"++ SWAP ETHER TO TK ==============================", "light_cyan"))  # NOTE: Checked
                            action = "swap ether by token"

                            # Links
                            add_link(xfrom_address, xto_address, xsymbol, xname, "", xvalue, action, xtype, node_create=True)
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=False,
                            )
                        elif (
                            (float(xvalue) == 0.0)
                            and ("deposit" in xfunc)
                            and (xfrom_address == row["from"] == address_central)
                            and (xto_address == row["to"] != row["contractAddress"])
                        ):
                            # logger.debug(colored(f"++ DEPOSIT TOKEN =================================", "light_cyan"))  # NOTE: Checked
                            action = "deposit token"

                            # Links
                            add_link(
                                xto_address,
                                xfrom_address,
                                group.iloc[-1]["symbol"],
                                group.iloc[-1]["name"],
                                group.iloc[-1]["contractAddress"],
                                group.iloc[-1]["valConv"],
                                action,
                                group.iloc[-1]["type"],
                                node_create=True,
                            )
                        elif (
                            (xfrom_address == row["from"] == address_central)
                            and (xto_address == row["contractAddress"])
                            and (row["to"] != "0x0000000000000000000000000000000000000000")
                        ):
                            # 0xf01325b1b4c10b4cbe86c94cf65e459dde587b86bcdcbe50beaa8fa94df4b7e8
                            # logger.debug(colored(f"++ TRANSFER TK FROM WA ===========================", "light_cyan"))  # NOTE: Checked
                            action = "transfer tk from wa"

                            # NOTE: From wallet to wallet

                            # Nodes
                            node_address = row["to"]
                            if (node_address not in nodes) and (node_address not in nodes_db):
                                tag = tags_dict.get(node_address, [])  # Get tag
                                label = labels_dict.get(node_address, [])  # Get label
                                add_nodes(node_address, tag, label, contract=False)
                            # Links
                            add_link(
                                xfrom_address,
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (
                            (xfrom_address == row["from"] == address_central)
                            and (row["from"] != "0x0000000000000000000000000000000000000000")
                            and (row["to"] != "0x0000000000000000000000000000000000000000")
                            and ("liquidity" in xfunc.lower())
                        ):  # 0xf8c274a35c37916eb0cd52355f68ff68252b28181dec64c074c33a537371f688
                            # logger.debug(colored(f"++ LIQUIDITY FROM WA =============================", "light_cyan"))  # NOTE: Checked
                            action = "add liquidity"

                            # Links
                            add_link(
                                xfrom_address,
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (xfrom_address == row["to"] == address_central) and (
                            "0x0000000000000000000000000000000000000000" == row["from"]
                        ):  # 0xf8c274a35c37916eb0cd52355f68ff68252b28181dec64c074c33a537371f688
                            # logger.error(f"++ NOT NECESARY ================================")  # NOTE: Checked
                            pass

                        elif (
                            (xfrom_address == row["to"] == address_central)
                            and (xto_address == row["from"])
                            and (row["to"] != "0x0000000000000000000000000000000000000000")
                        ):  # 0xfe45d513dc4fc8fb844f7a4b4375b15e3e7ac0a1923fb8e9cf70b6428968a408
                            # logger.debug(colored(f"++ TRANSFER TK TO WA ===========================", "light_cyan"))  # NOTE: Checked
                            action = "transfer tk to wa"

                            # Links
                            add_link(
                                xfrom_address,
                                xto_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (xfrom_address == row["from"] == address_central) and (
                            "0x0000000000000000000000000000000000000000" == row["to"]
                        ):  # 0xed24c36025e354558cd0ff7969351757dbde659bdad0e006e3dfddce0f5a9e9f
                            # logger.error(f"++ NOT NECESARY ================================")  # TODO: Do it in the future
                            pass

                        elif (xfrom_address == row["to"] == address_central) and (
                            "exit" in xfunc
                        ):  # 0xf14dfe2372837a17b43451969d318308db1de93897e27c2669d7ea2a2f3fa393
                            # WARN: Determine if it's always a bridging
                            # logger.debug(colored(f"++ BRIDGING (?) ================================", "light_cyan"))  # TODO: Checked
                            action = "bridging in"

                            # Links
                            add_link(
                                xto_address,
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )
                            # WARN: Bridging out?

                        elif ("withdraw" in xfunc) and (xfrom_address == row["to"] == address_central) and (xto_address == row["from"]):
                            # 0xa467f35aa8a63fbb853ce751490e0fac24fd2933414ad54f15daad3ef78bce49
                            # logger.debug(colored(f"++ WITHDRAW TRANSFER =(Do more research)==========", "light_cyan"))  # TODO: Checked
                            action = "transfer tk to wa (withdraw)"

                            # Links
                            add_link(
                                xto_address,
                                xfrom_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif ("stake" in xfunc) and (xfrom_address == row["from"] == address_central):
                            # 0xe944230ad186b7849ef6e3fbf79e12ddcba2d08d525ce05572bf759ebd694bab
                            # logger.debug(colored(f"++ STAKE TOKEN ===================================", 'light_cyan'))  # NOTE: Checked
                            action = "staking"

                            # Links
                            add_link(
                                xfrom_address,
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif xfrom_address == row["to"] == address_central:
                            # 0xdfe9f6c611b98a1b9c8fb1573152b02a82a69a1cb5437847357f89722c84ba41
                            # WARN: Super generic
                            # logger.debug(colored(f"++ TRANSFER TK TO WA ======(Generic)============", 'light_cyan'))  # NOTE: Checked
                            action = "transfer tk to wa (generic)"

                            # Links
                            add_link(
                                xto_address,
                                xfrom_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif xfrom_address == row["from"] == address_central:
                            # 0xe14b6581a3f101a9dc0de191662b5f5bec4b39d3fe391605527853040e1a2a00
                            # WARN: Super generic
                            # logger.debug(colored(f"++ TRANSFER TK FROM WA ====(Generic)============", "light_cyan"))  # NOTE: Checked
                            action = "transfer tk from wa (generic)"

                            # Links
                            add_link(
                                xfrom_address,
                                xto_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        else:
                            logger.error(f"++ NOT DETECTED = {row['type']} ==================")
                            logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            break

                    # INFO: Nfts
                    elif row["type"] == "nfts":
                        # if (
                        #     (float(xvalue) == 0.0)
                        #     and ("atomicMatch" in xfunc)
                        #     and (group.iloc[1]["from"] == group.iloc[-1]["to"])
                        #     and ((group["type"] == "transfers").any())
                        # ):
                        #     # print(f"GROUP")
                        #     # print(f"{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                        #     # WARN: Group processed
                        #     logger.debug(colored(f"++ TRANSFER TOKEN BY NFT =================", "light_cyan"))  # TODO: Checked
                        #     # TODO: Determine TOKEN BY NFT or NFT BY TOKEN
                        #     break
                        # elif (xfrom_address == row["to"] == address_central) and (row["from"] == "0x0000000000000000000000000000000000000000"):
                        if (xfrom_address == row["to"] == address_central) and (row["from"] == "0x0000000000000000000000000000000000000000"):
                            # 0xe38cd8174c029b1d0b452a499bb9e62de2402319fc9f9514ff5059c79d220119
                            # logger.debug(colored(f"++ MINT NFT ======================================", "light_cyan"))  # NOTE: Checked
                            action = "mint nft"
                            add_link(
                                xto_address,
                                xfrom_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (xfrom_address == row["from"] == address_central) and (
                            row["to"] == "0x0000000000000000000000000000000000000000"
                        ):  # 0x83a7a3f749d400f3d33adee0e4a055901015b8c7dda9bfdac0706183f4001071
                            # logger.debug(colored(f"++ BURN NFT ======================================", "light_cyan"))  # NOTE: Checked
                            action = "burn nft"

                            add_link(
                                xfrom_address,
                                xto_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (
                            (xfrom_address == row["from"] == address_central)
                            and (xto_address == row["contractAddress"])
                            and (row["to"] != "0x0000000000000000000000000000000000000000")
                        ):
                            # logger.debug(colored(f"++ TRANSFER NFT FROM WA ==========================", "light_cyan"))  # NOTE: Checked
                            action = "transfer nft from wa"
                            # WARN: Is always the "to" a wallet
                            # Nodes (We must create a wallet node)
                            node_address = row["to"]
                            if (node_address not in nodes) and (node_address not in nodes_db):
                                tag = tags_dict.get(node_address, [])  # Get tag
                                label = labels_dict.get(node_address, [])  # Get label
                                add_nodes(node_address, tag, label, contract=False)

                            add_link(
                                row["from"],
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (xfrom_address == row["from"] == address_central) and (row["to"] != "0x0000ea00000000000000000000000000000000000"):
                            # WARN: Similar to previous block but between particulars
                            # logger.debug(colored(f"++ TRANSFER NFT FROM WA =(Particulars)============", "light_cyan"))  # NOTE: Checked
                            action = "transfer nft from wa (marketplace)"
                            # WARN: Is always the "to" a wallet
                            # Nodes (We must create a wallet node)
                            node_address = row["to"]
                            if (node_address not in nodes) and (node_address not in nodes_db):
                                tag = tags_dict.get(node_address, [])  # Get tag
                                label = labels_dict.get(node_address, [])  # Get label
                                add_nodes(node_address, tag, label, contract=False)

                            add_link(
                                row["from"],
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        elif (
                            (xfrom_address == row["to"] == address_central)
                            and (xto_address != row["to"] != row["contractAddress"])
                            and (row["from"] != "0x0000000000000000000000000000000000000000")
                        ):
                            # logger.debug(colored(f"++ BUY NFT ETH ===================================", "light_cyan"))  # NOTE: Checked
                            action = "buy nft with eth"

                            add_link(xfrom_address, xto_address, xsymbol, xname, "", xvalue, action, xtype, node_create=True)
                            add_link(
                                xto_address,
                                xfrom_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                row["type"],
                                node_create=True,
                            )

                        else:
                            logger.error(f"++ NOT DETECTED = {row['type']} ==================")
                            logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            break

                    #         # INFO: Multitoken
                    elif row["type"] == "multitoken":
                        # FIX: Remove after code
                        logger.debug(
                            colored(
                                f"   ERC - {row['type']} =============================== \n"
                                + f"FROM: {row['from']} -> TO: {row['to']} <-> CON: {row['contractAddress']}\n"
                                + f"VALUE: {row['valConv']} - SYMBOL: {row['symbol']} - FUNC: {row['functionName']}",
                                "cyan",
                            )
                        )

                        # if (hash == "0x46e7f1b95cddd4596e55ebe650af5d26b8932de84d9a3461cc4069922fbbd14b"):
                        #     __import__('pdb').set_trace()

                        if (xfrom_address == row["to"] == address_central) and (row["from"] == "0x0000000000000000000000000000000000000000"):
                            # 0x8afd1b8eb53024809e52396faeb488d3b5fb769242ca2c777b32b46807ff33d2
                            # logger.debug(colored(f"++ MULTITOKEN MINT NFT ===========================", "light_cyan"))  # NOTE: Checked
                            action = "mint nft"

                            add_link(
                                xto_address,
                                xfrom_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                "multitoken - nfts",
                                node_create=True,
                            )

                        elif (xfrom_address == row["from"] == address_central) and (row["to"] == "0x0000000000000000000000000000000000000000"):
                            # 0x8afd1b8eb53024809e52396faeb488d3b5fb769242ca2c777b32b46807ff33d2
                            # logger.debug(colored(f"++ MULTITOKEN BURN NFT ===========================", 'light_cyan'))  # NOTE: Checked
                            action = "burn nft"

                            add_link(
                                xfrom_address,
                                xto_address,
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["value"],
                                action,
                                "multitoken - nfts",
                                node_create=True,
                            )

                        elif (
                            (xfrom_address == row["to"] == address_central)
                            and (float(xvalue) > 0.0)
                            and (xto_address != row["from"] != row["contractAddress"])
                            and (row["decimal"] == 1)
                        ):  # 0x3c5f26298c02fdcd9fe79d2bd7697ed65dfece0ddcab6d7228e469d58ea78afb
                            # logger.debug(colored(f"++ MULTITOKEN BUY NFT WITH ETHER =================", "light_cyan"))
                            action = "buy nft with ether"

                            # Links
                            add_link(xfrom_address, xto_address, xsymbol, xname, "", xvalue, action, xtype, node_create=True)

                            # Node
                            # NOTE: The source of the NFT is a wallet
                            node_address = row["from"]
                            if (node_address not in nodes) and (node_address not in nodes_db):
                                tag = tags_dict.get(node_address, [])  # Get tag
                                label = labels_dict.get(node_address, [])  # Get label
                                add_nodes(node_address, tag, label, contract=False)

                            add_link(
                                row["from"],
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                "multitoken - nfts",
                                node_create=True,
                            )
                        elif (
                            (xfrom_address == row["from"] == address_central)
                            and (float(xvalue) == 0.0)
                            and (xto_address == row["contractAddress"])
                            and (row["decimal"] == 1)
                        ):  # 0xff29144cab03e9f3fa2ce19e0c08041d67ea6ecea4a7aed69c657e58f6a0c5a4
                            # logger.debug(colored(f"++ MULTITOKEN TRANSFER NFT =======================", "light_cyan"))
                            action = "transfer nft"

                            # Node
                            # NOTE: The target of the NFT is a wallet
                            node_address = row["to"]
                            if (node_address not in nodes) and (node_address not in nodes_db):
                                tag = tags_dict.get(node_address, [])  # Get tag
                                label = labels_dict.get(node_address, [])  # Get label
                                add_nodes(node_address, tag, label, contract=False)

                            add_link(
                                row["from"],
                                row["to"],
                                row["symbol"],
                                row["name"],
                                row["contractAddress"],
                                row["valConv"],
                                action,
                                "multitoken - nfts",
                                node_create=True,
                            )
                        else:
                            logger.error(f"++ NOT DETECTED = {row['type']} ==================")
                            logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                            break

                    else:
                        logger.error(f"++ NOT DETECTED GENERAL = {row['type']} ==================")
                        logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
                        break

        elif not has_transaction:
            logger.debug(colored(f"== INCOMPLETE ({inc}) == HASH: {group.iloc[0]['hash']} ==", 'magenta'))
            pass
            logger.debug(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
            inc += 1
            # for _, row in group.iterrows():
            #     # INFO: Internals
            #     if (row['type'] == 'internals'):
            #         if ((group['type'] == 'nfts').any()) and (group.loc[group['type'] == 'internals', 'to'].values[0] == group.loc[group['type'] == 'nfts', 'from'].values[0]):
            #             # 0xfb11efd2453075e0998c3b6941886858b08b0431b373a96c0347621c1a114820
            #             # print(f"group")
            #             # print(f"{group[['type', 'from', 'to', 'value', 'contractaddress']]}")
            #             # WARN: group processed
            #             # WARN: there is a record “transfers” that is not shown but is in the group, due to the “break”.
            #             logger.info(colored(f"++ SWAP NFT BY ETHER =============================", 'magenta'))  # TODO: checked
            #             break
            #         elif (row['from'] == address_central):
            #             logger.info(colored(f"++ TRANSFER ETHER FROM WA ========================", 'magenta'))
            #         elif (row['to'] == address_central):
            #             logger.info(colored(f"++ TRANSFER ETHER TO WA ==========================", 'magenta'))
            #         else:
            #             logger.error(f"++ NOT DETECTED = INCOMPLETE = {row['type']} ==================")
            #             logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
            #             break
            #     # INFO: Transfers
            #     elif (row['type'] == 'transfers'):
            #         if (row['from'] == address_central):
            #             logger.info(colored(f"++ TRANSFER TOKEN FROM WA ========================", 'magenta'))
            #         elif (row['to'] == address_central):
            #             logger.info(colored(f"++ TRANSFER TOKEN TO WA ==========================", 'magenta'))
            #         else:
            #             logger.error(f"++ NOT DETECTED = INCOMPLETE = {row['type']} ==================")
            #             logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
            #             break
            #     elif (row['type'] == 'nfts'):
            #         if (row['to'] == address_central) and (row['from'] == '0x0000000000000000000000000000000000000000'):
            #             logger.info(colored(f"++ MINT NFT ======================================", 'magenta'))
            #         elif (row['from'] == address_central) and (row['to'] == '0x0000000000000000000000000000000000000000'):
            #             logger.info(colored(f"++ BURN NFT ======================================", 'magenta'))
            #         elif (row['from'] == address_central):
            #             logger.info(colored(f"++ TRANSFER NFT FROM WA ==========================", 'magenta'))
            #         elif (row['to'] == address_central):
            #             logger.info(colored(f"++ TRANSFER NFT TO WA ============================", 'magenta'))
            #         else:
            #             logger.error(f"++ NOT DETECTED = INCOMPLETE = {row['type']} ==================")
            #             logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
            #             break
            #     # INFO: Multitoken
            #     elif (row['type'] == 'multitoken'):
            #         if (row['from'] == address_central) and (row['decimal'] <= 2) and (row['to'] == '0x0000000000000000000000000000000000000000'):
            #             logger.info(colored(f"++ MULTITOKEN BURN NFT ===========================", 'magenta'))
            #         elif (row['to'] == address_central) and (row['decimal'] <= 2) and (row['from'] == '0x0000000000000000000000000000000000000000'):
            #             logger.info(colored(f"++ MULTITOKEN MINT NFT ===========================", 'magenta'))
            #         elif (row['from'] == address_central) and (row['decimal'] == 1):
            #             logger.info(colored(f"++ MULTITOKEN TRANSFER NFT FROM WA ===============", 'magenta'))
            #         elif (row['to'] == address_central) and (row['decimal'] == 1):
            #             logger.info(colored(f"++ MULTITOKEN TRANSFER NFT TO WA =================", 'magenta'))
            #         else:
            #             logger.error(f"++ NOT DETECTED = INCOMPLETE = {row['type']} ==================")
            #             logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
            #             break
            #     else:
            #         logger.error(f"++ NOT DETECTED = INCOMPLETE = {row['type']} ==================")
            #         logger.error(f"GROUP\n{group[['type', 'from', 'to', 'value', 'contractAddress']]}")
            #         break
            # logger.info(colored(f"==================================================\n", 'black'))
        # else:
        #     logger.error(f"== NOT CLASSIFIED ================================")
        #     logger.error(f"== NOT CLASSIFIED ================================")
        #     logger.error(f"GROUP: \n{group}")
        #     logger.error(f"== NOT CLASSIFIED ================================")
        #     logger.error(f"== NOT CLASSIFIED ================================")
        #     raise Exception("== NOT CLASSIFIED ================================")

    toc = time.perf_counter()
    logger.info(f"Time to classification {toc - tic:0.4f} seconds")

    # with open(f"./classification.json", 'w', encoding='utf-8') as file:
    #     json.dump({"nodes": nodes, "links": links}, file, ensure_ascii=False, indent=4)

    # INFO: Write nodes and links in db
    # nodes_list = list(nodes.values())
    # links_list = list(links.values())
    nodes_list = list(nodes.values())
    links_list = list(links.values())
    # df_nodes = pd.DataFrame(nodes_list)
    # df_nodes['tag'] = df_nodes['tag'].apply(lambda x: json.dumps(x))
    # df_nodes['label'] = df_nodes['label'].apply(lambda x: json.dumps(x))
    # df_links = pd.DataFrame(links_list)
    # df_links['detail'] = df_links['detail'].apply(lambda x: json.dumps(x))
    df_nodes = pd.DataFrame(nodes_list)
    df_nodes["tag"] = df_nodes["tag"].apply(lambda x: json.dumps(x))
    df_nodes["label"] = df_nodes["label"].apply(lambda x: json.dumps(x))
    df_links = pd.DataFrame(links_list)
    # print(df_links.info())
    # print(df_links.head())
    df_links["action"] = df_links["action"].apply(lambda x: json.dumps(x))

    # df_nodes.to_sql('t_nodes_classification', conn, if_exists='append', index=False, method=insert_with_ignore)
    # df_links.to_sql('t_links_classification', conn, if_exists='append', index=False, method=insert_with_ignore)
    df_links.to_sql("t_links_classification", conn, if_exists="append", index=False, method=insert_with_ignore)
    df_nodes.to_sql("t_nodes_classification", conn, if_exists="append", index=False, method=insert_with_ignore)

    # INFO: Generate stat table
    query = """
        SELECT *
        FROM t_stats
    """
    df_stats = pd.read_sql_query(query, conn)
    if df_stats.empty:
        initialize = pd.Series([0] * 10, index=df_stats.columns)
        df_stats.loc[0] = initialize

    type_counts = df_all["type"].value_counts()
    stat_trx = type_counts.get("transaction", 0)
    stat_int = type_counts.get("internals", 0)
    stat_tra = type_counts.get("transfers", 0)
    stat_nft = type_counts.get("nfts", 0)
    stat_mul = type_counts.get("multitokens", 0)
    stat_tot = stat_trx + stat_int + stat_tra + stat_nft + stat_mul + stat_err

    df_stats.loc[0, "stat_tot"] = df_stats.loc[0, "stat_tot"] + stat_tot
    df_stats.loc[0, "stat_trx"] = df_stats.loc[0, "stat_trx"] + stat_trx
    df_stats.loc[0, "stat_tra"] = df_stats.loc[0, "stat_tra"] + stat_tra
    df_stats.loc[0, "stat_err"] = df_stats.loc[0, "stat_err"] + stat_err
    df_stats.loc[0, "stat_int"] = df_stats.loc[0, "stat_int"] + stat_int
    df_stats.loc[0, "stat_wal"] = df_stats.loc[0, "stat_wal"] + stat_wal
    df_stats.loc[0, "stat_con"] = df_stats.loc[0, "stat_con"] + stat_con
    # df_stats.loc[0, 'stat_coo'] = df_stats.loc[0, 'stat_coo'] + stat_coo
    df_stats.loc[0, "stat_nft"] = df_stats.loc[0, "stat_nft"] + stat_nft
    df_stats.loc[0, "stat_mul"] = df_stats.loc[0, "stat_mul"] + stat_mul

    df_stats.to_sql("t_stats", conn, if_exists="replace", index=False, method=insert_with_ignore)

    return {"process": "ok"}


def get_nodes_links_bd(conn, address_central, params=[]):
    tic = time.perf_counter()

    # INFO: Config Log Level
    if params:
        log_format = "%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s"
        coloredlogs.install(level=params["config"]["level"], fmt=log_format, logger=logger)
        logger.propagate = False  # INFO: To prevent duplicates with flask

    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")
    logger.debug(f"+ Address: {address_central}")
    logger.debug("++++++++++++++++++++++++++++++++++++++++++++++++++++")

    address_central = address_central[1]

    # TODO: Query nodes

    # query = """
    #     SELECT *
    #     FROM t_nodes_classification
    # """
    # df_nodes = pd.read_sql_query(query, conn)
    # # df_nodes['tag'] = df_nodes['tag'].sapply(lambda x: json.dumps(x))
    # # df_nodes['label'] = df_nodes['label'].apply(lambda x: json.dumps(x))
    # df_nodes['tag'] = df_nodes['tag'].apply(json.loads)
    # df_nodes['label'] = df_nodes['label'].apply(json.loads)
    # # print("=================================================================")
    # # print(df_nodes.info())
    # # print(df_nodes.head())
    # # print("=================================================================")
    # nodes_list = json.loads(df_nodes.to_json(orient = "records"))
    # # print(nodes_list)

    query = """
        SELECT *
        FROM t_nodes_classification
    """
    df_nodes = pd.read_sql_query(query, conn)
    df_nodes["tag"] = df_nodes["tag"].apply(json.loads)
    df_nodes["label"] = df_nodes["label"].apply(json.loads)
    # print("=================================================================")
    # print(df_nodes.info())
    # print(df_nodes.head())
    # print("=================================================================")
    nodes_list = json.loads(df_nodes.to_json(orient="records"))
    # print(nodes_list)

    # with open(f"./nodes.json", 'w', encoding='utf-8') as file:
    #     json.dump({"nodes": nodes_list}, file, ensure_ascii=False, indent=4)

    # TODO: Query links
    query = """
        SELECT *
        FROM t_links_classification
    """
    df_links = pd.read_sql_query(query, conn)
    df_links["action"] = df_links["action"].apply(json.loads)
    # print("=================================================================")
    # print(df_links.info())
    # print(df_links.head())
    # print("=================================================================")
    links_list = json.loads(df_links.to_json(orient="records"))
    # print(links_list)
    grouped_data = defaultdict(lambda: {"source": "", "target": "", "detail": defaultdict(dict), "qty": 0})

    for link in links_list:
        key = (link["source"], link["target"])
        symbol = link["symbol"]
        detail = {
            "contract": link["contract"],
            "name": link["name"],
            "count": link["count"],
            "sum": link["sum"],
            "action": link["action"],
            "type": link["type"],
        }

        if grouped_data[key]["source"] == "":
            grouped_data[key]["source"] = link["source"]
            grouped_data[key]["target"] = link["target"]

        grouped_data[key]["detail"][symbol] = detail  # pyright: ignore
        grouped_data[key]["qty"] += link["count"]

    links_list = list(grouped_data.values())
    # import pprint
    # pprint.pprint(links_list)

    # TODO: Query links

    # query = """
    #     SELECT *
    #     FROM t_links_classification
    # """
    # df_links = pd.read_sql_query(query, conn)
    # df_links['detail'] = df_links['detail'].apply(json.loads)
    # # print("=================================================================")
    # # print(df_links.info())
    # # print(df_links.head())
    # # print("=================================================================")
    # links_list = json.loads(df_links.to_json(orient = "records"))
    # # print(links_list)

    # # with open(f"./links.json", 'w', encoding='utf-8') as file:
    # #     json.dump({"links": links_list}, file, ensure_ascii=False, indent=4)

    # # transactions = {"nodes": nodes_list, "links": links_list, "link_detail": links}
    transactions = {"nodes": nodes_list, "links": links_list}

    # TODO: Query stats

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
    df_all["timeStamp"] = pd.to_datetime(df_all["timeStamp"], unit="s")
    df_all = df_all[df_all["isError"] == 0]

    list_trans = df_all.loc[(df_all["from"] == address_central) | (df_all["to"] == address_central)].to_json(orient="records")

    query = """
        SELECT *
        FROM t_stats
    """
    df_stats = pd.read_sql_query(query, conn)
    stat_json = df_stats.to_json(orient="records")

    toc = time.perf_counter()
    logger.info(f"Time to get nodes and links {toc - tic:0.4f} seconds")

    return {"transactions": transactions, "list": list_trans, "stat": json.loads(stat_json)[0]}
