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


import os
import time
import json
import logging
import sqlite3
import traceback
import pandas as pd

from termcolor import colored
import coloredlogs, logging

# from core.eth import get_trx_from_addresses_opt, get_funders_creators, get_balance_and_gas, get_tags_labels
from core import eth
from core import bsc

logger = logging.getLogger(__name__)
# logger.setLevel(logging.DEBUG)
# log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
# coloredlogs.install(level='DEBUG', fmt=log_format, logger=logger)
logger.propagate = False  # INFO: To prevent duplicates with flask


def event_stream_checking(config):

    # INFO: Config Log Level
    log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
    coloredlogs.install(level=config['level'], fmt=log_format, logger=logger)

    if (config['action'] == "reset"):
        os.remove(config['dbname'])

    # if (config['filters']):
    #     filters = config['filters']
    # else:
    #     filters = {"filter": False}

    try:
        logger.info(f"Initializing project")
        data = json.dumps({"msg": f"Initializing project", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        logger.info(f"Get config information")
        data = json.dumps({"msg": f"Get config information", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        message = f"<strong>Using dbname: {config['dbname']}</strong>"
        logger.error(message.replace('<strong>', '').replace('</strong>', ''))
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        connection = sqlite3.connect(config['dbname'])
        cursor = connection.cursor()
        key = config['ethscan']

        message = f"Getting keys"
        logger.info(f"{message}")
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        # WARN: This isn't the best check
        if (config['ethscan'] == '') or (config['ethscan'] == 'XXX'):
            message = f"<strong>Etherscan.io api key possibly unconfigured</strong>"
            logger.error(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

        message = f"Checking db tables"
        logger.info(f"{message}")
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        try:
            query = f"SELECT COUNT(*) FROM t_test"
            cursor.execute(query)
        except Exception:
            start_time = time.time()

            message = f"<strong>Database empty</strong>"
            logger.error(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            # Create tables
            message = f"Creating table t_test"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_test_table = """CREATE TABLE IF NOT EXISTS t_test (
                                       id integer NOT NULL
                                       );"""
            connection.execute(sql_create_test_table)

            message = f"Creating table t_transactions"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_transactions_table = """CREATE TABLE IF NOT EXISTS t_transactions (
                                               blockChain text NOT NULL,
                                               blockNumber integer NOT NULL,
                                               timeStamp datetime NOT NULL,
                                               hash text NOT NULL,
                                               nonce integer NOT NULL,
                                               blockHash text NOT NULL,
                                               transactionIndex integer NOT NULL,
                                               `from` text NOT NULL,
                                               `to` text NOT NULL,
                                               value integer NOT NULL,
                                               gas integer NOT NULL,
                                               gasPrice integer NOT NULL,
                                               isError integer NOT NULL,
                                               txreceipt_status integer NOT NULL,
                                               input text NOT NULL,
                                               contractAddress text NOT NULL,
                                               cumulativeGasUsed integer NOT NULL,
                                               gasUsed integer NOT NULL,
                                               confirmations integer NOT NULL,
                                               methodId text NOT NULL,
                                               functionName text NOT NULL,
                                               UNIQUE(blockChain, blockNumber, timeStamp, hash, `from`, `to`, value)
                                               );"""
            connection.execute(sql_create_transactions_table)

            message = f"Creating table t_transfers"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_transfers_table = """CREATE TABLE IF NOT EXISTS t_transfers (
                                            blockChain text NOT NULL,
                                            blockNumber integer NOT NULL,
                                            timeStamp datetime NOT NULL,
                                            hash text NOT NULL,
                                            nonce integer NOT NULL,
                                            blockHash text NOT NULL,
                                            `from` text NOT NULL,
                                            contractAddress text NOT NULL,
                                            `to` text NOT NULL,
                                            value integer NOT NULL,
                                            tokenName text NOT NULL,
                                            tokenSymbol text NOT NULL,
                                            tokenDecimal integer NOT NULL,
                                            transactionIndex integer NOT NULL,
                                            gas integer NOT NULL,
                                            gasPrice integer NOT NULL,
                                            gasUsed integer NOT NULL,
                                            cumulativeGasUsed integer NOT NULL,
                                            input text NOT NULL,
                                            confirmations integer NOT NULL,
                                            methodId text NOT NULL,
                                            functionName text NOT NULL,
                                            UNIQUE(blockChain, blockNumber, timeStamp, hash, `from`, `to`, value)
                                            );"""
            connection.execute(sql_create_transfers_table)

            message = f"Creating table t_internals"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_internals_table = """CREATE TABLE IF NOT EXISTS t_internals (
                                            blockChain text NOT NULL,
                                            blockNumber integer NOT NULL,
                                            timeStamp datetime NOT NULL,
                                            hash text NOT NULL,
                                            `from` text NOT NULL,
                                            `to` text NOT NULL,
                                            value integer NOT NULL,
                                            contractAddress text NOT NULL,
                                            input text NOT NULL,
                                            type text NOT NULL,
                                            gas integer NOT NULL,
                                            gasUsed integer NOT NULL,
                                            traceId integer NOT NULL,
                                            isError integer NOT NULL,
                                            errCode text NOT NULL,
                                            methodId text NOT NULL,
                                            functionName text NOT NULL,
                                            UNIQUE(blockChain, blockNumber, timeStamp, hash, `from`, `to`, value)
                                            );"""
            connection.execute(sql_create_internals_table)

            message = f"Creating table t_nfts"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_nfts_table = """CREATE TABLE IF NOT EXISTS t_nfts (
                                       blockChain text NOT NULL,
                                       blockNumber integer NOT NULL,
                                       timeStamp datetime NOT NULL,
                                       hash text NOT NULL,
                                       nonce integer NOT NULL,
                                       blockHash text NOT NULL,
                                       `from` text NOT NULL,
                                       contractAddress text NOT NULL,
                                       `to` text NOT NULL,
                                       tokenID text NOT NULL,
                                       tokenName text NOT NULL,
                                       tokenSymbol text NOT NULL,
                                       tokenDecimal integer NOT NULL,
                                       transactionIndex integer NOT NULL,
                                       gas integer NOT NULL,
                                       gasPrice integer NOT NULL,
                                       gasUsed integer NOT NULL,
                                       cumulativeGasUsed integer NOT NULL,
                                       input text NOT NULL,
                                       confirmations integer NOT NULL,
                                       methodId text NOT NULL,
                                       functionName text NOT NULL,
                                       UNIQUE(blockChain, blockNumber, timeStamp, hash, `from`, `to`, tokenID)
                                       );"""
            connection.execute(sql_create_nfts_table)
            
            message = f"Creating table t_multitoken"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_mt_table = """CREATE TABLE IF NOT EXISTS t_multitoken (
                                     blockChain text NOT NULL,
                                     blockNumber integer NOT NULL,
                                     timeStamp datetime NOT NULL,
                                     hash text NOT NULL,
                                     nonce integer NOT NULL,
                                     blockHash text NOT NULL,
                                     transactionIndex integer NOT NULL,
                                     gas integer NOT NULL,
                                     gasPrice integer NOT NULL,
                                     gasUsed integer NOT NULL,
                                     cumulativeGasUsed integer NOT NULL,
                                     input text NOT NULL,
                                     contractAddress text NOT NULL,
                                     `from` text NOT NULL,
                                     `to` text NOT NULL,
                                     tokenID text NOT NULL,
                                     tokenValue integer NOT NULL,
                                     tokenName text NOT NULL,
                                     tokenSymbol text NOT NULL,
                                     confirmations integer NOT NULL,
                                     methodId text NOT NULL,
                                     functionName text NOT NULL,
                                     UNIQUE(blockChain, blockNumber, timeStamp, hash, `from`, `to`, tokenID)
                                     );"""
            connection.execute(sql_create_mt_table)
            
            # PERF: This table will be eliminated
            # message = "Creating Table t_contract"
            # logger.info(f"{message}")
            # data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            # yield f"data:{data}\n\n"
            # sql_create_contract_table = """CREATE TABLE IF NOT EXISTS t_contract (
            #                                blockChain text NOT NULL,
            #                                contract text NOT NULL,
            #                                block_from text NOT NULL,
            #                                block_to text NOT NULL,
            #                                first_block text NOT NULL,
            #                                transaction_creation text NOT NULL,
            #                                date_creation datetime NOT NULL,
            #                                creator text NOT NULL
            #                              );"""
            # cursor.execute(sql_create_contract_table)

            message = "Creating Table t_contract"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_contract_table = """CREATE TABLE IF NOT EXISTS t_contract (
                                             blockChain text NOT NULL,
                                             contract text NOT NULL,
                                             SourceCode text NOT NULL,
                                             ABI text NOT NULL,
                                             ContractName text NOT NULL,
                                             CompilerVersion text NOT NULL,
                                             OptimizationUsed text NOT NULL,
                                             Runs text NOT NULL,
                                             ConstructorArguments text NOT NULL,
                                             EVMVersion text NOT NULL,
                                             Library text NOT NULL,
                                             LicenseType text NOT NULL,
                                             Proxy text NOT NULL,
                                             Implementation text NOT NULL,
                                             SwarmSource text NOT NULL
                                           );"""
            cursor.execute(sql_create_contract_table)

            message = "Creating Table t_address_detail"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_wallet_detail = """CREATE TABLE IF NOT EXISTS t_address_detail (
                                          blockChain text NOT NULL,
                                          address text NOT NULL,
                                          block_from integer NOT NULL,
                                          block_to text NOT NULL,
                                          first_block integer NOT NULL,
                                          first_date datetime NOT NULL,
                                          first_hash text NOT NULL,
                                          first_from text NOT NULL,
                                          first_to text NOT NULL,
                                          first_value integer NOT NULL,
                                          first_input text NOT NULL,
                                          first_func text NOT NULL,
                                          type text NOT NULL,
                                          last_block text NOT NULL,
                                          last_date datetime NOT NULL,
                                          last_hash text NOT NULL,
                                          last_to text NOT NULL,
                                          last_from text NOT NULL,
                                          last_value integer NOT NULL,
                                          last_input text NOT NULL,
                                          last_func text NOT NULL,
                                          contract_name text NOT NULL,
                                          contract text NOT NULL
                                        );"""
            cursor.execute(sql_create_wallet_detail)

            message = "Creating Table t_balance"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            sql_create_balance_table = """CREATE TABLE IF NOT EXISTS t_balance (
                                           blockChain text NOT NULL,
                                           address text NOT NULL,
                                           token text NOT NULL,
                                           tokenName text NOT NULL,
                                           balance float NOT NULL
                                         );"""
            cursor.execute(sql_create_balance_table)

            message = "Creating Table t_blocks"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_blocks_table = """CREATE TABLE IF NOT EXISTS t_blocks (
                                         blockChain text NOT NULL,
                                         address text NOT NULL,
                                         block_from number NOT NULL,
                                         block_to number NOT NULL,
                                         date_from number NOT NULL,
                                         date_to number NOT NULL,
                                         all_data boolean NOT NULL
                                       );"""
            cursor.execute(sql_create_blocks_table)

            message = "Creating Table t_funders_creators"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_funders_table = """CREATE TABLE IF NOT EXISTS t_funders_creators (
                                           blockChain text NOT NULL,
                                           blockNumber integer NOT NULL,
                                           type text NOT NULL,
                                           timeStamp datetime NOT NULL,
                                           hash text NOT NULL,
                                           `from` text NOT NULL,
                                           `to` text NOT NULL,
                                           value integer NOT NULL,
                                           input text NOT NULL,
                                           contractAddress text NOT NULL,
                                           tokenDecimal integer NOT NULL,
                                           tokenSymbol text NOT NULL,
                                           tokenName text NOT NULL,
                                           UNIQUE(blockChain, blockNumber, timeStamp, hash, `from`, `to`, value)
                                         );"""
            cursor.execute(sql_create_funders_table)

            message = "Creating Table t_tagging"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_tagging_table = """CREATE TABLE IF NOT EXISTS t_tags (
                                           blockChain text NOT NULL,
                                           address text NOT NULL,
                                           tag text NOT NULL, 
                                           UNIQUE(blockChain, address, tag)
                                         );"""
            cursor.execute(sql_create_tagging_table)

            message = "Creating Table t_nodes_classification"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_nodes_c_table = """CREATE TABLE IF NOT EXISTS t_nodes_classification (
                                           id text NOT NULL, 
                                           address text NOT NULL,
                                           tag text NOT NULL, 
                                           label text NOT NULL, 
                                           UNIQUE(id)
                                         );"""
            cursor.execute(sql_create_nodes_c_table)

            message = "Creating Table t_links_classification"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_links_c_table = """CREATE TABLE IF NOT EXISTS t_links_classification (
                                           link_key TEXT NOT NULL,
                                           source TEXT NOT NULL,
                                           target TEXT NOT NULL,
                                           symbol TEXT NOT NULL,
                                           name TEXT NOT NULL,
                                           contract TEXT NOT NULL,
                                           count INTEGER NOT NULL,
                                           sum REAL NOT NULL,
                                           action TEXT NOT NULL,
                                           type TEXT NOT NULL
                                        );"""
            cursor.execute(sql_create_links_c_table)

            message = "Creating Table t_stats"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_stats_table = """CREATE TABLE IF NOT EXISTS t_stats (
                                         stat_tot integer DEFAULT 0,
                                         stat_trx integer DEFAULT 0,
                                         stat_tra integer DEFAULT 0,
                                         stat_err integer DEFAULT 0,
                                         stat_int integer DEFAULT 0,
                                         stat_wal integer DEFAULT 0,
                                         stat_con integer DEFAULT 0,
                                         stat_coo integer DEFAULT 0,
                                         stat_nft integer DEFAULT 0,
                                         stat_mul integer DEFAULT 0
                                       );"""
            cursor.execute(sql_create_stats_table)

            message = "Creating Table t_labels"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            sql_create_labels_table = """CREATE TABLE IF NOT EXISTS t_labels (
                                          blockChain text NOT NULL,
                                          source text NOT NULL,
                                          address text NOT NULL,
                                          name text NOT NULL,
                                          labels text NOT NULL
                                       );"""
            cursor.execute(sql_create_labels_table)

            # Load JSON etherscan
            message = "Loading Table t_labels with etherscan labels"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            with open('data/etherscanCombinedAllLabels.json', 'r') as file:
                etherscan = json.load(file)
            
            rows = []
            for address, info in etherscan.items():
                row = {'blockChain': 'ethereum',
                       'source': 'etherscan_label', 
                       'address': address, 
                       'name': info['name'], 
                       'labels': info['labels']}
                rows.append(row)

            df_labels = pd.DataFrame(rows)
            df_labels['labels'] = df_labels['labels'].apply(json.dumps)

            df_labels.to_sql('t_labels', connection, if_exists='replace', index=False)

            # Load JSON bscscan
            message = "Loading Table t_labels with bscscan labels"
            logger.info(f"{message}")
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"
            with open('data/bscscanCombinedAllLabels.json', 'r') as file:
                bscscan = json.load(file)
            
            rows = []
            for address, info in bscscan.items():
                row = {'blockChain': 'binance',
                       'source': 'bscscan_label', 
                       'address': address, 
                       'name': info['name'], 
                       'labels': info['labels']}
                rows.append(row)

            df_labels = pd.DataFrame(rows)
            df_labels['labels'] = df_labels['labels'].apply(json.dumps)

            df_labels.to_sql('t_labels', connection, if_exists='append', index=False)

            end_time = time.time()
            elapsed_time = end_time - start_time

            message = f"End of tables creation in <strong>{elapsed_time} s</strong>"
            logger.error(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

        # INFO: Getting data from DB
        message = "Getting cached info"
        logger.info(f"{message}")
        data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
        yield f"data:{data}\n\n"

        query = f"SELECT * FROM t_tags WHERE tag = 'central'"
        cursor.execute(query)
        address = cursor.fetchone()

        if (address):
            address_central = address[1]
            blockchain = address[0]
            logger.debug(f"ADDRESS: {address}")
            query = f"SELECT address FROM t_tags WHERE tag = 'path' AND blockChain = '{blockchain}';"
            cursor.execute(query)
            rows = cursor.fetchall()
            logger.debug(f"ROWS: {rows}")

            # Get addresses in path
            addresses = [row[0] for row in rows]
            logger.debug(f"ADDRESSES: {addresses}")

            # INFO: Send wallet detail information
            query = f"SELECT * FROM t_address_detail WHERE address = '{address_central}'"
            cursor.execute(query)
            wallet_detail = cursor.fetchall()
            type = wallet_detail[0][12]

            # INFO: Getting address and addresses and blockchain
            message = f"<strong>DATA</strong> - Received central and path addresses cached data..."
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            # TODO: Pass source, trx and type
            # data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"address": address[1], 
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"address": address, 
                                                                                              "addresses": addresses, 
                                                                                              "source": "central", 
                                                                                              "type": type, 
                                                                                              "network": blockchain}})
            yield f"data:{data}\n\n"

            message = f"<strong>TRANSACTIONS</strong> - Receiving wallet detail information..."
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"wallet_detail": wallet_detail}})
            yield f"data:{data}\n\n"

            message = f"<strong>DATA</strong> - Getting collected trxs, internals, transfers..."
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {}})
            yield f"data:{data}\n\n"

            # INFO: Getting trxs, internals and transfers
            tic = time.perf_counter()

            if (blockchain == "bsc"):
                if (config['graph'] == "Complex"):
                    trxs = bsc.get_trx_from_addresses_opt(connection, address) # TODO: Add experimental 
                else:
                    trxs = bsc.get_trx_from_addresses_opt(connection, address)
            elif (blockchain == "eth"):
                if (config['graph'] == "Complex"):
                    # trxs = eth.get_trx_from_addresses_experimental(connection, address)
                    trxs = eth.get_nodes_links_bd(connection, address, params=config)
                else:
                    trxs = eth.get_trx_from_addresses_opt(connection, address)
            else:
                # INFO: ERROR handle. Not in the followings
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

            toc = time.perf_counter()
            message = f"<strong>DATA</strong> - Proccesed...<strong>{toc - tic:0.4f}</strong> seconds"
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                               "content": {"graph": trxs['transactions'], "list": trxs['list'], "stat": trxs['stat']}})
            yield f"data:{data}\n\n"

            # INFO: Get Funders and creators
            tic = time.perf_counter()
            if (blockchain == "bsc"):
                funders = bsc.get_funders_creators(connection, address)
            else: # INFO: ETH
                funders = eth.get_funders_creators(connection, address)
            toc = time.perf_counter()
            message = f"<strong>DATA</strong> - Funders and creators...<strong>{toc - tic:0.4f}</strong> seconds"
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, "content": {"funders": funders}})
            yield f"data:{data}\n\n"

            # INFO: Get Balance and Gas
            tic = time.perf_counter()
            if (blockchain == "bsc"):
                balance = bsc.get_balance_and_gas(connection, address, type, key)
            else: # INFO: ETH
                balance = eth.get_balance_and_gas(connection, address, type, key)
            toc = time.perf_counter()
            message = f"<strong>DATA</strong> - Balance and Gas...<strong>{toc - tic:0.4f}</strong> seconds"
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                               "content": {"balance": balance['balance'], "tokens": balance['tokens'], "gas": balance['gas']}})
            yield f"data:{data}\n\n"

            # INFO: Get tags and labels
            tic = time.perf_counter()
            if (blockchain == "bsc"):
                tags = bsc.get_tags_labels(connection, address)
            else: # INFO: ETH
                tags = eth.get_tags_labels(connection, address)
            toc = time.perf_counter()
            message = f"<strong>DATA</strong> - Tags and Labels...<strong>{toc - tic:0.4f}</strong> seconds"
            logger.info(message.replace('<strong>', '').replace('</strong>', ''))
            data = json.dumps({"msg": f"{message}", "end": False, "error": False, 
                               "content": {"tags": tags['tags'], "labels": tags['labels']}})
            yield f"data:{data}\n\n"

        # INFO: Close connection
        connection.close()
        message = f"End checking...\n"
        logger.warning(f"{message}")
        data = json.dumps({"msg": f"{message}", "end": True, "error": False, "content": {}})
        yield f"data:{data}\n\n"

    except Exception:
        traceback.print_exc()
        traceback_text = traceback.format_exc()

        connection.close()
        message = f"<strong>Error...</strong>"
        logger.error(message.replace('<strong>', '').replace('</strong>', ''))
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
        
