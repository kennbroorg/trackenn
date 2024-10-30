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

import sys
import yaml
import json

from flask import Flask, render_template, request, Response, send_file
from flask_cors import CORS, cross_origin
from flask import current_app

import argparse
import multiprocessing
import http.server
import socketserver

from termcolor import colored
import coloredlogs, logging

from core import misc
from core import eth
from core import bsc

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "http://127.0.0.1:4200"}})  # TODO: Move in API_Server

log_format = '%(asctime)s %(name)-10s %(lineno)-6d %(levelname)-7s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)
# app.logger.handlers[0].setFormatter(logging.Formatter(log_format))
for handler in app.logger.handlers:
    handler.setFormatter(logging.Formatter(log_format))
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
coloredlogs.install(level='DEBUG', fmt=log_format, logger=logger)
# logger.propagate = False  # INFO: To prevent duplicates with flask

def API_Server(ip='127.0.0.1', port=5000, config=None, env="prod"):
    app.config['config'] = config
    logger.info(f"API ready...{env}")
    if (env == "prod"):
        app.run(port=port, debug=False, host=ip, use_reloader=False)
    else:
        app.run(host=ip, port=port, debug=True)


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory="./front/", **kwargs)


def httpServer():
    PORT = 4200
    logger.info("HTTPD ready...")
    logger.info("")
    logger.info("===================================================")
    logger.info("  Data Visualization Web on http://127.0.0.1:4200")
    logger.info("===================================================\n")
    with socketserver.TCPServer(("", PORT), Handler) as httpd_server:
        httpd_server.serve_forever()


@app.route('/recreatedb', methods=['GET'])  # NOTE: Used to test cases
def recreatedb():
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 
    data = eth.recreate_db(params)
    return data


@app.route('/internal-test-1', methods=['GET'])  # NOTE: Used to test cases
def internal_test_1():
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 
    data = eth.test_function_1(params)
    return data


@app.route('/internal-test-2', methods=['GET'])  # NOTE: Used to test cases
def internal_test_2():
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 
    data = eth.test_function_2(params)
    return data


@app.route('/stream-data-checking', methods=['GET'])
def get_data_checking():
    params = request.args.to_dict()
    config = current_app.config['config']
    config['action'] = params['action'] 
    if ("graph" in params.keys()):
        config['graph'] = params['graph'] 
    else:
        config['graph'] = "Star"
    filters_json = request.args.get('filters')
    if filters_json:
        filters = json.loads(filters_json)
    else:
        filters = {"filter": False}
    config['filters'] = filters
    logger.debug(f"PARAM: {config}")

    return Response(misc.event_stream_checking(config), mimetype='text/event-stream')


@app.route('/stream-data-info', methods=['GET'])
def get_data_info():
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 
    filters_json = request.args.get('filters')
    if filters_json:
        filters = json.loads(filters_json)
    else:
        filters = {"filter": False}
    params['filters'] = filters
    logger.debug(f"PARAM: {params}")

    if (params['network'] == 'bsc'):
        return Response(bsc.event_stream_bsc(params), mimetype='text/event-stream')
    else: 
        return Response(eth.event_stream_ether(params), mimetype='text/event-stream')


@app.route('/download_db')
def download_investigation():
    # TODO: Get from config file
    # HACK: Multiuser?
    file_path = './default.db'  # Change for the path
    return send_file(file_path, as_attachment=True)


@app.route('/reset_db')
def reset_investigation():
    # TODO: Get from config file
    # HACK: Multiuser?
    file_path = './default.db'  # Change for the path
    return Response(misc.event_reset_investigation(file_path), mimetype='text/event-stream')


if __name__ == '__main__':

    # INFO: For Windows platform
    if sys.platform == 'win32':
        multiprocessing.freeze_support()

    # INFO: Read config and keys
    with open(r'./config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    # INFO: Define loglevel
    log_format = '%(asctime)s %(name)s %(lineno)d %(levelname)s %(message)s'
    coloredlogs.install(level=config['level'], fmt=log_format, logger=logger)

    # coloredlogs.install(level='INFO')
    # try:
    #     coloredlogs.install(level=config['level'])
    # except Exception:
    #     logger.error("The level parameter is wrong, set to INFO by default")
    #     coloredlogs.install(level='INFO')

    # app.config['config'] = config
    # app.run(host='127.0.0.1', port=5000, debug=True)

    # INFO: Parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip', action='store', default='127.0.0.1',
                        help='IP address')  # TODO: For interface
    # TODO: Port
    parser.add_argument('-e', '--env', choices=["prod", "dev"], 
                        default="prod", help='Environment [dev, prod]')

    args = parser.parse_args()
    ip = str(args.ip)
    env = str(args.env)

    if (env == "prod"):
        logger.info('Starting PROD servers')
        logger.info("API starting...")
        kwargs_flask = {"ip": ip, "port": 5000, "config": config}
        API_proc = multiprocessing.Process(name='API',
                                             target=API_Server,
                                             kwargs=kwargs_flask)
        API_proc.daemon = True

        logger.info("HTTPD starting...")
        httpd_proc = multiprocessing.Process(name='httpd',
                                             target=httpServer)
        httpd_proc.daemon = True

        API_proc.start()
        httpd_proc.start()
        API_proc.join()
        httpd_proc.join()
    else:
        API_Server(ip, port=5000, env='desa', config=config)

