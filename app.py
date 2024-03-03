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

from flask import Flask, render_template, request, Response
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
logger.propagate = False  # INFO: To prevent duplicates with flask

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
    logger.info("HTTPD ready... Data Visualization Web on http://127.0.0.1:4200")
    with socketserver.TCPServer(("", PORT), Handler) as httpd_server:
        httpd_server.serve_forever()


@app.route('/internal-test-1', methods=['GET'])
def internal_test_1():
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 
    data = eth.test_function_1(params)
    return data


@app.route('/internal-test-2', methods=['GET'])
def internal_test_2():
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 
    data = eth.test_function_2(params)
    return data


@app.route('/stream-data-checking', methods=['GET'])
def get_data_checking():
    config = current_app.config['config']
    return Response(misc.event_stream_checking(config), mimetype='text/event-stream')


@app.route('/stream-data-ether', methods=['GET'])
def get_data_ether():
   
    params = request.args.to_dict()
    config = current_app.config['config']
    params['config'] = config 

    return Response(eth.event_stream_ether(params), mimetype='text/event-stream')


if __name__ == '__main__':

    # INFO: Read config and keys
    with open(r'./config.yaml') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)

    # INFO: Define loglevel
    coloredlogs.install(level='INFO')
    try:
        coloredlogs.install(level=config['level'])
    except Exception:
        logger.error("The level parameter is wrong, set to INFO by default")
        coloredlogs.install(level='INFO')

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

