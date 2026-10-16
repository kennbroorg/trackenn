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

import inspect

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import argparse
import multiprocessing
import http.server
import socketserver

from termcolor import colored
import coloredlogs, logging

from core import misc
from core import eth
from core import bsc

app = FastAPI(title="trackenn", version=__version__)
app.state.config = None
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:4200"],  # TODO: Move in API_Server
    allow_methods=["*"],
    allow_headers=["*"],
)

log_format = '%(asctime)s %(name)-10s %(lineno)-6d %(levelname)-7s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
coloredlogs.install(level='DEBUG', fmt=log_format, logger=logger)
# logger.propagate = False  # INFO: To prevent duplicates with uvicorn

def API_Server(ip='127.0.0.1', port=5000, config=None, env="prod"):
    app.state.config = config
    logger.info(f"API ready...{env}")
    if (env == "prod"):
        uvicorn.run(app, host=ip, port=port, log_level=str(config['level']).lower())
    else:
        uvicorn.run(app, host=ip, port=port, log_level="debug")


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


def query_params(request):
    # INFO: Keep first value of repeated keys (same as Flask request.args.to_dict())
    params = {}
    for key, value in request.query_params.multi_items():
        params.setdefault(key, value)
    return params


def get_config(request):
    # INFO: Copy per request, the shared config must not be mutated by handlers
    return dict(request.app.state.config)


def get_filters(params):
    filters_json = params.get('filters')
    if filters_json:
        return json.loads(filters_json)
    return {"filter": False}


def event_stream(generator):
    return StreamingResponse(generator, media_type='text/event-stream')


def test_response(data):
    if inspect.isgenerator(data):
        return event_stream(data)
    return JSONResponse(data)


@app.get('/recreatedb')  # NOTE: Used to test cases
def recreatedb(request: Request):
    params = query_params(request)
    params['config'] = get_config(request)
    data = eth.recreate_db(params)
    return test_response(data)


@app.get('/internal-test-1')  # NOTE: Used to test cases
def internal_test_1(request: Request):
    params = query_params(request)
    params['config'] = get_config(request)
    data = eth.test_function_1(params)
    return test_response(data)


@app.get('/internal-test-2')  # NOTE: Used to test cases
def internal_test_2(request: Request):
    params = query_params(request)
    params['config'] = get_config(request)
    data = eth.test_function_2(params)
    return test_response(data)


@app.get('/stream-data-checking')
def get_data_checking(request: Request):
    params = query_params(request)
    config = get_config(request)
    config['action'] = params['action']
    config['graph'] = params.get('graph', "Star")
    config['filters'] = get_filters(params)
    logger.debug(f"PARAM: {config}")

    return event_stream(misc.event_stream_checking(config))


@app.get('/stream-data-info')
def get_data_info(request: Request):
    params = query_params(request)
    params['config'] = get_config(request)
    params['filters'] = get_filters(params)
    logger.debug(f"PARAM: {params}")

    if (params['network'] == 'bsc'):
        return event_stream(bsc.event_stream_bsc(params))
    else:
        return event_stream(eth.event_stream_ether(params))


@app.get('/download_db')
def download_investigation():
    # TODO: Get from config file
    # HACK: Multiuser?
    file_path = './default.db'  # Change for the path
    return FileResponse(file_path, filename='default.db')


@app.get('/reset_db')
def reset_investigation():
    # TODO: Get from config file
    # HACK: Multiuser?
    file_path = './default.db'  # Change for the path
    return event_stream(misc.event_reset_investigation(file_path))


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
        kwargs_api = {"ip": ip, "port": 5000, "config": config}
        API_proc = multiprocessing.Process(name='API',
                                             target=API_Server,
                                             kwargs=kwargs_api)
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

