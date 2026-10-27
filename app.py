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
import sys
import yaml
import json

import inspect

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse

import argparse
import multiprocessing
import http.server
import socketserver

from termcolor import colored
import coloredlogs, logging

from core import misc
from core import api_client
from core import eth
from core import bsc

DEFAULT_FRONT_ORIGINS = ["http://127.0.0.1:4200"]


def front_origins():
    # INFO: Comma separated TRACKENN_FRONT_ORIGINS (e.g. the nginx origin in Docker), default 127.0.0.1:4200
    #       "*" is dropped: Starlette would open CORS to every site. Browsers send Origin without a trailing slash
    raw = os.environ.get("TRACKENN_FRONT_ORIGINS", "")
    origins = [origin.strip().rstrip("/") for origin in raw.split(",")]
    origins = [origin for origin in origins if origin and origin != "*"]
    return origins or list(DEFAULT_FRONT_ORIGINS)


FRONT_ORIGINS = front_origins()

app = FastAPI(title="trackenn", version=__version__)
app.state.config = None
app.state.config_path = './config.yaml'
app.add_middleware(
    CORSMiddleware,
    allow_origins=FRONT_ORIGINS,
    allow_methods=["*"],
    allow_headers=["*"],
)

log_format = '%(asctime)s %(name)-10s %(lineno)-6d %(levelname)-7s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
coloredlogs.install(level='DEBUG', fmt=log_format, logger=logger)
# logger.propagate = False  # INFO: To prevent duplicates with uvicorn

def process_context():
    # INFO: Python 3.14 defaults to forkserver on Linux, its children cannot load API_Server/httpServer
    #       from the frozen (cx_Freeze) __main__. fork keeps them; Windows keeps spawn + freeze_support
    if sys.platform.startswith('linux'):
        return multiprocessing.get_context('fork')
    return multiprocessing.get_context()


def API_Server(ip='127.0.0.1', port=5000, config=None, env="prod"):
    app.state.config = config
    logger.info(f"API ready...{env}")
    if (env == "prod"):
        uvicorn.run(app, host=ip, port=port, log_level=str(config['level']).lower())
    else:
        uvicorn.run(app, host=ip, port=port, log_level="debug")


class Handler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        # INFO: front_dir() is cwd-independent (code dir in source runs, executable dir when frozen)
        super().__init__(*args, directory=misc.front_dir(), **kwargs)


def httpServer(ip='127.0.0.1'):
    PORT = 4200
    front_path = misc.front_dir()
    if not os.path.isfile(os.path.join(front_path, 'index.html')):
        # INFO: A missing front must not look like a crash or a silent 404 wall; the API
        #       process is independent and keeps running, only this static server stops
        logger.error(f"Front not found at {front_path} (missing index.html).")
        logger.error("Run 'just front' to download the web UI, or build it from the front repo. "
                      "The API keeps running without it.")
        return

    logger.info("HTTPD ready...")
    logger.info("")
    logger.info("===================================================")
    logger.info(f"  Data Visualization Web on http://{ip}:{PORT}")
    logger.info("===================================================\n")
    with socketserver.TCPServer((ip, PORT), Handler) as httpd_server:
        httpd_server.serve_forever()


def load_config(path='./config.yaml'):
    # INFO: safe_load, config must not be able to build python objects
    try:
        with open(path) as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        sys.exit(f"Config file {path} not found, copy config.example.yaml to config.yaml and set your API keys")

    # INFO: TRACKENN_DB_DIR keeps the DB out of the code folder (the Docker volume /app/db)
    db_dir = os.environ.get("TRACKENN_DB_DIR", "").strip()
    if db_dir and not os.path.isabs(config['dbname']):
        config['dbname'] = os.path.join(db_dir, config['dbname'])
    return config


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
        try:
            return json.loads(filters_json)
        except json.JSONDecodeError:
            # INFO: client input, answer 400 instead of an unhandled 500 (T4.11)
            raise HTTPException(status_code=400, detail="Invalid 'filters' JSON")
    return {"filter": False}


def require_params(params, names):
    # INFO: missing required query params answered 500 (KeyError) before (T4.15)
    missing = [name for name in names if not params.get(name)]
    if missing:
        raise HTTPException(status_code=400,
                            detail=f"Missing required parameter(s): {', '.join(missing)}")


def dev_routes_enabled():
    # INFO: dev-only routes stay hidden (404) unless explicitly enabled (T4.12)
    return os.environ.get("TRACKENN_DEV_ROUTES", "").strip() == "1"


def event_stream(generator):
    return StreamingResponse(generator, media_type='text/event-stream')


def test_response(data):
    if inspect.isgenerator(data):
        return event_stream(data)
    return JSONResponse(data)


@app.get('/recreatedb')  # NOTE: Used to test cases
def recreatedb(request: Request):
    if not dev_routes_enabled():
        raise HTTPException(status_code=404)
    params = query_params(request)
    params['config'] = get_config(request)
    data = eth.recreate_db(params)
    return test_response(data)


@app.get('/internal-test-1')  # NOTE: Used to test cases
def internal_test_1(request: Request):
    if not dev_routes_enabled():
        raise HTTPException(status_code=404)
    params = query_params(request)
    params['config'] = get_config(request)
    data = eth.test_function_1(params)
    return test_response(data)


@app.get('/internal-test-2')  # NOTE: Used to test cases
def internal_test_2(request: Request):
    if not dev_routes_enabled():
        raise HTTPException(status_code=404)
    params = query_params(request)
    params['config'] = get_config(request)
    data = eth.test_function_2(params)
    return test_response(data)


@app.get('/stream-data-checking')
def get_data_checking(request: Request):
    params = query_params(request)
    config = get_config(request)
    require_params(params, ('action',))
    if params['action'] == 'reset':
        # INFO: Deleting the DB is a side effect, GET (EventSource) must not do it
        return JSONResponse({"msg": "Unsupported action 'reset', use POST /reset_db to delete the database",
                             "end": True, "error": True, "content": {}}, status_code=400)
    config['action'] = params['action']
    config['graph'] = params.get('graph', "Star")
    config['filters'] = get_filters(params)
    logger.debug(f"PARAM: {api_client.public_params(config)}")

    return event_stream(misc.event_stream_checking(config))


@app.get('/stream-data-info')
def get_data_info(request: Request):
    params = query_params(request)
    require_params(params, ('address', 'network'))
    params['config'] = get_config(request)
    params['filters'] = get_filters(params)
    # INFO: same default as /stream-data-checking; a missing graph crashed the
    #       stream with KeyError('graph') further down (T4.15)
    params['graph'] = params.get('graph', "Star")
    logger.debug(f"PARAM: {api_client.public_params(params)}")

    if (params['network'] == 'bsc'):
        return event_stream(bsc.event_stream_bsc(params))
    else:
        return event_stream(eth.event_stream_ether(params))


@app.get('/download_db')
def download_investigation(request: Request):
    # HACK: Multiuser?
    file_path = get_config(request)['dbname']
    return FileResponse(file_path, filename=os.path.basename(file_path))


@app.post('/reset_db')
def reset_investigation(request: Request):
    # INFO: A cross-site <form method=post> is a simple request, CORS alone does not stop its side effects
    origin = request.headers.get('origin')
    if origin is not None and origin not in FRONT_ORIGINS:
        return JSONResponse({"msg": "Origin not allowed", "deleted": False, "error": True}, status_code=403)

    # HACK: Multiuser?
    result = misc.reset_db(get_config(request)['dbname'])
    return JSONResponse(result, status_code=500 if result['error'] else 200)


@app.get('/config/keys')
def get_explorer_keys(request: Request):
    # INFO: Never answer the key value itself, only whether one is configured
    config = get_config(request)
    return JSONResponse({
        "ethscan": bool(config.get('ethscan')),
        "bscscan": bool(config.get('bscscan')),
    })


@app.post('/config/keys')
async def set_explorer_keys(request: Request):
    body = await request.json()
    updates = {name: str(body[name]).strip() for name in ('ethscan', 'bscscan') if name in body}
    if not updates:
        raise HTTPException(status_code=400, detail="No explorer key given (ethscan, bscscan)")
    blank = [name for name, value in updates.items() if not value]
    if blank:
        raise HTTPException(status_code=400, detail=f"Key(s) must not be blank: {', '.join(blank)}")

    config_path = request.app.state.config_path
    with open(config_path) as file:
        config = yaml.safe_load(file)
    config.update(updates)
    with open(config_path, 'w') as file:
        yaml.safe_dump(config, file)

    request.app.state.config.update(updates)
    return JSONResponse({
        "ethscan": bool(request.app.state.config.get('ethscan')),
        "bscscan": bool(request.app.state.config.get('bscscan')),
    })


def main(argv=None):
    # INFO: Parse arguments first, --help and bad options must not need config.yaml
    parser = argparse.ArgumentParser()
    parser.add_argument('-i', '--ip', action='store', default='127.0.0.1',
                        help='IP address to bind the API and the front (default 127.0.0.1)')
    # TODO: Port
    parser.add_argument('-e', '--env', choices=["prod", "dev"], 
                        default="prod", help='Environment [dev, prod]')
    parser.add_argument('--api-only', action='store_true',
                        help='Run only the API, without the front server (Docker serves the front with nginx)')

    args = parser.parse_args(argv)
    ip = str(args.ip)
    env = str(args.env)

    # INFO: Read config and keys
    config = load_config()

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

    if args.api_only:
        logger.info('Starting API only')
        API_Server(ip=ip, port=5000, config=config, env=env)
    elif (env == "prod"):
        logger.info('Starting PROD servers')
        logger.info("API starting...")
        kwargs_api = {"ip": ip, "port": 5000, "config": config}
        context = process_context()
        API_proc = context.Process(name='API',
                                             target=API_Server,
                                             kwargs=kwargs_api)
        API_proc.daemon = True

        logger.info("HTTPD starting...")
        httpd_proc = context.Process(name='httpd',
                                             target=httpServer,
                                             kwargs={"ip": ip})
        httpd_proc.daemon = True

        API_proc.start()
        httpd_proc.start()
        API_proc.join()
        httpd_proc.join()
    else:
        API_Server(ip, port=5000, env='desa', config=config)


if __name__ == '__main__':

    # INFO: For Windows platform
    if sys.platform == 'win32':
        multiprocessing.freeze_support()

    main()

