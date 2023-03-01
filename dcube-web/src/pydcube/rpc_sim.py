#!/usr/bin/env python

import DCM
from DCM.sim import Simulation

import logging
import json
import argparse
import signal
import socket

logger = logging.getLogger(__name__)


def get_hostname():
    return socket.gethostname()


def signal_handler(signum, frame):
    logger.info("Signal to shutdown received")
    exit(0)


signal.signal(signal.SIGINT, signal_handler)

parser = argparse.ArgumentParser(description="D-Cube RPC simulation server.")
parser.add_argument("--credentials", type=str, help="Credential JSON file")
parser.add_argument("--nodes", type=str, help="Node configuration file")
parser.add_argument("--hostname", type=str, default=get_hostname(), help="Override hostname")
parser.add_argument("--debug", action="store_true", help="Enable debug")

args = parser.parse_args()


DEFAULT_LOG_FILE_PATH: str = '/tmp/trace.log'
FORMAT = "%(asctime)s %(name)16s [%(levelname)-8s] %(message)s"

logging.basicConfig(level=logging.DEBUG, format=FORMAT)
logging.getLogger("pika").setLevel(logging.DEBUG)

file_handler: logging.Handler = logging.FileHandler(DEFAULT_LOG_FILE_PATH)
file_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(logging.Formatter(FORMAT))


if args.credentials is None:
    user_name = "guest"
    user_pass = "guest"
else:
    try:
        with open(args.credentials, 'r') as f:
            try:
                j = json.load(f)
                user_name = j["username"]
                user_pass = j["password"]
            except ValueError as e:
                logger.error("Invalid credentials JSON file: %s!", e)
                exit(-1)
    except IOError:
        logger.error("Credential JSON file does not exist or cannot be opened!")
        exit(-1)

if args.nodes is None:
    nodes = []
else:
    try:
        with open(args.nodes, 'r') as f:
            try:
                j = json.load(f)
            except ValueError as e:
                logger.error("Invalid node JSON file: %s!", e)
                exit(-1)
            nodes = j
    except IOError:
        logger.error("Node JSON file does not exist or cannot be opened!")
        exit(-1)

simply = Simulation("rabbitmq", args.hostname, user_name, user_pass, nodes, resturl="http://dcube-web")
simply.run()
