#!/usr/bin/env python
import DCM

# cli arguments
import argparse

# helper
from itertools import chain
import json
import logging

# List of all servers
SERVERS = ["rpi%d" % x for x in range(80, 82)]

# CLI arguments
parser = argparse.ArgumentParser(description="D-Cube RPC Client.")
parser.add_argument("--debug", action="store_true", help="Enable debug")

args = parser.parse_args()


def init_logger() -> logging.Logger:
    default_log_file_path: str = '/tmp/rpc_ping_trace.log'
    logging_format: str = "%(asctime)s %(name)16s [%(levelname)-8s] %(message)s"

    logging.basicConfig(level=logging.DEBUG,
                        format=logging_format)
    logging.getLogger("pika").setLevel(logging.DEBUG)

    file_handler: logging.Handler = logging.FileHandler(default_log_file_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(logging_format))

    log: logging.Logger = logging.getLogger("RPC_SIM")
    log.addHandler(file_handler)

    return log


logger = init_logger()

# clients
# TODO use credentials file! RabbitMQ password
dcube = DCM.Client("broker", "master", "dcube", "GWcq43x2", servers=SERVERS)

# ping all servers
logger.info("Checking if all %d Raspberry Pi nodes are pingable..." % len(SERVERS))
try:
    dcube.ping(timeout=5)
    logger.info("[OK] All nodes could be pinged correctly!")
except DCM.ServersUnresponseException as e:
    logger.error("[ERROR] Following nodes are not pingable:")
    for s in e.servers:
        logger.error("\t%s" % s)
    exit(-1)
