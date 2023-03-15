#!/usr/bin/env python

import DCM

# timezone format
import pytz
import datetime

# sleep
import time

# signal handler
import signal

# cli arguments
import argparse

# file operations
import os

# helper
from itertools import chain
import json
import logging

# used for logfile
import base64
import sys
from zipfile import ZipFile, ZIP_DEFLATED

MASTER_PID = os.getpid()


def init_logger() -> logging.Logger:
    default_log_file_path: str = '/tmp/trace.log'
    logging_format: str = "%(asctime)s %(name)16s [%(levelname)-8s] %(message)s"

    logging.basicConfig(level=logging.DEBUG,
                        format=logging_format)

    logging.getLogger("pika").setLevel(logging.DEBUG)
    logging.getLogger("urllib3").setLevel(logging.DEBUG)

    file_handler: logging.Handler = logging.FileHandler(default_log_file_path)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(logging_format))

    log: logging.Logger = logging.getLogger("RPC_TESTBED_LINUX")
    log.addHandler(file_handler)

    return log


logger = init_logger()


def signal_handler(signum, frame):
    if os.getpid() == MASTER_PID:
        logger.error("Signal %s caught, terminating experiment!" % signum)
        stop = datetime.datetime.now()
        stop = stop.replace(tzinfo=pytz.utc)
        stop = stop.astimezone(tz)
        try:
            if start:
                runtime = stop - start
                logger.info("Experiment terminated on %s after %s seconds." % (
                    stop.strftime(DATEFORMAT), int(runtime.total_seconds())))
        except NameError:
            logger.info("Experiment terminated on %s." % stop.strftime(DATEFORMAT))
            pass
        dcube.terminate()
        exit(-2)
    exit(0)


# Program loop parameters
PING_MAX = 40
PING_DELAY = 10
PROGRAM_RETRY_MAX = 10

# POE loop parameters
POE_RETRY_MAX = 5

# Local time format
DATEFORMAT = "%a %b %d %H:%M:%S %Z %Y"
tz = pytz.timezone("CET")

# Where to put the logfiles for the webserver
LOGFILEPATH = "/storage/logfiles"

# CLI arguments
parser = argparse.ArgumentParser(description="D-Cube RPC Client.")
parser.add_argument("--job_id", type=int, required=True, dest="job_id", help="Job ID to be run")
parser.add_argument("--topology", type=str, required=True, dest="topology_json", help="JSON formatted switch topology")
parser.add_argument("--debug", action="store_true", help="Enable debug")
parser.add_argument("--broker", type=str, required=True, dest="broker", help="Broker IP")

args = parser.parse_args()


# Pretty print functions
def print_job(job):
    logger.info("Experiment ID: %d" % job["id"])
    logger.info("Competing team number: %s" % job["group"])
    logger.info("Experiment duration: %d" % job["duration"])
    logger.info("Serial log: %s" % ("enabled" if job["logs"] else "disabled"))
    logger.info("Jamming: %s" % job["jamming_short"])


def print_motes(motes):
    for k in motes.keys():
        mote = motes[k]
        logger.info("%s:\t%s" % (k, mote))


JOB = args.job_id

################################################################################
# TODO implement PoE reboot logic
################################################################################
if args.topology_json:
    with open(args.topology_json, "r") as f:
        topology = json.load(f)
else:
    topology = {}

SERVERS = []
for switch in topology:
    for node in switch["nodes"]:
        SERVERS.append(node["hostname"])

BROKER = args.broker

# clients
# TODO use credentials file! RabbitMQ password
dcube = DCM.Client(BROKER, "master", "dcube", "12345", servers=SERVERS)
rest = DCM.RESTClient("http://dcube-web")

# print startup banner
startup = datetime.datetime.now()
startup = startup.replace(tzinfo=pytz.utc)
startup = startup.astimezone(tz)
banner = "Programming script started! (%s)" % startup.strftime(DATEFORMAT)
logger.info(banner)
logger.info("=" * len(banner))
job = rest.get_job(JOB)
print_job(job)
logger.info("=" * len(banner))

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

# ping all servers
logger.info("Checking if all %d Linux nodes are pingable..." % len(SERVERS))
try:
    dcube.ping()
    logger.info("[OK] All nodes could be pinged correctly!")
except DCM.ServersUnresponseException as e:
    logger.error("[ERROR] Following nodes are not pingable:")
    for s in e.servers:
        logger.error("\t%s" % s)
    exit(-1)

# Stop any orphaned experiments still in progress, ignore lack of experiments
try:
    dcube.experiment(state=DCM.CommandState.OFF)
except DCM.CommandFailedException as e:
    pass

logger.info("Programming all nodes...")

# start new experiment
dcube.experiment(state=DCM.CommandState.ON, job_id=JOB)
dcube.program(servers=SERVERS)

# start jamming
dcube.jamming(servers=SERVERS)

# start blinkers
dcube.blinker(servers=SERVERS)

# if logs are enabled, start traces
if job["logs"] is True:
    dcube.trace(state=DCM.CommandState.ON)

logger.info("Starting measurements...")

# start measurements
dcube.measurement(state=DCM.CommandState.ON)
dcube.sleep(3)

# print experiment start time and release motes from reset
start = datetime.datetime.now()
start = start.replace(tzinfo=pytz.utc)
start = start.astimezone(tz)

dcube.mote_reset(state=DCM.CommandState.OFF)
logger.info("Experiment started on %s. Duration: %s seconds..." % (start.strftime(DATEFORMAT), job["duration"]))

# wait for job duration
dcube.sleep(job["duration"])

# reset all nodes again and print experiment stop time
dcube.mote_reset(state=DCM.CommandState.ON)

stop = datetime.datetime.now()
stop = stop.replace(tzinfo=pytz.utc)
stop = stop.astimezone(tz)

logger.info("Experiment terminated on %s." % stop.strftime(DATEFORMAT))
dcube.sleep(5)
dcube.measurement(state=DCM.CommandState.OFF)

# stopping the traces will automatically also collect the logs
if job["logs"] is True:

    logger.info("Collecting Logfiles ...")
    r = dcube.trace(state=DCM.CommandState.OFF)

    if not os.path.exists(LOGFILEPATH):
        os.mkdir(LOGFILEPATH)

    # write base64 encoded replies into a single log zip
    basepath = os.path.join(LOGFILEPATH, str(JOB))
    path = os.path.join(basepath, "logs.zip")

    if not os.path.exists(basepath):
        os.mkdir(basepath)

    if os.path.exists(path):
        os.remove(path)
    with ZipFile(path, 'w', ZIP_DEFLATED) as zf:
        for k in r.keys():
            response = r[k]
            log = base64.b64decode(response["logs"])
            if "ext" in response:
                ext = response["ext"]
            else:
                ext = "txt"
            zf.writestr("log_%s.%s" % (k[3:6], ext), log)

# stopping the experiment will also stop all spawend processes (including jamming and blinker)
dcube.experiment(state=DCM.CommandState.OFF)
logger.info("Programming script terminated!")

dcube.close()
