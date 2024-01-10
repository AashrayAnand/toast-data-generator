#!/usr/bin/env python

import sys
import time
import argparse
import psycopg2 as pg
import os
import six
import json
import string
import random
from multiprocessing import Process

NUMPROCS=25
NUMBATCHES=100
NUMROWS=1
DATASIZE = 16 * 1024 * 1024 # Default to 16Mb value size.

def getConnectionParameters():
    """Get the parameters for the connection to the database."""

    parameters = {}

    if args.dbname:
        parameters['dbname'] = args.dbname

    if args.host:
        parameters['host'] = args.host

    if args.port:
        parameters['port'] = args.port

    if args.username:
        parameters['user'] = args.username

    if args.password:
        parameters['password'] = args.password

    return parameters

def handleTable(table, batches, records, recordsize):
    start_time = time.time()

    try:
        with pg.connect(**getConnectionParameters()) as conn:
            for i in range(batches):
                with conn.cursor() as cur:
                    # Handle content of the table
                    start_time = time.time()
                    six.print_("Inserting data ...")
                    valuesStr = ',\n'.join(["('" + ''.join(random.choices(string.ascii_uppercase + string.digits, k=recordsize)) + "')" for i in range(records)])
                    cmd = (
                        "INSERT INTO "
                        + table
                        + " VALUES\n"
                        + valuesStr
                        + ";"
                    )
                    cur.execute(cmd)
                    conn.commit()
                    six.print_(
                        "Batch '{0}' Table '{1}' processing took {2:.1f} seconds".format(
                            i, table, time.time() - start_time
                        )
                    )

    except pg.Error as e:
        six.print_("Error in dealing with the database.", file=sys.stderr)
        six.print_("pg.Error ({0}): {1}".format(e.pgcode, e.pgerror), file=sys.stderr)
        six.print_(str(e), file=sys.stderr)
    except pg.Warning as w:
        six.print_("Warning from the database.", file=sys.stderr)
        six.print_("pg.Warning: {0}".format(str(w)), file=sys.stderr)

def dispatchTableHandlers(table, procs, batches, records, recordsize):
    proclist = []
    for i in range(int(procs)):
        proci = Process(target=handleTable, args=(table, int(batches), int(records), int(recordsize),))
        proclist.append(proci)
        proci.start()

    # complete the processes
    for proc in proclist:
        proc.join()

#############################################################

parser = argparse.ArgumentParser()

parser.add_argument(
    "-d",
    "--dbname",
    help="Name of database to create the table in. The database must exist.",
    required=True
)

parser.add_argument(
    "-t",
    "--table",
    help="Name of the table to load data into. The table must exist.",
    required=True
)

parser.add_argument(
    "-u", 
    "--username", 
    help="Username for the database.", 
    required=True)

parser.add_argument(
    "-p", 
    "--password", 
    help="Password for the database.", 
    required=True
)

parser.add_argument(
    "-P", 
    "--port", 
    help="Port to connect with the database on.", 
    default=5432
)

parser.add_argument(
    "-H", 
    "--host", 
    help="Hostname for the database.", 
    required=True
)

parser.add_argument(
    "-n", 
    "--procs", 
    help="Number of processes to parallelize insert", 
    default=NUMPROCS
)

parser.add_argument(
    "-b", 
    "--batches", 
    help="Number of insert batches", 
    default=NUMBATCHES
)

parser.add_argument(
    "-r", 
    "--rows", 
    help="Number of rows per batch", 
    default=NUMROWS
)

parser.add_argument(
    "-D", 
    "--datasize", 
    help="Size (in bytes) of each column value", 
    default=DATASIZE
)

args = parser.parse_args()

try:
    # Python 2/3 compatibility
    input = raw_input
except NameError:
    pass

table = args.table
dispatchTableHandlers(table, args.procs, args.batches, args.rows, args.datasize)
