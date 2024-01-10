#!/usr/bin/env python

import sys
import time
import argparse
import psycopg2 as pg
import os
import row_processor as Processor
import six
import json
import string
import random

# Special rules needed for certain tables (esp. for old database dumps)
specialRules = {("Posts", "ViewCount"): "NULLIF(%(ViewCount)s, '')::int"}

# part of the file already downloaded
file_part = None

NUMBATCHES=100
NUMROWS=1
DATASIZE = 16000000

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

def _createMogrificationTemplate(table, keys, insertJson):
    """Return the template string for mogrification for the given keys."""
    table_keys = ", ".join(
        [
            "%(" + k + ")s"
            if (table, k) not in specialRules
            else specialRules[table, k]
            for k in keys
        ]
    )
    if insertJson:
        return "(" + table_keys + ", %(jsonfield)s" + ")"
    else:
        return "(" + table_keys + ")"


def _createCmdTuple(cursor, keys, templ, attribs, insertJson):
    """Use the cursor to mogrify a tuple of data.
    The passed data in `attribs` is augmented with default data (NULLs) and the
    order of data in the tuple is the same as in the list of `keys`. The
    `cursor` is used to mogrify the data and the `templ` is the template used
    for the mogrification.
    """
    defs = _makeDefValues(keys)
    defs.update(attribs)

    if insertJson:
        dict_attribs = {}
        for name, value in attribs.items():
            dict_attribs[name] = value
        defs["jsonfield"] = json.dumps(dict_attribs)

    return cursor.mogrify(templ, defs)


def _getTableKeys(table):
    """Return an array of the keys for a given table"""
    keys = None
    if table == "Users":
        keys = [
            "Id",
            "Reputation",
            "CreationDate",
            "DisplayName",
            "LastAccessDate",
            "WebsiteUrl",
            "Location",
            "AboutMe",
            "Views",
            "UpVotes",
            "DownVotes",
            "ProfileImageUrl",
            "Age",
            "AccountId",
        ]
    elif table == "Badges":
        keys = ["Id", "UserId", "Name", "Date"]
    elif table == "PostLinks":
        keys = ["Id", "CreationDate", "PostId", "RelatedPostId", "LinkTypeId"]
    elif table == "Comments":
        keys = ["Id", "PostId", "Score", "Text", "CreationDate", "UserId"]
    elif table == "Votes":
        keys = ["Id", "PostId", "VoteTypeId", "UserId", "CreationDate", "BountyAmount"]
    elif table == "Posts":
        keys = [
            "Id",
            "PostTypeId",
            "AcceptedAnswerId",
            "ParentId",
            "CreationDate",
            "Score",
            "ViewCount",
            "Body",
            "OwnerUserId",
            "LastEditorUserId",
            "LastEditorDisplayName",
            "LastEditDate",
            "LastActivityDate",
            "Title",
            "Tags",
            "AnswerCount",
            "CommentCount",
            "FavoriteCount",
            "ClosedDate",
            "CommunityOwnedDate",
        ]
    elif table == "Tags":
        keys = ["Id", "TagName", "Count", "ExcerptPostId", "WikiPostId"]
    elif table == "PostHistory":
        keys = [
            "Id",
            "PostHistoryTypeId",
            "PostId",
            "RevisionGUID",
            "CreationDate",
            "UserId",
            "Text",
        ]
    elif table == "Comments":
        keys = ["Id", "PostId", "Score", "Text", "CreationDate", "UserId"]
    return keys


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

#############################################################

parser = argparse.ArgumentParser()

parser.add_argument(
    "-d",
    "--dbname",
    help="Name of database to create the table in. The database must exist.",
    default="stackoverflow",
)

parser.add_argument("-u", "--username", help="Username for the database.", default=None)

parser.add_argument("-p", "--password", help="Password for the database.", default=None)

parser.add_argument(
    "-P", "--port", help="Port to connect with the database on.", default=None
)

parser.add_argument("-H", "--host", help="Hostname for the database.", default=None)

args = parser.parse_args()

try:
    # Python 2/3 compatibility
    input = raw_input
except NameError:
    pass

table = "random_large_strings_external_uncompressed"
handleTable(table, NUMBATCHES, NUMROWS, DATASIZE)