import json
from typing import Union

import click

from mongoie.core.api import (
    export_from_mongo,
    import_to_mongo,
    list_mongo_collections,
    list_mongo_databases,
)


@click.group()
def cli():
    """

    mongoie - simple tool for exporting/importing data from/to mongodb.

    """


@cli.command(
    "export"
)
@click.option("--uri", help="MongoDB host", required=True, type=click.STRING)
@click.option(
    "-d", "--db", "--database", help="MongoDB database", required=True, type=str
)
@click.option("-c", "--collection", help="MongoDB collection", required=True, type=str)
@click.option(
    "-q",
    "--query",
    help="Query/Pipeline for find/aggregate method, can be a json file",
    required=False,
    type=str,
)
@click.option(
    "-f", "-fp", "--file_path", help="output file path", required=True, type=str
)
@click.option("-n", "--normalize", help="Whether to normalize file when exporting", required=False, type=bool)
@click.option("-fs", "--file_size", help="maximum records per file", required=False, type=int)
def mongo_export(uri, db, collection, query, file_path, normalize, file_size):
    """export data from mongo"""
    try:
        query = json.loads(query)
    except Exception:
        query = {}
    export_from_mongo(
        mongo_uri=uri,
        db=db,
        collection=collection,
        query=query,
        file_path=file_path,
        normalize=normalize,
        file_size=file_size
    )


@cli.command("import")
@click.option("-u", "--uri", "--host", help="MongoDB host", required=True, type=str)
@click.option(
    "-d", "--db", "--database", help="Target MongoDB database", required=True, type=str
)
@click.option(
    "-c", "--collection", help="Target MongoDB collection", required=True, type=str
)
@click.option(
    "-f", "-fp", "--file_path", help="Input file path", required=False, type=str
)
@click.option(
    "-dp", "-dir", "--dir_path", help="Input directory  path", required=False, type=str
)
@click.option(
    "-ext", "-ex", "--extension", help="File extension", required=False, type=str
)
@click.option(
    "-p", "-pat", "--pattern", help="File extension", required=False, type=str
)
@click.option(
    "-dn",
    "--denormalized",
    help="Whether the data in the file is denormalized.",
    required=False,
    type=bool,
)
@click.option(
    "-rp", "--prefix", help=" A prefix in denormalized file.", required=False, type=str
)
@click.option(
    "-clr",
    "--clear",
    help="Whether to clear the collection before importing data.",
    required=False,
    type=bool,
    default=True,
)
def mongo_import(mongo_uri, db, collection, fp, dp, ext, pattern, denormalized, record_prefix, clear_before):
    "import to mongo"
    import_to_mongo(
        mongo_uri,
        db=db,
        collection=collection,
        denormalized=denormalized,
        denormalization_record_prefix=record_prefix,
        clear_before=clear_before,
        file_path=fp,
        dir_path=dp,
        file_extension=ext,
        pattern=pattern
    )


# utilit
@click.option("-u", "--uri", "--host", help="MongoDB host", required=True, type=str)
@click.option(
    "-d", "--db", "--database", help="MongoDB database", required=True, type=str
)
@click.option(
    "-r",
    "--re",
    "--regex",
    help="regex to filter collection names",
    required=False,
    type=str,
)
@click.option(
    "-l", "--limit", help="Limit of collections to show", required=False, type=int
)
def list_collections(host, db, regex, limit):
    collections = list_mongo_collections(host, db, regex)
    if limit and limit > 0:
        collections = collections[:limit]
    for c in collections:
        click.echo(c)
