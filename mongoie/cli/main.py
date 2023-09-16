from typing import Union

import click

from mongoie.core.api import export_from_mongo, import_to_mongo


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option("-u", "--uri", "--host", help="MongoDB host", required=True, type=str)
@click.option(
    "-d", "--db", "--database", help="MongoDB database", required=True, type=str
)
@click.option("-c", "--collection", help="MongoDB collection", required=True, type=str)
@click.option(
    "-q",
    "--query",
    help="Query/Pipeline for find/aggregate method, can be a json file",
    required=False,
    type=Union[dict, list, str],
)
@click.option(
    "-f", "-fp", "--file_path", help="output file path", required=True, type=str
)
@click.pass_context
def mongo_export(host, db, collection, query, file_path):
    """export data from mongo"""
    export_from_mongo(
        host,
        db=db,
        collection=collection,
        query=query,
        file_path=file_path,
    )


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option("-u", "--uri", "--host", help="MongoDB host", required=True, type=str)
@click.option(
    "-d", "--db", "--database", help="Target MongoDB database", required=True, type=str
)
@click.option(
    "-c", "--collection", help="Target MongoDB collection", required=True, type=str
)
@click.option(
    "-f", "-fp", "--file_path", help="Input file path", required=True, type=str
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
def mongo_import(uri, db, collection, fp, denormalized, record_prefix, clear_before):
    "import to mongo"
    import_to_mongo(
        uri,
        db=db,
        collection=collection,
        denormalized=denormalized,
        denormalization_record_prefix=record_prefix,
        clear_before=clear_before,
        file_path=fp,
    )
