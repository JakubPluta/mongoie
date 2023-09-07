from typing import Union

import click

from mongoie.core.api import write_mongo_to_json


@click.command(
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    )
)
@click.option("-u", "--uri", '--host', help="MongoDB host", required=True, type=str)
@click.option(
    "-d", "--db", "--database", help="MongoDB database", required=True, type=str
)
@click.option("-c", "--collection", help="MongoDB collection", required=True, type=str)
@click.option("-q", "--query", help="Query/Pipeline for find/aggregate method", required=False, type=Union[dict, list])
@click.option(
    "-f", "-fp", "--file_path", help="output file path", required=True, type=str
)
@click.pass_context
def from_mongo_writer(ctx, host, db, collection, query, file_path):
    write_mongo_to_json(
        host,
        db=db,
        collection=collection,
        query=query,
        file_path=file_path,
    )

