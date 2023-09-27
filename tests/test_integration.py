
import pandas as pd
from pymongo import MongoClient
from mongoie.core import (
    import_to_mongo,
    import_to_mongo_collection,
    export_collection,
    export_cursor,
    export_from_mongo,
)
import os


def test_can_instantiate_mongo_connector(mongo_client):
    assert isinstance(mongo_client, MongoClient)


def test_can_import_to_collection(
    mongo_client, test_data_json, test_db, test_collection, setup_and_teardown_db
):
    coll = mongo_client.get_collection(test_db, test_collection)
    import_to_mongo_collection(coll, file_path=test_data_json)
    cnt = mongo_client.get_collection_count(test_db, test_collection)
    doc = mongo_client.get_collection(test_db, test_collection).find_one()
    assert cnt > 0 and isinstance(doc, dict)


def test_can_import_to_mongo(
    test_data_json,
    test_db,
    test_collection,
    mongo_uri,
    mongo_client,
    setup_and_teardown_db,
):
    import_to_mongo(
        mongo_uri=mongo_uri,
        db=test_db,
        collection=test_collection,
        file_path=test_data_json,
    )
    cnt = mongo_client.get_collection_count(test_db, test_collection)
    doc = mongo_client.get_collection(test_db, test_collection).find_one()
    assert cnt > 0 and isinstance(doc, dict)


def test_can_import_to_mongo_multi_files(
    test_data_multi_json,
    test_db,
    test_collection,
    mongo_uri,
    mongo_client,
    setup_and_teardown_db,
):
    import_to_mongo(
        mongo_uri=mongo_uri,
        db=test_db,
        collection=test_collection,
        dir_path=test_data_multi_json,
        file_extension="json",
    )
    records = 10 * 1000
    cnt = mongo_client.get_collection_count(test_db, test_collection)
    doc = mongo_client.get_collection(test_db, test_collection).find_one()
    assert cnt == records and isinstance(doc, dict)


def test_can_export_from_mongo(
    _import_test_data_to_mongo,
    test_db,
    test_collection,
    mongo_uri,
    prepare_tmpdir,
    tmp_dir,
    mongo_client,
):
    file_path = os.path.join(tmp_dir, "file.csv")
    export_from_mongo(
        mongo_uri, db=test_db, collection=test_collection, file_path=file_path
    )
    df = pd.read_csv(file_path)
    assert len(df) == 20000
    assert os.path.exists(file_path) is True


def test_can_export_from_mongo_to_multi_files(
    _import_test_data_to_mongo,
    test_db,
    test_collection,
    mongo_uri,
    prepare_tmpdir,
    tmp_dir,
    mongo_client,
):
    file_path = os.path.join(tmp_dir, "file.csv")
    export_from_mongo(
        mongo_uri,
        db=test_db,
        collection=test_collection,
        file_path=file_path,
        file_size=5000,
    )
    assert os.path.exists(tmp_dir) is True
    assert len(os.listdir(tmp_dir)) == 4
    for f in os.listdir(tmp_dir):
        df = pd.read_csv(os.path.join(tmp_dir, f))
        assert len(df) == 5000


def test_can_export_collection(
    _import_test_data_to_mongo,
    test_db,
    test_collection,
    mongo_uri,
    prepare_tmpdir,
    tmp_dir,
    mongo_client,
):
    file_path = os.path.join(tmp_dir, "file.json")
    coll = mongo_client.get_collection(test_db, test_collection)
    export_collection(collection=coll, db=test_db, file_path=file_path)
    df = pd.read_json(file_path)
    assert len(df) == 20000
    assert os.path.exists(file_path) is True


def test_can_export_cursor(
    _import_test_data_to_mongo,
    test_db,
    test_collection,
    mongo_uri,
    prepare_tmpdir,
    tmp_dir,
    mongo_client,
):
    file_path = os.path.join(tmp_dir, "file.parquet")
    cursor = mongo_client.find(test_db, test_collection, {})
    export_cursor(cursor=cursor, db=test_db, file_path=file_path)
    df = pd.read_parquet(file_path)
    assert len(df) == 20000
    assert os.path.exists(file_path) is True
