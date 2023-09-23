import pytest

from mongoie.dal.mongo import MongoConnector, MongoClient


def test_can_instantiate_mongo_connector(mongo_client):
    assert isinstance(mongo_client._client, MongoClient)
    assert mongo_client.db.name == 'test_database'
