import pytest

from mongoie.mongo import MongoConnector, MongoClient


@pytest.fixture
def mongo_client():
    return MongoConnector('localhost:27017', db='test_database')


def test_can_instantiate_mongo_connector(mongo_client):
    assert isinstance(mongo_client._client, MongoClient)
    assert mongo_client.db == 'test_database'
