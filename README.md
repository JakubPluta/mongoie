# mongoie

Simple Tool to export & import mongo data to/from json, csv or parquet in a lazy way



## API

### Exporting data

#### To json
```python

from mongoie.core import export_from_mongo

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"

export_from_mongo(mongo_uri, db=db, collection=collection, query={}, file_path=r".\file.json")
```
#### To CSV
```python

from mongoie.core import export_from_mongo

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"

export_from_mongo(
    mongo_uri, 
    db=db, 
    collection=collection, 
    query={}, 
    file_path=r".\file.csv",
    normalize=True, # normalize nested documents 
    # e.g : "address" : {"city":"London", "country" : "GB"} -> address.city, address.country
)
```
#### To parquet
```python

from mongoie.core import export_from_mongo

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"

export_from_mongo(
    mongo_uri, 
    db=db, 
    collection=collection, 
    query={}, 
    file_path=r".\file.parquet",
    normalize=True, # normalize nested documents 
    # e.g : "address" : {"city":"London", "country" : "GB"} -> address.city, address.country
)
```

#### other ways for exporting data
```python

from mongoie.core import export_cursor, export_collection
from mongoie.dal.mongo import MongoConnector

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"
mongo_client = MongoConnector(mongo_uri, db=db)
coll = mongo_client.get_collection(collection)

export_collection(
    coll, 
    file_path=r".\file.json",
    normalize=True,       
)

cursor = coll.find({"city": {"$eq" : "London"}})

export_cursor(
    cursor,
    file_path=r".\file.json",
    normalize=True,  
)

```


### Importing data 

#### from json
```python

from mongoie.core import import_to_mongo

host = "localhost:27017"
db = "some_db"
collection = "some_collection"

import_to_mongo(host, db=db, collection=collection, file_path=r".\file.json", clear_before=True)
```

### from csv
```python

from mongoie.core import import_to_mongo

host = "localhost:27017"
db = "some_db"
collection = "some_collection"

import_to_mongo(
    host, 
    db=db, 
    collection=collection, 
    file_path=r".\file.csv", 
    clear_before=True, # clear collection before insert
    denormalized=True, # if data is normalized - reverse this process
    # e.g : address.city, address.country -> "address" : {"city":"London", "country" : "GB"}
    # if not provided by default is True
    denormalization_record_prefix="."  # by default normalized data will have records 
    # prefix for nested paths seperated with dot e.g address.city,
    # if not provided it will be default set to dot
)
```

### from parquet
```python

from mongoie.core import import_to_mongo

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"

import_to_mongo(
    mongo_uri, 
    db=db, 
    collection=collection, 
    file_path=r".\file.parquet", 
    clear_before=True, # clear collection before insert
    denormalized=True, # if data is normalized - reverse this process
    # e.g : address.city, address.country -> "address" : {"city":"London", "country" : "GB"}
    # if not provided by default is True
    denormalization_record_prefix="."  # by default normalized data will have records 
    # prefix for nested paths seperated with dot e.g address.city,
    # if not provided it will be default set to dot
)
```

#### Import directly to collection object
```python

from mongoie.core import import_to_mongo_collection
from mongoie.dal.mongo import MongoConnector

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"
mongo_client = MongoConnector(mongo_uri, db=db)
coll = mongo_client.get_collection(collection)

import_to_mongo_collection(
    coll,
    file_path=r".\file.parquet", 
    clear_before=True, # clear collection before insert
    denormalized=True, # if data is normalized - reverse this process
)

```




### importing all json files from given directory that contains `logs` in name
```python

from mongoie.core import import_to_mongo

mongo_uri = "localhost:27017"
db = "some_db"
collection = "some_collection"

import_to_mongo(
    mongo_uri, 
    db=db, 
    collection=collection, 
    dir_path=r"../data/files/", 
    file_extension="json",
    recursive=False,
    pattern=r"*logs*",
    clear_before=True,
    denormalized=False, 
)
```