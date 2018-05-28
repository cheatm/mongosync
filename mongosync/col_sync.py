from pymongo import MongoClient, UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database
import logging


def make_update(docs):
    for doc in docs:
        try:
            yield UpdateOne({"_id": doc.pop('_id')}, {"$set": doc}, True)
        except Exception as e:
            logging.error("make update| %s | %s", doc, e)


class SyncCollection(object):

    def __init__(self, source, target, count=128):
        assert isinstance(source, Collection), type(source)
        assert isinstance(target, Collection), type(target)
        self.source = source
        self.target = target
        self.count = count
        self.methods = {
            "insert": self.insert_sync,
            "update": self.update_sync,
            "auto": self.auto_sync
        }

    def sync(self, how="auto"):
        self.methods[how]()

    def find(self):
        chunk = []
        for doc in self.source.find():
            chunk.append(doc)
            if len(chunk) >= self.count:
                yield chunk
                chunk = []

        if len(chunk):
            yield chunk

    def sync_index(self):
        for name, doc in self.source.index_information().items():
            if "_id" in name:
                continue
            try:
                result = self._create(doc)
            except Exception as e:
                logging.error("sync index | %s | %s | %s | %s",
                              self.source.full_name,
                              self.target.full_name,
                              name, e)
            else:
                logging.warning("sync index | %s | %s | %s | %s",
                                self.source.full_name,
                                self.target.full_name,
                                name, result)

    def _create(self, doc):
        doc.pop("v", None)
        doc.pop("ns", None)
        key = doc.pop("key")
        return self.target.create_index(key, **doc)

    def insert_sync(self):
        for chunk in self.find():
            try:
                result = self.target.insert_many(chunk)
                ids = len(result.inserted_ids)
            except Exception as e:
                logging.error("insert sync | %s | %s | %s",
                              self.source.full_name,
                              self.target.full_name,
                              e)
            else:
                logging.warning("insert sync | %s | %s | %s",
                                self.source.full_name,
                                self.target.full_name,
                                ids)
        self.sync_index()

    def update_bulks(self):
        for chunk in self.find():
            yield list(make_update(chunk))

    def update_sync(self):
        self.sync_index()
        for bulks in self.update_bulks():
            try:
                result = self.target.bulk_write(bulks)
                msg = (result.matched_count, result.upserted_count)
            except Exception as e:
                logging.error("update sync | %s | %s | %s",
                              self.source.full_name,
                              self.target.full_name,
                              e)
            else:
                logging.warning("update sync | %s | %s | %s",
                                self.source.full_name,
                                self.target.full_name,
                                msg)

    def auto_sync(self):
        cursor = self.target.find()
        count = cursor.count()
        cursor.close()

        if count == 0:
            self.insert_sync()
        else:
            self.update_sync()


def sync_database(source, target, how="auto", count=128):
    assert isinstance(source, Database), type(source)
    assert isinstance(target, Database), type(target)

    for name in source.collection_names():
        SyncCollection(source[name], target[name], count).sync(how)

