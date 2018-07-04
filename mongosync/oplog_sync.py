from pymongo.cursor import CursorType
from pymongo import MongoClient
from pymongo import errors
from threading import Thread
from queue import Queue, Empty
from bson.timestamp import Timestamp
from datetime import datetime
import pickle
import logging


def get_ts(time):
    if isinstance(time, str):
        return get_ts(datetime.strptime(time.replace("-", ""), "%Y%m%d"))
    elif isinstance(time, datetime):
        return get_ts(int(time.timestamp()))
    elif isinstance(time, int):
        return Timestamp(time, 1)
    else:
        return None


class OpLogTracker(Thread):

    def __init__(self, client, filename, queue, wait=5, start=None, end=None, filters=None):
        super(OpLogTracker, self).__init__()
        assert isinstance(client, MongoClient), type(client)
        self.client = client
        self.filename = filename
        self.oplog = self.client["local"]["oplog.rs"]
        self.looping = False
        self.queue = queue
        self.start_ts = get_ts(start)
        self.end_ts = get_ts(end)
        self.filters = filters if isinstance(filters, dict) else {}
        self.wait = wait
        self.daemon = True

    def init(self, start=None, end=None, filters=None):
        self.end_ts = end
        if start is not None:
            self.start_ts = start
        else:
            try:
                self.start_ts = self.last_sync_time()
                logging.warning("start set to last sync time")
            except:
                self.start_ts = self.last_log_time()
                logging.warning("start set to last oplog time")
        if isinstance(filters, dict):
            self.filters.update(filters)

    def run(self):
        self.start_loop(self.start_ts, self.end_ts, **self.filters)

    def start(self):
        self.looping = True
        super(OpLogTracker, self).start()

    def join(self, timeout=None):
        # self.looping = False
        super(OpLogTracker, self).join(timeout)

    def last_log_time(self):
        cursor = self.oplog.find().sort([("$natural", -1)])
        doc = next(cursor)
        cursor.close()
        return doc["ts"]

    def last_sync_time(self):
        with open(self.filename, "rb") as f:
            return pickle.load(f)

    def save_sync_time(self, timestamp):
        with open(self.filename, "wb") as f:
            pickle.dump(timestamp, f)

    def get_cursor(self, start=None, end=None, **filters):
        if start is not None and isinstance(start, Timestamp):
            filters.setdefault("ts", {})["$gt"] = start
        if end is not None and isinstance(end, Timestamp):
            filters.setdefault("ts", {})["$lt"] = end
        if len(filters) == 0:
            filters = None
        # return self.oplog.find(filters, cursor_type=CursorType.TAILABLE_AWAIT)
        return self.oplog.find(filters, cursor_type=CursorType.TAILABLE)

    def op_loop(self, start=None, end=None, **filters):
        cursor = self.get_cursor(start, end, **filters)
        # cursor.max_await_time_ms(self.await*1000)
        while self.looping:
            try:
                doc = cursor.next()
            except StopIteration as e:
                # logging.error("tracker cursor stopped | %s", e)
                sleep(self.wait)
            except Exception as e:
                
                logging.error("tracker cursor error | %s", e)
                break
            else:
                yield doc
            
        cursor.close()
        logging.warning("oplog loop stoped")

    def start_loop(self, start=None, end=None, **filters):
        logging.warning("start oplog loop | %s | %s | %s", start, end, filters)
        for doc in self.op_loop(start, end, **filters):
            self.queue.put(doc)


from collections import Iterable

COMMANDS = {"create", "drop"}

class OpLogExecutor(Thread):

    def __init__(self, client, db_map, queue, ts=None):
        super(OpLogExecutor, self).__init__()
        assert isinstance(client, MongoClient), type(client)
        self.client = client

        if isinstance(db_map, dict):
            self.db_map = db_map
        elif isinstance(db_map, Iterable):
            self.db_map = {name: name for name in db_map}
        else:
            raise TypeError("Type of db_map should be dict or Iterable not %s" % type(db_map))

        self.methods = {
            "i": self.insert,
            "u": self.update,
            "d": self.delete,
            "c": self.create
        }

        self.looping = False
        self.queue = queue
        from datetime import datetime
        self.ts = ts if isinstance(ts, Timestamp) else Timestamp(int(datetime.now().timestamp()), 1)
        self.daemon = True

    def run(self):
        self.sync_op()

    def start(self):
        self.looping = True
        super(OpLogExecutor, self).start()

    def join(self, timeout=None):
        self.looping = False
        super(OpLogExecutor, self).join(timeout)

    def sync_op(self):
        while self.looping:
            self._do_sync()

        while self.queue.not_empty:
            self._do_sync()

    def _do_sync(self):
        doc = self.queue.get()
        try:
            self.sync_log(doc)
        except Exception as e:
            logging.error("sync target | %s ", e)

    def sync_log(self, doc):
        op = doc["op"]
        method = self.methods.get(op, None)
        if method:
            db, col = doc["ns"].split(".", 1)
            db = self.db_map.get(db, None)
            if db:
                result = method(db, col, doc)
                logging.debug("sync target | %s | %s | %s | %s", db, col, op, result)
                self.ts = doc["ts"]

    def insert(self, db, col, doc):
        collection = self.client[db][col]
        if col == "system.indexes":
            doc["o"].pop("v", None)
        return collection.insert_one(doc["o"]).inserted_id

    def update(self, db, col, doc):
        collection = self.client[db][col]
        result = collection.update_one(doc["o2"], doc["o"], True)
        return result.matched_count, result.modified_count, result.upserted_id

    def delete(self, db, col, doc):
        collection = self.client[db][col]
        return collection.delete_one(doc["o"]).deleted_count

    def create(self, db, col, doc):
        o = doc["o"]
        result = []
        for command, value in o.items():
            if command in COMMANDS:
                r = self.client[db].command(command, value)
                result.append(r)
        return result


from time import sleep


class SyncOpLogManager(object):

    def __init__(self, source, target, db_map, filename, await=5):
        self.queue = Queue()
        self.tracker = OpLogTracker(source, filename, self.queue, await)
        try:
            ts = self.tracker.last_sync_time()
        except:
            ts = None
        self.executor = OpLogExecutor(target, db_map, self.queue, ts)
        self.filters = {}
        regex = [{"ns": {"$regex": key}} for key in self.executor.db_map]
        if len(regex) == 1:
            self.filters.update(regex[0])
        else:
            self.filters["$or"] = regex

    def start_tracker(self, start=None, end=None):
        self.tracker.init(start, end, self.filters)
        self.tracker.start()

    def check_status(self, start=None, end=None):
        if not self.tracker.is_alive():
            tracker = OpLogTracker(self.tracker.client, self.tracker.filename, self.queue)
            self.tracker = tracker
            self.start_tracker(start, end)

        if not self.executor.is_alive():
            executor = OpLogExecutor(self.executor.client, self.executor.db_map, self.queue, self.executor.ts)
            self.executor = executor
            self.executor.start()

        try:
            self.save_last_sync()
        except:
            pass

        sleep(5)

    def sync(self, start=None, end=None):
        self.start_tracker(start, end)
        self.executor.start()
        # while True:
        #     try:
        #         self.check_status()
        #     except KeyboardInterrupt:
        #         break

        self.tracker.join()
        self.executor.looping=False
        self.executor.join(timeout=0)

    def save_last_sync(self):
        self.tracker.save_sync_time(self.executor.ts)

    def freeze(self):
        self.tracker.save_sync_time(self.tracker.last_log_time())

    def __del__(self):
        self.save_last_sync()


def assert_instance(o, j):
    assert isinstance(o, j), type(o)


def start_sync_oplog(source, target, db_map, ts_file, await=5, start=None, end=None):
    print(source, target, db_map, ts_file, await)
    assert_instance(db_map, (dict, list))
    assert_instance(ts_file, str)
    assert_instance(await, int)
    from pymongo import MongoClient
    source = MongoClient(source)
    target = MongoClient(target)
    manager = SyncOpLogManager(source, target, db_map, ts_file, await)
    manager.sync(start, end)


def main():
    # from pymongo import MongoClient
    # tracker = OpLogTracker(MongoClient(), r"D:\mongosync\mongosync\last_sync_ts", None)
    # print(tracker.last_sync_time())
    # tracker.save_sync_time(Timestamp(1528405396, 2))
    start_sync_oplog(
        source="mongodb://192.168.0.102,192.168.0.101",
        target="mongodb://root:Xingerwudi520factor@dds-wz966a0a3eea1ce41896-pub.mongodb.rds.aliyuncs.com:3717,dds-wz966a0a3eea1ce42204-pub.mongodb.rds.aliyuncs.com:3717/admin?replicaSet=mgset-6035227",
        db_map=["fxdayu_factors"],
        ts_file=r"D:\mongosync\mongosync\last_sync_ts"
    )

if __name__ == '__main__':
    main()