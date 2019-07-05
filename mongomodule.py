from .basemodule import Singleton
from .debugmodule import Log
import pymongo


class MongoManager(Singleton):
    def __init__(self, ip, port, user_id, passwd):
        self.ip = ip
        self.port = port
        self.user_id = user_id
        self.passwd = passwd
        self.tag = 'MongoManager'
        self.conn = None

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create_db(self):
        pass

    def create_collection(self):
        pass

    def insert_doc(self):
        pass

    def delete_doc(self):
        pass

    def connect(self):
        if self.conn:
            Log.d(self.tag, 'already connected: ', self.ip, self.port, self.user_id)
        Log.d(self.tag, 'connect to MongoDB Server:', self.ip, self.port)
        self.conn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self.user_id, self.passwd, self.ip, self.port))

    def close(self):
        Log.d()
        self.conn.close()
