from basemodule import Singleton
from debugmodule import Log
import pymongo


class MongoManager(Singleton):
    def __init__(self, conf_path='../conf/mongo.conf'):
        self.tag = 'MongoManager'

        self.conf_parser = None
        self.conn = None

        self._bind_conf(conf_path)
        self._connect()

    def _bind_conf(self, conf_path):
        import configparser
        conf_section = 'ConnectionInfo'
        Log.d(self.tag, 'load .conf file:: filepath:', conf_path, ', section: ', conf_section)

        self.conf_parser = configparser.ConfigParser()
        self.conf_parser.read(conf_path)

        self.ip = self.conf_parser.get(conf_section, 'ip')
        self.port = self.conf_parser.get(conf_section, 'port')
        self.id = self.conf_parser.get(conf_section, 'id')
        self.password = self.conf_parser.get(conf_section, 'password')

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def select_doc(self, filter: dict, db_name: str, collection_name: str):
        db = self.conn.get_database(db_name)
        collection = db.get_collection(collection_name)
        return collection.find(filter)

    def insert_doc(self, data: dict, db_name: str, collection_name: str):
        db = self.conn.get_database(db_name)
        collection = db.get_collection(collection_name)
        id = collection.insert_one(data).inserted_id
        Log.d(self.tag, 'insertion succeed:', data[:50] + '...' if len(str(data)) > 50 else data, ':', id)

    def delete_doc(self):
        pass

    def _connect(self):
        if self.conn:
            Log.d(self.tag, 'already connected: ', self.ip, self.port, self.id)

        try:
            self.conn = pymongo.MongoClient('mongodb://%s:%s@%s:%s/' % (self.id, self.password, self.ip, self.port))
            dict_serverinfo = self.conn.server_info()
            Log.d(self.tag, 'Succesfully connected to MongoDB Server :: ok:', dict_serverinfo['ok'],
                  ', version:', dict_serverinfo['version'])
        except Exception as e:
            Log.e(self.tag, 'failed to connect to MongoDB Server (%s)' % e.__class__.__name__)
            Log.e(self.tag, e.with_traceback)

    def close(self):
        Log.d(self.tag, 'disconnecting from MongoDB Server...')
        self.conn.close()
