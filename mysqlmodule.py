from .basemodule import Singleton


class MysqlManager(Singleton):
    def __init__(self):
        pass

    def init(self, conn_args: list):
        from sqlalchemy import create_engine
        # args => list ['username', 'passwd', 'host', int(port), 'db_name']
        self.engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % tuple(conn_args), encoding='utf-8')
        self.conn = self.engine.connect()

    def close(self):
        self.conn.close()

