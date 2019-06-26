import datetime
import sys


class Log:
    def __init__(self):
        pass

    @staticmethod
    def _timestamp():
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    @staticmethod
    def _get_header(tag, ):
        return 'dbg_' + tag + ' ' + Log._timestamp() + '>>'

    @staticmethod
    def d(tag, *content):
        print(Log._get_header(tag), *content)

    @staticmethod
    def e(tag, *content):
        print(Log._get_header(tag), *content, file=sys.stderr)

