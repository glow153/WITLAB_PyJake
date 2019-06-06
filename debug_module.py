import datetime
import sys


class DbgModule:
    def __init__(self, tag):
        self._tag = tag
        pass

    def _timestamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def print_p(self, *content):
        header = 'dbg_' + self._tag + ' ' + self._timestamp() + '>>'
        print(header, *content)

    def print_e(self, *content):
        header = 'dbg_' + self._tag + ' ' + self._timestamp() + '>>'
        print(header, *content, file=sys.stderr)

