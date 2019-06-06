import datetime
import sys


class DbgModule:
    def __init__(self, tf=True, tag=''):
        self._tf = tf
        self._tag = tag

        self._header = 'dbg_' + self._tag + ' ' + self._timestamp() + '>>'

    def _timestamp(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def print_p(self, *content):
        if self._tf:
            print(self._header, *content)

    def print_e(self, *content):
        if self._tf:
            print(self._header, *content, file=sys.stderr)

