from abc import (ABCMeta, abstractmethod)
from selenium import webdriver
import os
import datetime
import pandas as pd


class AbsCrawler(metaclass=ABCMeta):
    _options = None
    _driver = None
    _debug = False

    def __init__(self, base_url, crawl_type='static', debug=False):
        self._base_url = base_url
        self._debug = debug

        if crawl_type == 'dynamic':
            self._init_driver()
        else:  # crawl_type == 'static'
            pass

    def _debug_print(self, content):
        if self._debug:
            print('dbg__', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '>', content)

    def _init_driver(self):  # 드라이버와 옵션을 클래스화하여 싱글톤으로 만들면 좋을듯
        self._debug_print('init driver...')

        chrome_driver_path = os.getcwd() + '/driver/chromedriver.exe'
        self._options = webdriver.ChromeOptions()
        self._options.add_argument('headless')
        self._options.add_argument('disable-gpu')
        self._driver = webdriver.Chrome(chrome_driver_path, options=self._options)

        self._debug_print('driver init completed.')

    @abstractmethod
    def _make_url(self, **kwargs):
        pass

    @abstractmethod
    def scrap(self, **kwargs):
        pass

    # kwargs: {username, passwd, host, db_name, table_name}
    def to_db(self, pdf: pd.DataFrame, db_type='mysql', **kwargs):
        self._debug_print('db type : ' + db_type)
        if db_type == 'mysql':
            from sqlalchemy import create_engine
            args = (kwargs['username'], kwargs['passwd'], kwargs['host'], kwargs['port'], kwargs['db_name'])
            engine = create_engine('mysql+pymysql://%s:%s@%s:%d/%s' % args, encoding='utf-8')
            conn = engine.connect()

            # db insert
            pdf.to_sql(name=kwargs['table_name'], con=engine, if_exists='append', index=False)

    def close(self):
        self._debug_print('driver closing...')
        self._driver.close()
        self._debug_print('driver closed.')


