from abc import (abstractmethod, ABCMeta)
from basemodule import (PySparkManager, MysqlManager)
from selenium import webdriver

import pandas as pd
import requests
import json
import threading
import datetime
import time


class AbsApi(metaclass=ABCMeta):
    _json_dict = {}
    _pdf = None
    _spdf = None
    _debug = False

    def __init__(self, base_url: str, service_key: str, column_list: list,
                 hdfs_path: str, mysql_conn_args: list, debug: bool):
        self._base_url = base_url
        self._service_key = service_key
        self._column = column_list
        self._hdfs_path = hdfs_path
        self._mysql_conn_args = mysql_conn_args
        self._debug = debug

    def debug_print(self, content: str):
        if self._debug:
            print('dbg_' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + '>', content)

    @abstractmethod
    def _make_query_param(self, **kwargs):
        """
        make api query string.
        내부 메소드, _req_api()로부터 호출됨
        :param kwargs: {'station': 지점명 또는 지역명
                        'time': 데이터 측정 시간 및 날짜}
        :return: nothing
        """
        pass

    def _req_api(self, query_param: str):
        json_response = requests.get(self._base_url + query_param)
        self._json_dict = json.loads(json_response.text)

    @abstractmethod
    def _json2pdf(self, **kwargs):
        """
        _req_api()에 의해 AbsApi 객체 내부변수로 생성된 dict형의 json을 pandas dataframe으로 변환.
        api마다 json형식이 모두 다르므로 개발자가 직접 구현해주어야함,
        pdf 객체를 만든 다음엔 AbsApi 객체 내부변수로 저장, 리턴 x
        :param kwargs: pandas dataframe 생성에 필요한 input data
        :return: nothing
        """
        pass

    def pdf2hdfs(self, mode='append', hdfs_path=''):
        """
        parquet 형식으로 저장
        :param mode: 저장 방식, 'append', 'overwrite'
        :param hdfs_path: hdfs 경로 지정, 입력 없으면 기본 저장경로
        :return: nothing
        """
        if hdfs_path == '':
            path = self._hdfs_path
        else:
            path = hdfs_path

        self.debug_print('pdf -> parquet :: ' + str(list(self._pdf.iloc[0])))

        # make spark dataframe
        self._spdf = PySparkManager().sqlctxt.createDataFrame(self._pdf)

        # append new data
        self._spdf.write.mode(mode).parquet(path)

        self.debug_print('parquet write completed.')

    def pdf2mysql(self, table_name: str, if_exists: str = 'append'):
        """
        mysql에 테이블 형식으로 저장, 테이블이 있어야 함 (테이블 없을 시 새로 생성 기능도 추가해야 함)
        :param table_name: 테이블 명,
        :param if_exists: to_sql() params, ex. 'append', 'replace', 'fail'
        :return: nothing
        """
        self.debug_print('pdf -> mysql :: ' + str(list(self._pdf.iloc[0])))

        # connect to mysql
        mm = MysqlManager()
        mm.init(self._mysql_conn_args)

        # write to sql
        self._pdf.to_sql(name=table_name, con=mm.engine, if_exists=if_exists, index=False)

        # db close
        mm.close()

        self.debug_print('mysql write completed.')

    def pdf2csv(self, out_path: str):
        """
        make pandas dataframe to csv file
        :return: nothing
        """
        self._pdf.to_csv(out_path, columns=self._column, index=False)

    @abstractmethod
    def log(self, db_type: list, mode='append', **kwargs):
        """
        api의 최소 측정 단위를 로깅하는 메소드
        :param db_type: 로깅할 db 종류 리스트
        :param mode: default 'append'
        :param kwargs: 필요한 외부 변수
        :return: nothing
        """
        pass

    def normalize_parquet(self, hdfs_path=''):
        """
        parquet 형식의 spark dataframe을 중복제거, 시간 정렬 등 정규화(normalize)하는 메소드
        로깅을 같은 날 데이터를 두번 했다거나 하면 한번씩 normalize 해줘야함
        :param hdfs_path:
        :return:
        """

        if hdfs_path == '':  # default path
            path = self._hdfs_path
        else:  # specific path
            path = hdfs_path

        spdf = PySparkManager().sqlctxt.read.parquet(path).cache()
        spdf_new = spdf.distinct().sort('station', 'datehour').cache()
        spdf_new.write.mode('overwrite').parquet(path)

    def get_last_log_datehour(self, db='hdfs'):
        if db == 'hdfs':
            spdf_total = PySparkManager().sqlctxt.read.parquet(self._hdfs_path)
            last_date = spdf_total.sort(spdf_total.datehour.desc()).first()
            return last_date['datehour']
        else:
            pass

    def get_json_dict(self):
        return self._json_dict

    def get_pdf(self):
        return self._pdf

    def get_spdf(self):
        return self._spdf


class AbsLogger(threading.Thread, metaclass=ABCMeta):

    def __init__(self, api_obj, tag, debug=False):
        threading.Thread.__init__(self)

        self.on = True
        self.running = False
        self.debug = debug

        self._api_obj = api_obj
        self._tag = tag

    def current_time_millis(self):
        return int(round(time.time() * 1000))

    def run(self):
        last_ms = self.current_time_millis()
        current_ms = 0

        print(self._tag, '> logging start...')
        self._api_obj.log()
        print(self._tag, '> logging end.')

        while self.on:
            current_ms = self.current_time_millis()

            if current_ms - last_ms > 3600000:
                last_ms = current_ms
                print('logging start...')
                self._api_obj.log()
                print('logging end.')

            time.sleep(0.001)


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


