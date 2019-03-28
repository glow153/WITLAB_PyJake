from abc import (abstractmethod, ABCMeta)
from basemodule import PySparkManager
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

    def __init__(self, base_url, service_key, column_list, hdfs_spdf_path, tag, debug):
        self._base_url = base_url
        self._service_key = service_key
        self._column = column_list
        self._hdfs_spdf_path = hdfs_spdf_path
        self._tag = tag
        self._debug = debug

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

    def _req_api(self, query_param):
        json_response = requests.get(self._base_url + query_param)
        self._json_dict = json.loads(json_response)

    @abstractmethod
    def _json2pdf(self, term):
        pass

    def _pdf2parquet(self, mode='append', hdfs_path=''):
        print('>>', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'api data:', list(self._pdf.iloc[0]))
        self._spdf = PySparkManager().sqlctxt.createDataFrame(self._pdf)  # make spark dataframe

        if hdfs_path == '':  # to default location
            self._spdf.write.mode(mode).parquet(self._hdfs_spdf_path)  # append new data to old parquet
        else:
            self._spdf.write.mode(mode).parquet(hdfs_path)  # append new data to old parquet

    # @abstractmethod
    # def parquet2csv(self):
    #     pass

    @abstractmethod
    def log(self, mode='append', **kwargs):
        query_param = self._make_query_param(**kwargs)
        self._req_api(query_param)
        self._json2pdf(kwargs['term'])
        self._pdf2parquet(mode=mode)

    def normalize_parquet(self):  # 중복제거, 시간정렬 등
        spdf = PySparkManager().sqlctxt.read.parquet(self._hdfs_spdf_path).cache()
        spdf_new = spdf.distinct().sort('station', 'datehour').cache()
        spdf_new.write.mode('overwrite').parquet(self._hdfs_spdf_path)

    def get_last_log_datehour(self, db='hdfs'):
        if db == 'hdfs':
            spdf_total = PySparkManager().sqlctxt.read.parquet(self._hdfs_spdf_path)
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



