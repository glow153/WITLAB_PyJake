import requests
import json
import csv
import datetime
import os
import pandas as pd
from basemodule import PySparkManager


class RealtimeParticulateMatter:
    _json_dict = {}
    _data_dict = {}
    _pdf = None
    _spdf = None

    def __init__(self, service_key):
        self._base_url = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/' \
                    'getMsrstnAcctoRltmMesureDnsty'
        self._column = ['station', 'datehour', 'khaiGrade', 'khaiValue',
                        'no2', 'co', 'o3', 'so2', 'pm10', 'pm25']
        self._station_list = ['성성동', '성거읍', '성황동', '백석동']

        self._service_key = service_key
        self._hdfs_spdf_path = 'hdfs:///pm/airkorea/pm_realtime.parquet'
        # self._hdfs_tmp_spdf_path = 'hdfs:///pm/airkorea/pm_realtime_tmp.parquet'

    # 필요성 : 미세먼지 api의 datetime 데이터 중 시간 값은 1~24시로 되어있음,
    #         이는 해당 1시간 동안 누적한 미세먼지의 양을 의미하나 기존 사용해오던 datehour의 시간형식과 다르므로
    #         맞춰줄 필요성이 있음
    def _datetime_corrector(self, sDatetime):
        sdate = sDatetime.split()[0]
        stime = sDatetime.split()[1].split(':')[0]
        dtdate = datetime.datetime.strptime(sdate, '%Y-%m-%d')
        if stime == '24':
            oneday = datetime.timedelta(days=1)
            dtdate += oneday
            sdate = dtdate.strftime('%Y-%m-%d')
            stime = '00'
        return sdate + ' ' + stime

    def _req_api(self, station='nearest', term='daily', debug=False):
        queryParams = '?ServiceKey=' + self._service_key
        if station == 'nearest':
            queryParams += '&stationName=성성동'
        else:
            queryParams += '&stationName=' + station

        if term == 'daily' or term == 'hourly':
            queryParams += '&dataTerm=DAILY&numOfRows=25'
        elif term == 'month':
            queryParams += '&dataTerm=MONTH&numOfRows=744'
        elif term == '3month':
            queryParams += '&dataTerm=3MONTH&numOfRows=2232'

        queryParams += '&_returnType=json'

        self._json_dict = json.loads(requests.get(self._base_url + queryParams).text)

    def _json2pdf(self, term, debug=False):
        param = self._json_dict['parm']
        data_list = self._json_dict['list']

        """
        dict to pandas df 예제
        >>> data = {'row_1': [3, 2, 1, 0], 'row_2': ['a', 'b', 'c', 'd']}
        >>> pd.DataFrame.from_dict(data, orient='index')
               0  1  2  3
        row_1  3  2  1  0
        row_2  a  b  c  d

        우리에게 rowkey는 필요없다
        pm 데이터 스키마는 아래와 같다

        parm
            stationName -> station
        list
            dataTime -> datehour
            khaiGrade
            khaiValue
            no2Value -> no2
            coValue -> co
            o3Value -> o3
            pm10Value -> pm10
            pm25Value -> pm25
            so2Value -> so2

        mapping) json key 값 -> pandas DataFrame column name
        """
        rawdata = []  # 리스트로 만든 raw data, 이걸 df로 변환할 예정
        station = param['stationName']

        for data in data_list:  # api로 1회 가져온 데이터를 스키마에 맞게 매핑하고 한줄씩 리스트에 추가하기
            row = [station, self._datetime_corrector(data['dataTime']),
                   data['khaiGrade'], data['khaiValue'],
                   data['no2Value'], data['coValue'], data['o3Value'], data['so2Value'],
                   data['pm10Value'], data['pm25Value']]
            rawdata.append(row)

        if debug:
            print('debug> get data from api:', rawdata)

        self._pdf = pd.DataFrame(rawdata)  # make pandas dataframe
        self._pdf.columns = self._column  # set new column name

        if term == 'hourly':
            self._pdf = self._pdf.sort_values(by=['datehour'], ascending=False).iloc[:1]

        # print(self._pdf)  # for debug

    def _pdf2parquet(self, mode='append', hdfs_path='', debug=False):
        print('>>', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'api data:', list(self._pdf.iloc[0]))
        self._spdf = PySparkManager().sqlctxt.createDataFrame(self._pdf)  # make spark dataframe

        if hdfs_path == '':  # to default location
            self._spdf.write.mode(mode).parquet(self._hdfs_spdf_path)  # append new data to old parquet
        else:
            self._spdf.write.mode(mode).parquet(hdfs_path)  # append new data to old parquet

    def normalize_parquet(self):  # 중복제거, 시간정렬 등
        spdf = PySparkManager().sqlctxt.read.parquet(self._hdfs_spdf_path).cache()
        spdf_new = spdf.distinct().sort('station', 'datehour').cache()
        spdf_new.write.mode('overwrite').parquet(self._hdfs_spdf_path)

    def parquet2csv_oldver(self, local_outfilepath):
        outfile = open(local_outfilepath, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(outfile)

        raw_colume_name = ['dataTime', 'pm10Value', 'pm25Value']
        if os.path.getsize(local_outfilepath) > 0:
            csv_writer.writerow(raw_colume_name)
        lsData = self._data_dict['list']
        lsData_rev = []
        # reverse row list
        for i in reversed(range(len(lsData))):
            lsData_rev.append(lsData[i])

        for item in lsData_rev:
            row = []
            for key in raw_colume_name:
                row.append(item[key])
            row[0] = self._datetime_corrector(row[0])
            csv_writer.writerow(row)

    def log(self, station='cheonan_all', term='daily', mode='append', debug=False):
        if station == 'cheonan_all':
            for _station in self._station_list:
                self._req_api(station=_station, term=term, debug=debug)
                self._json2pdf(term=term, debug=debug)
                self._pdf2parquet(mode=mode, debug=debug)

        else:
            self._req_api(station=station, term=term, debug=debug)
            self._json2pdf(term=term, debug=debug)
            self._pdf2parquet(mode='append', debug=debug)

    def get_json_dict(self):
        return self._json_dict

    def get_pdf(self):
        return self._pdf

    def get_spdf(self):
        return self._spdf


if __name__ == '__main__':
    key = 'zo2rUB1wM3I11GNZFDuB84l4C94PZjP6cEb4qEff%2B94h83%2Fihaj1JJS75%2Bm0uHdFCchJw7SyGE0HZgKiZDpq%2FA%3D%3D'
    pm = RealtimeParticulateMatter(key)
    # pm.log(station='cheonan_all', term='month', mode='append')

    # normalize
    pm.normalize_parquet()

