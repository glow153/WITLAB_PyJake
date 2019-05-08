import json
import requests
import datetime
import csv
import pandas as pd
from basemodule import PySparkManager


class RealtimeKmaWeather:
    _json_dict = {}
    _pdf = None

    def __init__(self, service_key, debug=False):
        self._base_url = 'http://newsky2.kma.go.kr/service/SecndSrtpdFrcstInfoService2/' \
                         'ForecastSpaceData'
        self._column = ['station', 'datehour', 'POP', 'PTY', 'R06', 'REH', 'S06', 'SKY',
                        'T3H', 'UUU', 'VEC', 'VVV', 'WSD']
        self._hdfs_spdf_path = 'hdfs:///weather/kma/weather.parquet'

        self._service_key = service_key
        self.debug = debug

    def _replace_last_basedt(self, ctime):
        h = ctime.hour

        if h < 2:
            ctime += datetime.timedelta(days=-1)
            ctime = ctime.replace(hour=23)
        else:
            ctime = ctime.replace(hour=(h - ((h + 1) % 3)))

        return ctime

    def _change_dt_strfmt(self, ctime):
        return ctime.strftime('%Y%m%d %H00')

    def _replace_malencoded_str(self, s):
        return

    def _get_localweather_coord(self, location='충청남도 천안시서북구 부성동'):  # 형식 : '시군구 시도 동면읍'
        top_url = 'http://www.kma.go.kr/DFSROOT/POINT/DATA/top'
        mdl_url = 'http://www.kma.go.kr/DFSROOT/POINT/DATA/mdl'
        leaf_url = 'http://www.kma.go.kr/DFSROOT/POINT/DATA/leaf'
        tail = '.json.txt'

        # top
        res1 = requests.get(top_url + tail)
        res1.encoding = 'utf-8'  # MUST DO IT!!!
        json_top = json.loads(res1.text)
        dict_top = {}
        for item in json_top:
            dict_top[item['value']] = item['code']

        # mdl
        res2 = requests.get(mdl_url + '.' + dict_top[location.split()[0]] + tail)
        res2.encoding = 'utf-8'  # MUST DO IT!!!
        json_mdl = json.loads(res2.text)
        dict_mdl = {}
        for item in json_mdl:
            dict_mdl[item['value']] = item['code']

        # leaf
        res3 = requests.get(leaf_url + '.' + dict_mdl[location.split()[1]] + tail)
        res3.encoding = 'utf-8'  # MUST DO IT!!!
        json_leaf = json.loads(res3.text)
        dict_leaf = {}
        for item in json_leaf:
            dict_leaf[item['value']] = [item['x'], item['y']]

        coord = dict_leaf[location.split()[2]]
        print(coord)
        return coord[0], coord[1]

    def req_api(self, base_dt, location='충청남도 천안시서북구 부성동'):
        sadt = base_dt.split(' ')

        nx, ny = self._get_localweather_coord(location)

        url = self._base_url + '?serviceKey=' + self._service_key \
                             + '&base_date=' + sadt[0] \
                             + '&base_time=' + sadt[1] \
                             + '&nx=' + nx \
                             + '&ny=' + ny \
                             + '&numOfRows=20&_type=json'

        self._json_dict = json.loads(requests.get(url).text, encoding='utf-8')
        if self.debug:
            print('url:', url)
            print('response:', self._json_dict)

    def _json2pdf(self, station='충청남도 천안시서북구 부성동'):
        wdata = self._json_dict['response']['body']['items']['item']
        obj_baseDt = self._replace_last_basedt(datetime.datetime.now())
        obj_fcstDt = obj_baseDt + datetime.timedelta(hours=4)

        baseDate = obj_baseDt.strftime('%Y%m%d')
        baseTime = obj_baseDt.strftime('%H00')
        fcstDate = obj_fcstDt.strftime('%Y%m%d')
        fcstTime = obj_fcstDt.strftime('%H00')

        # make dict for one measurement
        tmpdict = {}
        for col in self._column:
            tmpdict[col] = ''

        # fill dict using api measurement data
        for item in wdata:
            # get last weather data that matches base datetime
            if str(item['baseDate']) == baseDate \
                    and str(item['baseTime']) == baseTime \
                    and str(item['fcstDate']) == fcstDate \
                    and str(item['fcstTime']) == fcstTime:
                tmpdict[item['category']] = [item['fcstValue']]

        # make pdf
        tmpdict['station'] = station
        tmpdict['datehour'] = [obj_fcstDt.strftime('%Y-%m-%d %H')]
        self._pdf = pd.DataFrame(tmpdict)

        if self.debug:
            print(self._pdf)

    def _pdf2parquet(self, hdfs_path='', mode='append'):
        print('>>', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'api data:', list(self._pdf.iloc[0]))
        self._spdf = PySparkManager().sqlctxt.createDataFrame(self._pdf)  # make spark dataframe

        if hdfs_path == '':  # to default location
            self._spdf.write.mode(mode).parquet(self._hdfs_spdf_path)  # append new data to old parquet
        else:
            self._spdf.write.mode(mode).parquet(hdfs_path)  # append new data to old parquet

    def log(self, location='충청남도 천안시서북구 부성동', hdfs_path='', mode='append'):
        obj_basedt = self._replace_last_basedt(datetime.datetime.now())
        basedt = self._change_dt_strfmt(obj_basedt)
        self.req_api(basedt, location=location)
        self._json2pdf()
        self._pdf2parquet(hdfs_path=hdfs_path, mode=mode)

    def weather_daemon(self, outfilepath):
        toggle = True
        while True:
            ctime = datetime.datetime.now()
            if (ctime.hour + 1) % 3 == 0:
                if toggle:
                    self.log(outfilepath)
                    toggle = False
            else:
                toggle = True


if __name__ == '__main__':
    key = '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D'
    outfilepath = '/home/witlab/weather_logdata.csv'

    weather = RealtimeKmaWeather(key, True)
    # weather.log()
    weather._get_localweather_coord()
