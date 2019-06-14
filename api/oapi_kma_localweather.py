import json
import requests
import datetime
import csv
import pandas as pd
from abs_class import AbsApi


class RealtimeKmaWeather(AbsApi):
    def __init__(self, service_key, tag, debug=False):
        base_url = 'http://newsky2.kma.go.kr/service/SecndSrtpdFrcstInfoService2/' \
                         'ForecastSpaceData'
        column = ['station', 'datehour', 'POP', 'PTY', 'R06', 'REH', 'S06', 'SKY',
                  'T3H', 'UUU', 'VEC', 'VVV', 'WSD']
        hdfs_path = 'hdfs:///weather/kma/weather.parquet'
        mysql_conn_param = []  # to be continued...

        super().__init__(base_url, service_key, column, hdfs_path,
                         mysql_conn_param, tag=tag, debug=debug)

    def get_last_basedt(self, obj_ctime):
        h = obj_ctime.hour
        m = obj_ctime.minute

        if h < 2:
            obj_ctime -= datetime.timedelta(days=1)
            obj_ctime = obj_ctime.replace(hour=23)
        else:
            obj_ctime = obj_ctime.replace(hour=(h - ((h + 1) % 3)))

        obj_ctime = obj_ctime.replace(minute=15)

        return obj_ctime

    def _get_localweather_coord(self, station='충청남도 천안시서북구 부성동'):  # 형식 : '시군구 시도 동면읍'
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
        res2 = requests.get(mdl_url + '.' + dict_top[station.split()[0]] + tail)
        res2.encoding = 'utf-8'  # MUST DO IT!!!
        json_mdl = json.loads(res2.text)
        dict_mdl = {}
        for item in json_mdl:
            dict_mdl[item['value']] = item['code']

        # leaf
        res3 = requests.get(leaf_url + '.' + dict_mdl[station.split()[1]] + tail)
        res3.encoding = 'utf-8'  # MUST DO IT!!!
        json_leaf = json.loads(res3.text)
        dict_leaf = {}
        for item in json_leaf:
            dict_leaf[item['value']] = [item['x'], item['y']]

        coord = dict_leaf[station.split()[2]]
        self._dbg.print_p('kma coord:', coord)
        return coord[0], coord[1]

    def _make_query_param(self, **kwargs):
        if 'station' in kwargs.keys():
            station = kwargs['station']
        else:
            station = '충청남도 천안시서북구 부성동'

        if 'base_dt' in kwargs.keys():
            sadt = kwargs['base_dt'].split(' ')
        else:
            # 현재 시간으로부터 가장 최근의 예보시각을 datetime 객체로 가져옴
            obj_basedt = self.get_last_basedt(datetime.datetime.now())
            # 객체를 API에 맞는 형식문자열로 변환
            sadt = obj_basedt.strftime('%Y%m%d %H%M').split(' ')

        # 주소 문자열을 토대로 기상청 동네예보 좌표를 구함
        nx, ny = self._get_localweather_coord(station)

        query_params = '?serviceKey=' + self._service_key \
                       + '&base_date=' + sadt[0] \
                       + '&base_time=' + sadt[1] \
                       + '&nx=' + nx \
                       + '&ny=' + ny \
                       + '&numOfRows=20&_type=json'

        return query_params

    def _json2pdf(self, station):
        """
        최근 1개 발표 데이터만 가져오게끔 구성됨
        api에 request 날릴때는 basetime을 15분에 맞춰서 날렸지만
        response로 받은 json의 basetime은 정각으로 표기됨.
        따라서 datetime string format에서 '분'을 '00'으로 해야함
        :param station:
        :return:
        """
        wdata = self._json_dict['response']['body']['items']['item']
        obj_baseDt = self.get_last_basedt(datetime.datetime.now() - datetime.timedelta(hours=3))
        obj_fcstDt = obj_baseDt + datetime.timedelta(hours=4)

        baseDate, baseTime = obj_baseDt.strftime('%Y%m%d %H00').split(' ')
        fcstDate, fcstTime = obj_fcstDt.strftime('%Y%m%d %H00').split(' ')

        self._dbg.print_e('json base time:', baseDate, baseTime, ', fcsttime', fcstDate, fcstTime)
        # make dict for one measurement
        tmpdict = {}
        for col in self._column:
            tmpdict[col] = ''

        # fill dict using api measurement data
        for item in wdata:
            self._dbg.print_e('item in wdata: ', item)
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

        # 190606 issue
        # R06, S06이 문자열과 실수값 모두 가지면서 dataframe이 읽히지 않는 문제
        # 해결방안 : 해당 행을 drop
        self._pdf = self._pdf.drop(['R06', 'S06'], axis=1)
        self._dbg.print_p('pdf =>', self._pdf.to_string)

    def log(self, db_type, mode='append', **kwargs):
        if 'station' in kwargs.keys():
            station = kwargs['station']
        else:
            station = '충청남도 천안시서북구 부성동'

        query_param = self._make_query_param(station=station)
        self._req_api(query_param)
        self._json2pdf(station)

        if 'hdfs' in db_type:
            self.pdf2hdfs(hdfs_path=self._hdfs_path, mode=mode)
        if 'mysql' in db_type:
            pass  # to be continued...


if __name__ == '__main__':
    key = '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D'

    weather = RealtimeKmaWeather(key, tag='RealtimeKmaWeather_API', debug=True)
    # weather.log(['hdfs'], mode='append', station='충청남도 천안시서북구 부성동')
    station = '충청남도 천안시서북구 부성동'
    query_param = weather._make_query_param(station=station)
    weather._req_api(query_param)
    weather._json2pdf(station)

    # normalize
    # weather.normalize_parquet()

