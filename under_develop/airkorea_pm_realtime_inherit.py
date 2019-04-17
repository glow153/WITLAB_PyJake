import datetime
import pandas as pd
from abs_class import AbsApi


class RealtimeParticulateMatter(AbsApi):
    _json_dict = {}
    _data_dict = {}
    _pdf = None
    _spdf = None

    def __init__(self, service_key, debug=False):
        self._station_list = ['성성동', '성거읍', '성황동', '백석동']
        base_url = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/' \
                   'getMsrstnAcctoRltmMesureDnsty'
        column_list = ['station', 'datehour', 'khaiGrade', 'khaiValue', 'no2', 'co', 'o3', 'so2', 'pm10', 'pm25']
        hdfs_spdf_path = 'hdfs:///pm/airkorea/pm_realtime.parquet'

        super().__init__(base_url=base_url,
                         service_key=service_key,
                         column_list=column_list,
                         hdfs_spdf_path=hdfs_spdf_path,
                         tag='ParticulateMatter',
                         debug=debug)

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

    def _make_query_param(self, **kwargs):
        querystring = '?ServiceKey=' + self._service_key

        if kwargs['location'] == 'nearest':
            querystring += '&stationName=성성동'
        else:
            querystring += '&stationName=' + kwargs['location']

        if kwargs['term'] == 'daily' or kwargs['term'] == 'hourly':
            querystring += '&dataTerm=DAILY&numOfRows=25'
        elif kwargs['term'] == 'month':
            querystring += '&dataTerm=MONTH&numOfRows=744'
        elif kwargs['term'] == '3month':
            querystring += '&dataTerm=3MONTH&numOfRows=2232'

        querystring += '&_returnType=json'

    def _json2pdf(self, term):
        param = self._json_dict['parm']
        data_list = self._json_dict['list']

        if super()._debug:
            print(param)
            print(data_list)
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

        if super()._debug:
            print('debug> get data from api:', rawdata)

        self._pdf = pd.DataFrame(rawdata)  # make pandas dataframe
        self._pdf.columns = self._column  # set new column name

        if term == 'hourly':
            self._pdf = self._pdf.sort_values(by=['datehour'], ascending=False).iloc[:1]

        # print(self._pdf)  # for debug

    def log(self, station='cheonan_all', term='daily', mode='append', debug=False):
        if station == 'cheonan_all':
            for _station in self._station_list:
                self._req_api(location=_station, term=term)
                self._json2pdf(term=term)
                self.pdf2hdfs(mode=mode)

        else:
            self._req_api(location=station, term=term)
            self._json2pdf(term=term)
            self.pdf2hdfs(mode='append', debug=debug)


if __name__ == '__main__':
    key = 'zo2rUB1wM3I11GNZFDuB84l4C94PZjP6cEb4qEff%2B94h83%2Fihaj1JJS75%2Bm0uHdFCchJw7SyGE0HZgKiZDpq%2FA%3D%3D'
    pm = RealtimeParticulateMatter(key, True)
    # pm._req_api(term='daily')
    # pm._json2pdf(term='daily')

    # normalize
    # pm.normalize_parquet()

    print(pm.get_last_log_datehour())
