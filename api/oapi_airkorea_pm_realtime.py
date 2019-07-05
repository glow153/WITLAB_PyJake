import datetime
import pandas as pd
from ..basemodule import AbsApi
from ..debugmodule import Log


class RealtimeParticulateMatter(AbsApi):
    _data_dict = {}
    _pdf = None
    _spdf = None

    def __init__(self, service_key: str):
        """
        constructor.
        :param service_key: service key, :type: str
        :param mysql_conn_param: mysql connection arguments,
                                 ex. ('username', 'passwd', 'host', 'port', 'db_name')
        """
        self._station_list = ['성성동', '성거읍', '성황동', '백석동']

        base_url = 'http://openapi.airkorea.or.kr/openapi/services/rest/ArpltnInforInqireSvc/' \
                   'getMsrstnAcctoRltmMesureDnsty'
        column = ['station', 'datehour', 'khaiGrade', 'khaiValue', 'no2', 'co', 'o3', 'so2', 'pm10', 'pm25']
        hdfs_path = 'hdfs:///pm/airkorea/pm_realtime.parquet'
        mysql_conn_param = ['root', 'defacto8*jj', 'localhost', 3306, 'pm_measure']
        self.tag = 'RealTimePM'

        super().__init__(base_url, service_key, column, hdfs_path,
                         mysql_conn_param, tag=self.tag)

    def _datetime_corrector(self, org_dh: str):
        """
        필요성 : 미세먼지 api의 datetime 데이터 중 시간 값은 1~24시로 되어있음,
                이는 해당 1시간 동안 누적한 미세먼지의 양을 의미하나
                기존 사용해오던 datehour의 시간형식과 다르므로 맞춰줄 필요성이 있음
        :param org_dh: original datehour string (yyyy-mm-dd 01~24), :type: str
        :return: reformated datehour (yyyy-mm-dd 00~23), :type: str
        """

        sdate = org_dh.split()[0]
        stime = org_dh.split()[1].split(':')[0]
        dtdate = datetime.datetime.strptime(sdate, '%Y-%m-%d')
        if stime == '24':
            oneday = datetime.timedelta(days=1)
            dtdate += oneday
            sdate = dtdate.strftime('%Y-%m-%d')
            stime = '00'
        return sdate + ' ' + stime

    def _make_payload(self, **kwargs):
        pass

    def _make_query_param(self, **kwargs):
        query_params = '?ServiceKey=' + self._service_key

        if kwargs['station'] == 'nearest':
            query_params += '&stationName=성성동'
        else:
            query_params += '&stationName=' + kwargs['station']

        if kwargs['term'] == 'daily' or kwargs['term'] == 'hourly':
            query_params += '&dataTerm=DAILY&numOfRows=25'
        elif kwargs['term'] == 'month':
            query_params += '&dataTerm=MONTH&numOfRows=744'
        elif kwargs['term'] == '3month':
            query_params += '&dataTerm=3MONTH&numOfRows=2232'

        query_params += '&_returnType=json'

        return query_params

    def _json2pdf(self, **kwargs):
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
                   data['no2Value'], data['coValue'],
                   data['o3Value'], data['so2Value'],
                   data['pm10Value'], data['pm25Value']]
            rawdata.append(row)

        Log.d(self.tag, 'get data from api:', str(rawdata))

        # make pandas dataframe
        self._pdf = pd.DataFrame(rawdata)

        # set new column name
        self._pdf.columns = self._column

        if kwargs['term'] == 'hourly':  # hourly data
            self._pdf = self._pdf.sort_values(by=['datehour'], ascending=False).iloc[:1]

    def log(self, db_type: list, mode='append', **kwargs):
        # kwargs : {station='cheonan_all', term='daily'}

        if kwargs['station'] == 'cheonan_all':
            station_list = self._station_list
        else:
            station_list = kwargs['station']

        for _station in station_list:
            query_param = self._make_query_param(station=_station, term=kwargs['term'])
            self._req_api(method='get', query_param=query_param, payload=None)
            self._json2pdf(term=kwargs['term'])
            if 'hdfs' in db_type:
                self.pdf2hdfs(mode=mode)
            if 'mysql' in db_type:
                self.pdf2mysql(table_name='pm_realtime', if_exists='append')


if __name__ == '__main__':
    key = 'zo2rUB1wM3I11GNZFDuB84l4C94PZjP6cEb4qEff%2B94h83%2Fihaj1JJS75%2Bm0uHdFCchJw7SyGE0HZgKiZDpq%2FA%3D%3D'

    pm = RealtimeParticulateMatter(key)
    pm.log(['hdfs'], mode='append', station='cheonan_all', term='daily')

    # normalize
    pm.normalize_parquet()

