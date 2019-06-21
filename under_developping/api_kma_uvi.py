from abs_class import AbsApi

import requests
import datetime
import pandas as pd


class KmaUvi(AbsApi):
    def __init__(self):
        self.stnCodeList = {
            '강릉': '105',
            '서울': '108',
            '인천': '112',
            '울릉도': '115',
            '안면도': '132',
            '청주': '131',
            '대전': '133',
            '포항': '138',
            '대구': '143',
            '전주': '146',
            '울산': '152',
            '부산': '159',
            '광주': '156',
            '목포': '165',
            '고산': '013'
        }

        base_url = 'http://www.climate.go.kr/home/09_monitoring/index.php/UV/getDailyIndex'
        column = ['date', 'time', 'site_code', 'tuvi']
        hdfs_path = 'hdfs:///nl/kma/uvi/uvi_10min.parquet'

        super().__init__(base_url, '', column, hdfs_path, [], 'KmaUvi', True)

    def _make_query_param(self, **kwargs):
        pass

    def _make_payload(self, **kwargs):
        stn_code = kwargs['stn_code']
        date = kwargs['date']
        payload = {'stnCode': str(stn_code), 'dStr': date + '0300'}

        return payload

    def _json2pdf(self, **kwargs):
        self._pdf = pd.DataFrame(columns=('date', 'time', 'site_code', 'tuvi'))

        for item in self._json_dict['timeseries']:
            date = item['uvb_date'][:-4]
            date = date[:4] + '-' + date[4:6] + '-' + date[6:]
            time = item['uvb_date'][-4:][:2] + ':' + item['uvb_date'][-4:][2:]
            row_list = [date, time, item['site_code'], item['tuvi']]
            self._pdf.loc[len(self._pdf)] = row_list

    def log(self, db_type: list, mode='append', **kwargs):  # 우선 하루치 통째 로깅하는 것만 구현
        # station_list = []

        if kwargs['station'] == 'all':
            station_list = self.stnCodeList.values()
        else:
            try:
                station_list = [ self.stnCodeList[kwargs['station']] ]
            except KeyError:
                self._dbg.print_e('측정소 이름이 잘못되었습니다.')
                return
            except Exception as e:
                self._dbg.print_e('Exception occurred!! : ', e.__class__.__name__)
                return

        for _station_code in station_list:
            payload = self._make_payload(stn_code=_station_code, date=kwargs['date'])
            self._req_api(method='post', query_param='', payload=payload)
            self._json2pdf()
            if 'hdfs' in db_type:
                self.pdf2hdfs(mode=mode)
            if 'mysql' in db_type:
                self.pdf2mysql(table_name='kma_uvi', if_exists='append')

    def get_daily_uvi(self, strDate):
        stn_codes = kma_uvi.stnCodeList.values()
        for stn_code in stn_codes:
            payload = {'stnCode': str(stn_code), 'dStr': strDate + '0300'}
            self._dbg.print_p('request to', self._base_url, payload)
            response = requests.post(self._base_url, data=payload)
            self._json_dict = response.json()

            for item in self._json_dict['timeseries']:
                date = item['uvb_date'][:-4]
                time = item['uvb_date'][-4:]
                row_list = [date, time, item['site_code'], item['tuvi']]
                self._pdf.loc[len(self._pdf)] = row_list

    def getTotalUviData(self, start_date, end_date):
        sdt = datetime.datetime.strptime(start_date, '%Y-%m-%d')
        edt = datetime.datetime.strptime(end_date, '%Y-%m-%d')
        oneday = datetime.timedelta(days=1)

        dtcursor = sdt
        while dtcursor <= edt:
            self.get_daily_uvi(dtcursor.strftime('%Y%m%d'))
            dtcursor += oneday
    
    def to_csv(self, hdfs_outfilepath: str):
        self._pdf.to_csv(hdfs_outfilepath)

    def csv2parquet(self, local_infilepath, hdfs_outpath):
        from pyspark.sql import Row
        from basemodule import PySparkManager

        sc = PySparkManager().sc
        kma_uvi = sc.textFile(local_infilepath)\
                    .map(lambda s: s.split(','))\
                    .map(lambda s: Row(datetime='%s-%s-%s %s:%s' % (s[0][:4], s[0][4:6], s[0][6:],
                                                                    ('0' if len(s[1]) == 3 else '') + s[1][:len(s[1])-2],
                                                                    s[1][len(s[1])-2:]),
                                       time=s[1], stCode=s[2], uvi=s[3])).toDF()
        kma_uvi.write.mode('overwrite').parquet('hdfs://' + hdfs_outpath)


if __name__ == "__main__":
    kma_uvi = KmaUvi()

    dt_start = datetime.datetime.strptime('2018-01-01', '%Y-%m-%d')
    dt_end = datetime.datetime.strptime('2019-06-21', '%Y-%m-%d')
    oneday = datetime.timedelta(days=1)

    while dt_start <= dt_end:
        kma_uvi.log(['hdfs'], station='all', date=dt_start.strftime('%Y%m%d'))
        dt_start += oneday

