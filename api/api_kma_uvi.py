from ..abs_class import AbsApi
from ..debugmodule import Log
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
        self.tag = 'KmaUvi'

        super().__init__(base_url, '', column, hdfs_path, [], self.tag)

    def _make_query_param(self, **kwargs):
        pass

    def _make_payload(self, **kwargs):
        stn_code = kwargs['stn_code']

        if kwargs['term'] == 'manual':
            date = kwargs['date'].replace('-', '')
        else:
            date = datetime.datetime.now().strftime('%Y%m%d')

        payload = {'stnCode': str(stn_code), 'dStr': date + '0300'}

        return payload

    def _json2pdf(self, **kwargs):
        self._pdf = pd.DataFrame(columns=('date', 'time', 'station_code', 'tuvi'))

        for item in self._json_dict['timeseries']:
            date = item['uvb_date'][:-4]
            date = date[:4] + '-' + date[4:6] + '-' + date[6:]
            time = item['uvb_date'][-4:][:2] + ':' + item['uvb_date'][-4:][2:]
            row_list = [date, time, item['site_code'], item['tuvi']]
            self._pdf.loc[len(self._pdf)] = row_list

            # self._pdf.sort_values(by=['date', 'time'], axis=0, ascending=False)

            if kwargs['term'] == '10min':
                dtobj = datetime.datetime.now()
                date = dtobj.strftime('%Y-%m-%d')
                minute = int(dtobj.minute / 10) * 10
                time = dtobj.replace(minute=minute).strftime('%H:%M')
                Log.e(self.tag, '_json2pdf(): select date time :', date, time)
                self._pdf = self._pdf.loc[(self._pdf['date'] == date) & (self._pdf['time'] == time)]

    def log(self, db_type: list, mode='append', **log_prop):
        # station_list = []
        try:
            if log_prop['station'] == 'all':
                station_list = self.stnCodeList.values()
            else:
                try:
                    station_list = [self.stnCodeList[log_prop['station']]]
                except KeyError:
                    Log.e(self.tag, '측정소 이름이 잘못되었습니다.')
                    return
                except Exception as e:
                    Log.e(self.tag, 'Exception occurred!! : ', e.__class__.__name__)
                    return

            for _station_code in station_list:
                payload = self._make_payload(stn_code=_station_code, **log_prop)
                self._req_api(method='post', query_param='', payload=payload)
                self._json2pdf(term=log_prop['term'])
                if 'hdfs' in db_type:
                    self.pdf2hdfs(mode=mode)
                if 'mysql' in db_type:
                    self.pdf2mysql(table_name='kma_uvi', if_exists='append')

        except KeyError:
            Log.e(self.tag, 'wrong log properties error! :', log_prop)

        # except Exception as e:
        #     self._dbg.print_e('exception occurred while logging data! :', e.__class__.__name__)


if __name__ == "__main__":
    from basemodule import PySparkManager
    spdf_uvi = PySparkManager().sqlctxt.read.parquet('hdfs:///nl/kma/uvi/uvi_10min.parquet')
    spdf_uvi = spdf_uvi.withColumnRenamed('site_code', 'station_code')
    spdf_uvi.write.mode('overwrite').parquet('hdfs:///nl/kma/uvi_10min.parquet')
    # kma_uvi = KmaUvi()
    # kma_uvi.normalize_parquet()

    # kma_uvi.log(['hdfs'], station='all', term='10min')

    # dt_start = datetime.datetime.strptime('2019-06-25', '%Y-%m-%d')
    # dt_end = datetime.datetime.strptime('2019-06-26', '%Y-%m-%d')
    # oneday = datetime.timedelta(days=1)
    #
    # while dt_start <= dt_end:
    #     # kma_uvi.log(['hdfs'], station='all', term='10min')
    #     kma_uvi.log(['hdfs'], station='all', term='manual', date=dt_start.strftime('%Y%m%d'))
    #     dt_start += oneday

