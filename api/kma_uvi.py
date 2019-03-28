import requests
import datetime

import pandas as pd


class KmaUvi:
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

        self.base_url = 'http://www.climate.go.kr/home/09_monitoring/index.php/UV/getDailyIndex'
        self.df = pd.DataFrame(columns=('date', 'time', 'site_code', 'tuvi'))

    def getDailyUvi(self, stnCode, strDatetime):
        payload = {'stnCode': str(stnCode), 'dStr': strDatetime}
        print('request to', self.base_url, payload)
        response = requests.post(self.base_url, data=payload)
        uvi_data = response.json()

        for item in uvi_data['timeseries']:
            date = item['uvb_date'][:-4]
            time = item['uvb_date'][-4:]
            row_list = [date, time, item['site_code'], item['tuvi']]
            self.df.loc[len(self.df)] = row_list

        # print(self.df)

    def getTotalUviData(self, start_date, end_date):
        sdt = datetime.datetime.strptime(start_date + ' 03:00', '%Y-%m-%d %H:%M')
        edt = datetime.datetime.strptime(end_date + ' 22:00', '%Y-%m-%d %H:%M')
        oneday = datetime.timedelta(days=1)

        print(self.stnCodeList.keys())

        for stn_name in self.stnCodeList.keys():
            dtcursor = sdt
            while dtcursor <= edt:
                self.getDailyUvi(self.stnCodeList[stn_name], dtcursor.strftime('%Y%m%d%H%M'))
                dtcursor += oneday
    
    def to_csv(self, hdfs_outfilepath: str):
        self.df.to_csv(hdfs_outfilepath)

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
