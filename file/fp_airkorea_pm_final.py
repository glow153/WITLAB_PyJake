import pandas as pd
import datetime
import os

from pyspark.sql.types import (StringType, IntegerType, FloatType)

from sparkmodule import PySparkManager
from ..debugmodule import Log


class FinalParticulateMatter:

    def __init__(self):
        self.col_list_src = ['location', 'station_code', 'station_name', 'datetime',
                             'so2', 'co', 'o3', 'no2', 'pm10', 'pm25', 'address']
        self.col_list = ['location', 'station_code', 'address', 'datetime',
                         'so2', 'co', 'o3', 'no2', 'pm10', 'pm25']
        self.type_list = [StringType, IntegerType, StringType, StringType,
                          FloatType, FloatType, FloatType, FloatType, FloatType, FloatType]
        self.sc = PySparkManager().sc
        self.sqlctxt = PySparkManager().sqlctxt
        self.tag = 'FinalPM'

    def search(self, dirname):
        # dirname 디렉토리 내의 모든 파일과 디렉토리 이름을 리스트로 반환함
        filelist = []
        filenames = os.listdir(dirname)
        for filename in filenames:
            full_filename = os.path.join(dirname, filename)
            filelist.append(full_filename)
        return filelist

    # 필요성 : 미세먼지 api의 datetime 데이터 중 시간 값은 1~24시로 되어있음,
    #         이는 해당 1시간 동안 누적한 미세먼지의 양을 의미하나 기존 사용해오던 datehour의 시간형식과 다르므로
    #         맞춰줄 필요성이 있음
    def _datetime_corrector(self, datetime_int):
        datetime_s = str(datetime_int)
        dtdate = datetime.datetime.strptime(datetime_s[:-2], '%Y%m%d')
        stime = datetime_s[8:]
        if stime == '24':
            oneday = datetime.timedelta(days=1)
            dtdate += oneday
            stime = '00'
        sdate = dtdate.strftime('%Y-%m-%d')
        return str(sdate + ' ' + stime)

    def xlsx2spdf(self, infilepath: str):
        data = pd.read_excel(infilepath, encoding='utf-8')  # read as pandas dataframe
        Log.d(self.tag + '.xlsx2spdf()', 'before changing column\n', data.iloc[:2])  # for debug

        if '망' in data.columns:  # if exists column name '망'
            data = data.drop(['망'], axis=1)  # drop it
            Log.d(self.tag, 'dropped column "망"\n', data.iloc[:2])  # for debug

        data.columns = self.col_list_src  # change column name
        Log.d(self.tag, 'after changing column\n', data.iloc[:2])  # for debug

        # correct datetime
        data['datetime'] = data['datetime'].apply(self._datetime_corrector)

        df = self.sqlctxt.createDataFrame(data)
        return df

    def xlsxdir2parquet(self, dirpath: str, hdfs_outpath: str):
        from pyspark.sql.functions import udf
        from pyspark.sql.types import StringType
        udf_mergeCol = udf(lambda s, t: s + ' ' + t, StringType())

        infilelist = self.search(dirpath)

        # 디렉토리 내에 확정데이터 파일 하나씩 읽어서 merged로 통합시키기
        Log.d(self.tag + '.xlsxdir2parquet()', 'target file name:', infilelist[0])
        # read xlsx and make spdf
        merged = self.xlsx2spdf(infilelist[0])
        # concatenate two columns
        merged = merged.withColumn('location', udf_mergeCol('location', 'station_name'))
        merged = merged.drop('station_name')
        Log.d(self.tag + '.xlsxdir2parquet()', 'target file converted to spdf')
        merged.show()

        for i in range(1, len(infilelist)):
            Log.d(self.tag + '.xlsxdir2parquet()', 'target file name:', infilelist[i])

            # read xlsx and make spdf
            spdf = self.xlsx2spdf(infilelist[i])

            # concatenate two columns
            spdf = spdf.withColumn('location', udf_mergeCol('location', 'station_name'))
            spdf = spdf.drop('station_name')

            # merge spdf
            merged = merged.union(spdf)
            Log.d(self.tag + '.xlsxdir2parquet()', 'target file converted to spdf')
            merged.show()

        merged.show()
        merged.write.mode('overwrite').parquet(hdfs_outpath)
        Log.d(self.tag + '.xlsxdir2parquet()', 'parquet write completed.')


if __name__ == '__main__':
    fpm = FinalParticulateMatter()
    fpm.xlsxdir2parquet('/home/witlab/pm', 'hdfs:///pm/airkorea/pm_final.parquet')

