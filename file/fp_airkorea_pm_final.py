import pandas as pd
import datetime
import os

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, FloatType)
from basemodule import PySparkManager
from debug_module import Log


class FinalParticulateMatter:

    def __init__(self):
        self.col_list = ['location', 'station_code', 'station_name', 'datetime',
                         'so2', 'co', 'o3', 'no2', 'pm10', 'pm25', 'address']
        self.type_list = [StringType, IntegerType, StringType, StringType,
                          FloatType, FloatType, FloatType, FloatType,
                          FloatType, FloatType, StringType]
        self.sc = PySparkManager().sc
        self.sqlctxt = PySparkManager().sqlctxt

    def search(self, dirname):
        # dirname 디렉토리 내의 모든 파일과 디렉토리 이름을 리스트로 반환함
        filelist = []
        filenames = os.listdir(dirname)
        for filename in filenames:
            full_filename = os.path.join(dirname, filename)
            filelist.append(full_filename)
        return filelist

    def make_empty_df(self):
        field_list = []
        for i in range(len(self.col_list)):
            field = StructField(self.col_list[i], self.type_list[i](), True)
            field_list.append(field)
        schema = StructType(field_list)
        return self.sqlctxt.createDataFrame(self.sc.emptyRDD(), schema)

    # 필요성 : 미세먼지 api의 datetime 데이터 중 시간 값은 1~24시로 되어있음,
    #         이는 해당 1시간 동안 누적한 미세먼지의 양을 의미하나 기존 사용해오던 datehour의 시간형식과 다르므로
    #         맞춰줄 필요성이 있음
    def _datetime_corrector(self, datetime_int):
        datetime_int = str(datetime_int)
        sdate = datetime_int[:-2]
        stime = datetime_int[8:]
        dtdate = datetime.datetime.strptime(sdate, '%Y%m%d')
        if stime == '24':
            oneday = datetime.timedelta(days=1)
            dtdate += oneday
            sdate = dtdate.strftime('%Y%m%d')
            stime = '00'
        return sdate + stime

    def xlsx2spdf(self, infilepath: str):
        data = pd.read_excel(infilepath, encoding='utf-8')  # read as pandas dataframe
        print(data.iloc[:10])  # for debug
        if '망' in data.columns:  # if exists column name '망'
            data = data.drop(['망'], axis=1)  # drop it
            print(data.iloc[:10])  # for debug
        data.columns = self.col_list  # change column name

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
        Log.d('xlsxdir2parquet()', 'target file name:', infilelist[0])
        merged = self.xlsx2spdf(infilelist[0])
        for i in range(1, len(infilelist)):
            Log.d('xlsxdir2parquet()', 'target file name:', infilelist[i])

            # read xlsx and make spdf
            spdf = self.xlsx2spdf(infilelist[i])

            # concatenate two columns
            spdf = spdf.withColumn('location', udf_mergeCol('location', 'station_name'))
            spdf = spdf.drop('station_name')

            # merge spdf
            merged = merged.union(spdf)
            print(i, ':', merged.count())

        merged.show()
        merged.write.mode('overwrite').parquet(hdfs_outpath)


if __name__ == '__main__':
    fpm = FinalParticulateMatter()
    fpm.xlsxdir2parquet('/home/witlab/pm', 'hdfs:///pm/airkorea/pm_final.parquet')

