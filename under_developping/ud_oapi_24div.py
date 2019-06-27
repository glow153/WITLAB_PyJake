import requests
from datetime import datetime, timedelta
from bs4 import BeautifulSoup


class Division:
    _url = 'http://apis.data.go.kr/B090041/openapi/service/SpcdeInfoService/get24DivisionsInfo'
    _service_key = ''
    _division = {}
    _div_range = None

    def __init__(self, service_key):
        self._service_key = service_key

    # 월별 절기 dictionary로 만듬
    def get_monthly(self, year: int, month: int):
        # month string formatting
        strMonth = str(month) if len(str(month)) == 2 else '0' + str(month)
        # make query parameter
        query_param = '?solYear=' + str(year) + '&solMonth=' + strMonth + '&ServiceKey=' + self._service_key

        # send get method
        doc = requests.get(self._url + query_param)
        # parse xml
        soup = BeautifulSoup(doc.text, 'html.parser')

        # find element
        for item in soup.findAll('item'):
            datename = item.datename.text
            locdate = item.locdate.text
            self._division[locdate] = datename

    # 연도별 절기 dictionary로 만듬
    def get_yearly(self, year: int):
        for i in range(1, 13):
            self.get_monthly(year, i)

    def get_division(self):
        return self._division

    def get_range(self, div):
        pass

    def what_is_the_division_of_this_day(self, date):
        oneday = timedelta(days=1)
        objdt = datetime.strptime(date, '%Y%m%d')
        divday_list = list(self._division.keys())
        divday_list.sort()

        for i in range(len(divday_list) - 2):
            objdt_left = datetime.strptime(divday_list[i], '%Y%m%d')
            objdt_right = datetime.strptime(divday_list[i+1], '%Y%m%d')

            # # debug
            # print(date, ':',
            #       divday_list[i], '(', self.division[divday_list[i]], ') ~',
            #       divday_list[i+1], '(', self.division[divday_list[i+1]], ')')

            if divday_list[i] == date:
                return self._division[divday_list[i]]
            elif divday_list[i] < date < divday_list[i+1]:
                td = objdt_right - objdt_left
                # # debug
                # print('objdt_right - objdt_left =', td.days)
                # print('thr=', (objdt_left + timedelta(days=int(td.days/2))).strftime('%Y%m%d'))
                if date < (objdt_left + timedelta(days=int(td.days/2))).strftime('%Y%m%d'):
                    return self._division[divday_list[i]]
                else:
                    return self._division[divday_list[i + 1]]

            objdt += oneday

        return 'no data'


if __name__ == '__main__':
    from sparkmodule import PySparkManager
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    spark = PySparkManager().sqlctxt
    key = '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D'
    dg = Division(key)
    dg.get_yearly(2017)
    dg.get_yearly(2018)
    dg.get_yearly(2019)

    day = '2019-03-18'.replace('-', '')
    print(day)
    print(dg.what_is_the_division_of_this_day(day))

    udf_dnt2dt = udf(lambda d, t: d + ' ' + t)
    udf_datetime_rfmt = udf(lambda d: d.replace('-', ''))
    udf_24division = udf(lambda d: dg.what_is_the_division_of_this_day(d.replace('-', '').strip()), StringType())
    udf_dt2ym = udf(lambda d: d[:-3])

    df_uv = spark.read.parquet('hdfs:///nl/witlab/cas/uv_srs.parquet')
    df_uv.show()

    df_uv_r = df_uv.withColumn('datetime', udf_dnt2dt('date', 'time')) \
        .withColumn('division', udf_24division('date')) \
        .withColumn('ym', udf_dt2ym('date')).where('date > "2018-01-01"')
    df_uv_r.show()

    df_uv_r.createOrReplaceTempView('uv_division')



