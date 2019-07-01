from pyspark.sql.functions import udf
from pyspark.sql.types import (IntegerType, StringType, DoubleType)
from basemodule import Singleton


class PySparkManager(Singleton):
    sc = None

    def __init__(self, ip='210.102.142.14', app_name='appName', master='local[*]'):
        self.server_ip = ip
        self.sc = self._set_spark_context(app_name, master)
        self.sqlctxt = self._set_sql_context()

    def _set_spark_context(self, app_name, master):
        from pyspark import (SparkConf, SparkContext)
        if self.sc:
            return self.sc

        conf = SparkConf().setAppName(app_name) \
            .setMaster(master) \
            .set('spark.local.ip', self.server_ip) \
            .set('spark.driver.host', self.server_ip)
        return SparkContext(conf=conf)

    def _set_sql_context(self):
        from pyspark.sql import SQLContext
        return SQLContext(self.sc)

    def close(self):
        self.sc.stop()

    def __enter__(self):
        self.__init__()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


clean_day_list = ['2017-04-13', '2017-04-19', '2017-04-24', '2017-05-01', '2017-05-17',
                  '2017-06-02', '2017-06-15', '2017-08-26', '2017-09-01', '2017-09-13',
                  '2017-09-14', '2017-09-21', '2017-09-28', '2017-10-21', '2017-10-30',
                  '2017-10-31', '2017-11-11', '2017-11-16', '2017-12-13', '2017-12-27',
                  '2018-01-06', '2018-02-17', '2018-03-31', '2018-04-18', '2018-04-19',
                  '2018-04-28', '2018-05-04', '2018-05-24', '2018-05-26', '2018-06-01',
                  '2018-06-06', '2018-06-16', '2018-06-24', '2018-07-19', '2018-07-21',
                  '2018-09-08', '2018-09-29', '2018-10-03', '2018-10-31', '2018-11-02',
                  '2018-11-03', '2018-11-30', '2018-12-12', '2018-12-14', '2018-12-23',
                  '2018-12-27', '2018-12-28', '2019-01-03', '2019-01-20', '2019-01-26',
                  '2019-01-29', '2019-02-21', '2019-03-01', '2019-03-08']


def weather(month):
    if 3 <= month <= 5:
        return 0
    elif 6 <= month <= 8:
        return 1
    elif 9 <= month <= 11:
        return 2
    elif month <= 12 or 1 <= month <= 2:
        return 3
    else:
        return -1


def floatize(dec):
    try:
        return float(dec)
    except ValueError:
        return None


def intize(dec):
    try:
        return int(dec)
    except ValueError:
        return None


def classifyPm10(pm):
    if pm <= 30:
        return 1
    elif 30 < pm <= 80:
        return 2
    elif 80 < pm <= 150:
        return 3
    else:
        return 4


def classifyPm25(pm):
    if pm <= 15:
        return 1
    elif 15 < pm <= 35:
        return 2
    elif 35 < pm <= 75:
        return 3
    else:
        return 4


def is_number(n):
    try:
        float(n)
        return True
    except TypeError:
        return False


udf_year = udf(lambda date: int(date.split('-')[0]), IntegerType())
udf_month = udf(lambda date: int(date.split('-')[1]), IntegerType())
udf_day = udf(lambda date: int(date.split('-')[2]), IntegerType())
udf_hour = udf(lambda time: int(time.split(':')[0]), IntegerType())
udf_minute = udf(lambda time: int(time.split(':')[1]), IntegerType())

udf_dt2dh = udf(lambda d, t: d + ' ' + t.split(':')[0], StringType())
udf_dnt2dt = udf(lambda d, t: d + ' ' + t, StringType())
udf_dt2d = udf(lambda dt: dt.split(' ')[0], StringType())
udf_dt2t = udf(lambda dt: dt.split(' ')[1], StringType())
udf_dh2h = udf(lambda dh: int(dh.split(' ')[1]), IntegerType())
udf_ymd2ym = udf(lambda ymd: ymd[:-3], StringType())
udf_date2month = udf(lambda d: d.split('-')[1], StringType())
udf_joule = udf(lambda f: f * 60, DoubleType())

udf_cleanday = udf(lambda date: 1 if date in clean_day_list else 0, IntegerType())

udf_weather = udf(weather, IntegerType())
udf_classifyPm10 = udf(classifyPm10, IntegerType())
udf_classifyPm25 = udf(classifyPm25, IntegerType())
udf_intize = udf(intize, IntegerType())
udf_floatize = udf(floatize, DoubleType())
udf_string = udf(lambda s: str(s), StringType())

udf_30min = udf(lambda t: t.split(':')[0] + ':' + ('00' if int(int(t.split(':')[1]) / 30) == 0 else '30'))
udf_10min = udf(lambda t: t[:3] + str(int(int(t[3:])/10)) + '0', StringType())

udf_sum = udf(lambda *v: sum(v), DoubleType())
udf_avg = udf(lambda *v: sum(v) / len(v), DoubleType())
udf_max = udf(lambda *v: max(v), DoubleType())
udf_min = udf(lambda *v: min(v), DoubleType())


