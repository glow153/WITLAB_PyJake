from pyspark.sql import Row
from basemodule import PySparkManager

clean_days = ['2017-04-13', '2017-04-19', '2017-04-24', '2017-05-01', '2017-05-17', '2017-06-02', '2017-06-15', '2017-08-26', '2017-09-01', '2017-09-13', '2017-09-14', '2017-09-21', '2017-09-28', '2017-10-21', '2017-10-30', '2017-10-31', '2017-11-11', '2017-11-16', '2017-12-13', '2017-12-27', '2018-01-06', '2018-02-17', '2018-03-31', '2018-04-18', '2018-04-19', '2018-04-28', '2018-05-04', '2018-05-24', '2018-05-26', '2018-06-01', '2018-06-06', '2018-06-16', '2018-06-24', '2018-07-19', '2018-07-21', '2018-09-08', '2018-09-29', '2018-10-03', '2018-10-31', '2018-11-02', '2018-11-03', '2018-11-30', '2018-12-12', '2018-12-14', '2018-12-23', '2018-12-27', '2018-12-28', '2019-01-03', '2019-01-20', '2019-01-26', '2019-01-29', '2019-02-21', '2019-03-01', '2019-03-08']

sc = PySparkManager().sc
df_clday = sc.parallelize(clean_days)\
             .map(lambda s: Row(date=s))\
             .toDF()
df_clday.show()
df_clday.write.mode('overwrite').parquet('hdfs:///dayday/clean_days.parquet')
