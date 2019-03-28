from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyjake.nldc_basicmodule import PySparkManager
from pyjake.py24div import DivisionGetter

spark = PySparkManager().sqlctxt
dg = DivisionGetter(
    '8Op%2FMD5uSP4m2OZ8SYn43FH%2FRpEH8BBW7dnwU1zUqG%2BAuAnfH6oYADIASnGxh7P9%2BH8dzRFGxHl9vRY%2FFwSDvw%3D%3D')
dg.get_yearly(2017)
dg.get_yearly(2018)
dg.get_yearly(2019)
print(dg.get_division())
day = '2019-03-18'.replace('-', '')
print(day)
print(dg.what_is_the_division_of_this_day(day))

# udf_dnt2dt = udf(lambda d, t: d + ' ' + t)
# udf_datetime_rfmt = udf(lambda d: d.replace('-', ''))
# udf_24division = udf(lambda d: dg.what_is_the_division_of_this_day(d.replace('-', '').strip()), StringType())
# udf_dt2ym = udf(lambda d: d[:-3])
#
# df_uv = spark.read.parquet('hdfs:///nl/witlab/cas/uv_srs.parquet')
# df_uv.show()
#
# df_uv_r = df_uv.withColumn('datetime', udf_dnt2dt('date', 'time'))\
#                .withColumn('division', udf_24division('date'))\
#                .withColumn('ym', udf_dt2ym('date')).where('date > "2018-01-01"')
# df_uv_r.show()
#
# df_uv_r.createOrReplaceTempView('uv_division')
