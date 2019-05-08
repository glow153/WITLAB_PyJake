from pyspark.sql import Row
from basemodule import *

locationcode = 132


# 현재 안면도 파일만 넣어야 작동함

class KmaUv:
    sc = None

    def __init__(self):
        self.sc = PySparkManager().sc

    def read_raw(self, filepath, outpath):
        uva = self.sc.textFile(filepath) \
                  .filter(lambda s: s != 'Site: Anmyeon') \
                  .map(lambda s: s.split(',')) \
                  .filter(lambda s: is_number(s[1].strip())) \
                  .map(lambda s: [s[0][:4] + '-' + s[0][4:6] + '-' + s[0][6:8],
                                  s[0][8:],
                                  float(s[1].strip()),
                                  float(s[2].strip())]) \
                  .map(lambda s: Row(datehour=s[0] + ' ' + s[1], sum_uva=s[2], max_uva=s[3])) \
                  .toDF()

        uva.write.mode("overwrite").parquet(outpath)



