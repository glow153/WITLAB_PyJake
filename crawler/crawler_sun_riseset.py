from abs_class import AbsCrawler
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from pyspark.sql import Row
from basemodule import PySparkManager


class SunRiseSetCrawler(AbsCrawler):
    def __init__(self, debug=False):
        base_url = 'https://astro.kasi.re.kr/life/pageView/9'
        super().__init__(base_url, crawl_type='dynamic', debug=debug)

    def _make_url(self, **kwargs):
        try:
            date = str(kwargs['date'])
        except KeyError:  # set today
            date = datetime.now().strftime('%Y-%m-%d')

        try:
            address = str(kwargs['address'])
        except KeyError:  # witlab
            address = '충남+천안시+서북구+천안대로+1223-24'

        return self._base_url + '?date=' + date + '&address=' + address

    def scrap(self, **kwargs):
        url = self._make_url(**kwargs)
        self._debug_print('request url :' + url)
        self._driver.get(url)
        soup = BeautifulSoup(self._driver.page_source, 'html.parser')

        sunrise = soup.find_all('span', {'class': 'sunrise'})[0].string
        culmination = soup.find_all('span', {'class': 'culmination'})[0].string
        sunset = soup.find_all('span', {'class': 'sunset'})[0].string

        sr = sunrise[0:2] + ':' + sunrise[4:-1]
        cul = culmination[0:2] + ':' + culmination[4:-1]
        ss = sunset[0:2] + ':' + sunset[4:-1]

        self._debug_print(str(kwargs['date']) + ':%s %s %s' % (sr, cul, ss))

        return [kwargs['date'], sr, cul, ss]

    def make_dataset_csv(self, local_outfilepath, start_date, end_date, address):
        import csv

        # set vals for scrap
        oneday = timedelta(days=1)
        sdt = datetime.strptime(start_date, '%Y-%m-%d')
        edt = datetime.strptime(end_date, '%Y-%m-%d')

        # set module
        outfile = open(local_outfilepath, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(outfile)

        while sdt <= edt:
            sunrslist = self.scrap(date=sdt.strftime('%Y-%m-%d'), address=address)
            csv_writer.writerow(sunrslist)
            ptstr = ''
            for s in sunrslist:
                ptstr += str(s) + ','
            print(ptstr[:-1])
            sdt = sdt + oneday

        outfile.close()

    def csv2parquet(self, local_infilepath: str, hdfs_outpath: str):
        pysparkmgr = PySparkManager()
        srs = pysparkmgr.sc.textFile(local_infilepath) \
            .map(lambda s: s.split(",")) \
            .map(lambda s: Row(datetime=s[0], rise=s[1], culmination=s[2], set=s[3])) \
            .toDF()

        srs.write.mode('overwrite').parquet(hdfs_outpath)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == '__main__':
    src = SunRiseSetCrawler(True)
    r = src.scrap(date='2019-05-03')
    print(r)
    src.close()


