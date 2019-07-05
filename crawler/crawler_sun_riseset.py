from ..basemodule import AbsCrawler
from ..debugmodule import Log
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


class SunRiseSetCrawler(AbsCrawler):
    def __init__(self, debug=False):
        base_url = 'https://astro.kasi.re.kr/life/pageView/9'
        super().__init__(base_url, tag='SunRiseSetCrawler', crawl_type='dynamic', debug=debug)

    def print_usage(self):
        print('필수항목 : lat=(위도)&lon=(경도)&date=(날짜)')

    def _make_url(self, **kwargs):
        return self._base_url + '?' \
               + 'lat=' + kwargs['lat'] \
               + '&lon=' + kwargs['lon'] \
               + '&date=' + kwargs['date']

    def scrap(self, **kwargs):
        try:
            date = str(kwargs['date'])
        except KeyError:  # set today
            date = datetime.now().strftime('%Y-%m-%d')
            kwargs['date'] = date

        try:
            lat, lon = str(kwargs['lat']), str(kwargs['lon'])
        except KeyError:  # witlab
            lat, lon = '36.8523', '127.1510'
            kwargs['lat'] = lat
            kwargs['lon'] = lon

        url = self._make_url(**kwargs)
        Log.d(self.tag, 'request url :' + url)
        self._driver.get(url)
        soup = BeautifulSoup(self._driver.page_source, 'html.parser')

        sunrise = soup.find_all('span', {'class': 'sunrise'})[0].string
        culmination = soup.find_all('span', {'class': 'culmination'})[0].string
        sunset = soup.find_all('span', {'class': 'sunset'})[0].string

        sr = sunrise[0:2] + ':' + sunrise[4:-1]
        cul = culmination[0:2] + ':' + culmination[4:-1]
        ss = sunset[0:2] + ':' + sunset[4:-1]

        Log.d(self.tag, date, lat, lon, ': %s %s %s' % (sr, cul, ss))

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

    # def csv2parquet(self, local_infilepath: str, hdfs_outpath: str):
    #     pysparkmgr = PySparkManager()
    #     srs = pysparkmgr.sc.textFile(local_infilepath) \
    #         .map(lambda s: s.split(",")) \
    #         .map(lambda s: Row(datetime=s[0], rise=s[1], culmination=s[2], set=s[3])) \
    #         .toDF()
    #
    #     srs.write.mode('overwrite').parquet(hdfs_outpath)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == '__main__':
    src = SunRiseSetCrawler(True)
    r = src.scrap()
    print(r)
    src.close()


