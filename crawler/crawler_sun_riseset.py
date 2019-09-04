from basemodule import AbsCrawler
from debugmodule import Log
from bs4 import BeautifulSoup
from datetime import datetime, timedelta


class SunRiseSetCrawler(AbsCrawler):
    def __init__(self, debug=False):
        base_url = 'https://astro.kasi.re.kr/life/pageView/9'
        super().__init__(base_url, tag='SunRiseSetCrawler', crawl_type='dynamic', debug=debug)

    def print_usage(self):
        """
        사이트 개편 이후 주소 인자 필요 없어지고 위경도가 필수항목으로 들어가게 됨
        :return:
        """
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
            lat = '36.850490744236744'
            lon = '127.15250390636234'

        kwargs['lat'] = lat
        kwargs['lon'] = lon

        url = self._make_url(**kwargs)
        Log.d(self.tag, 'request url :' + url)
        self._driver.get(url)

        soup = BeautifulSoup(self._driver.page_source, 'html.parser')

        sunrise = soup.find_all('span', {'class': 'sunrise'})[0].string
        culmination = soup.find_all('span', {'class': 'culmination'})[0].string
        sunset = soup.find_all('span', {'class': 'sunset'})[0].string

        Log.d(self.tag, 'result:', sunrise, culmination, sunset)

        sr = sunrise[0:2] + ':' + sunrise[4:-1]
        cul = culmination[0:2] + ':' + culmination[4:-1]
        ss = sunset[0:2] + ':' + sunset[4:-1]

        Log.d(self.tag, date, lat, lon, ': %s %s %s' % (sr, cul, ss))
        return [kwargs['date'], sr, cul, ss]

    def make_dataset_csv(self, local_outfilepath, start_date, end_date):
        import csv

        # set vals for scrap
        oneday = timedelta(days=1)
        sdt = datetime.strptime(start_date, '%Y-%m-%d')
        edt = datetime.strptime(end_date, '%Y-%m-%d')

        # set module
        outfile = open(local_outfilepath, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(outfile)

        while sdt <= edt:
            sunrslist = self.scrap(date=sdt.strftime('%Y-%m-%d'))
            csv_writer.writerow(sunrslist)
            ptstr = ''
            for s in sunrslist:
                ptstr += str(s) + ','
            print(ptstr[:-1])
            sdt = sdt + oneday

        outfile.close()

    def dataset_to_db(self, start_date, end_date):
        import pandas as pd
        # set vals for scrap
        oneday = timedelta(days=1)
        sdt = datetime.strptime(start_date, '%Y-%m-%d')
        edt = datetime.strptime(end_date, '%Y-%m-%d')

        while sdt <= edt:
            sunrslist = self.scrap(date=sdt.strftime('%Y-%m-%d'))
            pdf = pd.DataFrame([sunrslist], columns=['date', 'rise', 'culmination', 'set'])
            Log.d(self.tag, '\n', pdf)
            self.to_db(pdf, username='root', passwd='defacto8*jj', host='210.102.142.14', port=3306,
                       db_name='nl_witlab', table_name='kasi_sun_riseset')
            sdt = sdt + oneday

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


if __name__ == '__main__':
    src = SunRiseSetCrawler(True)
    src.dataset_to_db('2017-01-01', '2020-12-31')
    src.close()


