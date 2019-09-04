from basemodule import AbsCrawler
from debugmodule import Log
from bs4 import BeautifulSoup
import datetime


class SunAngleCrawler(AbsCrawler):
    def __init__(self, debug=False):
        base_url = 'https://astro.kasi.re.kr/life/pageView/10'
        super().__init__(base_url, tag='SunAngleCrawler', crawl_type='dynamic', debug=debug)

    def _make_url(self, **kwargs):
        try:
            h = str(kwargs['hour'])
        except KeyError:
            h = ''

        try:
            m = str(kwargs['minute'])
        except KeyError:
            m = ''

        try:
            s = str(kwargs['second'])
        except KeyError:
            s = ''

        try:
            lat, lon = str(kwargs['lat']), str(kwargs['lon'])
        except KeyError:
            lat, lon = '36.850490744236744', '127.15250390636234'

        try:
            elevation = str(kwargs['elevation'])
        except KeyError:
            elevation = '-106.09210616048128'

        return self._base_url + '?useElevation=1' \
                              + '&elevation=%s ' % elevation \
                              + '&output_range=' \
                              + ('1' if kwargs['term'] == 'hourly' else '2') \
                              + '&lat=%s&lng=%s' % (lat, lon) \
                              + '&date=' + kwargs['date'] \
                              + '&hour=%s&minute=%s&second=%s' % (h, m, s) \
                              + '&address=' + kwargs['address']

    def _transform_angle(self, str_angle: str):
        return int(str_angle.split()[0]) + int(str_angle.split()[1]) / 60 \
               + float(str_angle.split()[2]) / 3600

    def scrap(self, **kwargs):
        url = self._make_url(**kwargs)
        Log.d(self.tag, 'request url: ' + url)

        try:
            self._driver.get(url)
        except Exception as e:
            Log.e(self.tag, 'chromedriver get error!:', e.__class__.__name__)
            return

        soup = BeautifulSoup(self._driver.page_source, 'lxml-xml')
        Log.d(self.tag, 'response received. parsing...')
        table_list = []

        if kwargs['term'] == 'hourly':
            tr_list = soup.select('#sun-height-table > table > tbody')[0].find_all('tr')

            for html_row in tr_list:
                row = []
                for html_element in html_row.find_all('td'):
                    # print(html_element.getText())
                    if len(html_element.getText()) > 2:
                        element = self._transform_angle(html_element.getText())
                    else:
                        element = kwargs['date'] + ' ' + html_element.getText()
                    row.append(element)

                solar_zenith = 90 - float(row[2])
                row.insert(3, solar_zenith)
                table_list.append(row)

        elif kwargs['term'] == 'minutely':
            objdt = datetime.datetime(year=int(kwargs['date'].split('-')[0]),
                                      month=int(kwargs['date'].split('-')[1]),
                                      day=int(kwargs['date'].split('-')[2]),
                                      hour=kwargs['hour'], minute=kwargs['minute'],
                                      second=0)
            html_row = soup.select('#sun-height-table > table > tbody')[0].find('tr')
            row = [objdt.strftime('%Y-%m-%d %H:%M')]
            for html_element in html_row.find_all('td'):
                row.append(self._transform_angle(html_element.getText()))
            solar_zenith = 90 - float(row[2])
            row.insert(3, solar_zenith)
            table_list.append(row)

        return table_list


if __name__ == '__main__':
    import pandas as pd

    sac = SunAngleCrawler(debug=True)

    # this code works with hourly term only!!!!
    obj_startdt = datetime.datetime.strptime('2020-06-02 14:19', '%Y-%m-%d %H:%M')
    obj_enddt = datetime.datetime.strptime('2020-12-31 23:59', '%Y-%m-%d %H:%M')

    while obj_startdt <= obj_enddt:
        table_list = sac.scrap(date=obj_startdt.strftime('%Y-%m-%d'),
                               address='충남+천안시+서북구+천안대로+1223-24',
                               hour=obj_startdt.hour, minute=obj_startdt.minute, second=0,
                               term='minutely')
        pdf = pd.DataFrame(table_list,
                           columns=['datetime', 'azimuth', 'altitude',
                                    'solar_zenith', 'right_asc', 'left_asc'])

        Log.d(sac.tag, '\n', pdf)

        sac.to_db(pdf, username='root', passwd='defacto8*jj', host='210.102.142.14', port=3306,
                  db_name='nl_witlab', table_name='kasi_sun_angle')

        obj_startdt += datetime.timedelta(minutes=1)

    sac.close()  # must close!



