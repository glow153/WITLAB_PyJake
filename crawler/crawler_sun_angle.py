from abs_class import AbsCrawler
from bs4 import BeautifulSoup
import datetime


class SunAngleCrawler(AbsCrawler):
    def __init__(self, debug=False):
        base_url = 'https://astro.kasi.re.kr/life/pageView/10'
        super().__init__(base_url, 'dynamic', debug)

    def _make_url(self, **kwargs):
        try:
            h = str(kwargs['hour'])
        except KeyError:
            h = ''

        try:
            m = str(kwargs['minute'])
        except KeyError:
            m = ''

        return self._base_url + '?useElevation=1&output_range=' \
                              + ('1' if kwargs['term'] == 'hourly' else '2') \
                              + '&date=' + kwargs['date'] \
                              + '&address=' + kwargs['address'] \
                              + '&hour=%s&minute=%s' % (h, m)

    def _transform_angle(self, str_angle: str):
        return int(str_angle.split()[0]) + int(str_angle.split()[1]) / 60 \
               + float(str_angle.split()[2]) / 3600

    def scrap(self, **kwargs):
        url = self._make_url(**kwargs)
        self._debug_print('request url: ' + url)
        self._driver.get(url)
        soup = BeautifulSoup(self._driver.page_source, 'lxml-xml')
        self._debug_print('response received. parsing...')
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
                                      hour=kwargs['hour'], minute=kwargs['minute'], second=0)
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
    obj_startdt = datetime.datetime.strptime('2019-11-06', '%Y-%m-%d')
    obj_enddt = datetime.datetime.strptime('2019-12-31', '%Y-%m-%d')

    while obj_startdt <= obj_enddt:
        table_list = sac.scrap(date=obj_startdt.strftime('%Y-%m-%d'),
                               address='충남+천안시+서북구+천안대로+1223-24',
                               term='hourly')
        pdf = pd.DataFrame(table_list,
                           columns=['datehour', 'azimuth', 'altitude', 'solar_zenith', 'right_asc', 'left_asc'])
        print(pdf)
        sac.to_db(pdf, username='root', passwd='defacto8*jj', host='210.102.142.14', port=3306,
                  db_name='nl_witlab', table_name='sun_angle')

        obj_startdt += datetime.timedelta(days=1)

    sac.close()  # must close!



