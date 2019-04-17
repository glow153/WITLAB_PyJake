from abs_class import AbsCrawler
from bs4 import BeautifulSoup
import datetime


class SunAngleCrawler(AbsCrawler):
    def __init__(self, crawl_type='dynamic', debug=False):
        base_url = 'https://astro.kasi.re.kr/life/pageView/10'
        super().__init__(base_url, crawl_type, debug)

    def _make_url(self, **kwargs):
        return self._base_url + '?useElevation=1&output_range=1' \
                              + '&date=' + kwargs['date'] \
                              + '&address=' + kwargs['address']

    def _transform_angle(self, str_angle: str):
        return float(str_angle.split()[0] + '.' + str_angle.split()[1])

    def scrap(self, date, address):
        url = self._make_url(date=date, address=address)

        self._debug_print('request url: ' + url)

        self._driver.get(url)
        soup = BeautifulSoup(self._driver.page_source, 'lxml-xml')

        self._debug_print('response received. parsing...')

        table_list = []
        tr_list = soup.select('#sun-height-table > table > tbody')[0].find_all('tr')

        for html_row in tr_list:
            row = []
            for html_element in html_row.find_all('td'):
                # print(html_element.getText())
                if len(html_element.getText()) > 2:
                    element = self._transform_angle(html_element.getText())
                else:
                    element = date + ' ' + html_element.getText()
                row.append(element)

            solar_zenith = 90 - float(row[2])
            row.insert(3, solar_zenith)

            table_list.append(row)

        return table_list


if __name__ == '__main__':
    import pandas as pd

    sac = SunAngleCrawler(debug=True)

    obj_startdt = datetime.datetime.strptime('2019-11-06', '%Y-%m-%d')
    obj_enddt = datetime.datetime.strptime('2019-12-31', '%Y-%m-%d')

    while obj_startdt <= obj_enddt:
        table_list = sac.scrap(obj_startdt.strftime('%Y-%m-%d'), '충남+천안시+서북구+천안대로+1223-24')
        pdf = pd.DataFrame(table_list, columns=['datehour', 'azimuth', 'altitude', 'solar_zenith', 'right_asc', 'left_asc'])
        print(pdf)
        sac.to_db(pdf, username='root', passwd='defacto8*jj', host='210.102.142.14', port=3306,
                  db_name='nl_witlab', table_name='sun_angle')

        obj_startdt += datetime.timedelta(days=1)

    sac.close()  # must close!



