from bs4 import BeautifulSoup
from selenium import webdriver


class ScrapSunRiseSet:
    baseUrl = 'https://astro.kasi.re.kr/life/pageView/9'
    url = ''
    options = None
    driver = None
    debug = False

    def __init__(self, debug):
        self.debug = debug
        if self.debug:
            print('dbg> init driver...')
        chrome_driver_path = './drivers/chromedriver'
        self.options = webdriver.ChromeOptions()
        self.options.add_argument('headless')
        self.options.add_argument('disable-gpu')
        self.driver = webdriver.Chrome(chrome_driver_path, options=self.options)
        if self.debug:
            print('dbg> complete!')

    def scrap(self, date, address):
        self.url = self.baseUrl + '?date=' + date + '&address=' + address
        if self.debug:
            print('dbg> request url :', self.url)
        self.driver.get(self.url)
        soup = BeautifulSoup(self.driver.page_source, 'html.parser')

        sunrise = soup.find_all('span', {'class': 'sunrise'})[0].string
        culmination = soup.find_all('span', {'class': 'culmination'})[0].string
        sunset = soup.find_all('span', {'class': 'sunset'})[0].string

        sr = sunrise[0:2] + ':' + sunrise[4:-1]
        cul = culmination[0:2] + ':' + culmination[4:-1]
        ss = sunset[0:2] + ':' + sunset[4:-1]

        if self.debug:
            print('dbg>', date, sr, cul, ss)

        return [date, sr, cul, ss]
        
    def make_dataset_csv(self, local_outfilepath, start_date, end_date, address):
        import csv
        from datetime import datetime, timedelta

        # set vals for scrap
        oneday = timedelta(days=1)
        sdt = datetime.strptime(start_date, '%Y-%m-%d')
        edt = datetime.strptime(end_date, '%Y-%m-%d')
        
        # set module
        outfile = open(local_outfilepath, 'w', encoding='utf-8', newline='')
        csv_writer = csv.writer(outfile)

        while sdt <= edt:
            sunrslist = self.scrap(sdt.strftime('%Y-%m-%d'), address)
            csv_writer.writerow(sunrslist)
            ptstr = ''
            for s in sunrslist:
                ptstr += str(s) + ','
            print(ptstr[:-1])
            sdt = sdt + oneday

        outfile.close()

    def csv2parquet(self, local_infilepath: str, hdfs_outpath: str):
        from pyspark.sql import Row
        from basemodule import PySparkManager

        pysparkmgr = PySparkManager()
        srs = pysparkmgr.sc.textFile(local_infilepath)\
                        .map(lambda s: s.split(","))\
                        .map(lambda s: Row(datetime=s[0],
                                           rise=s[1], culmination=s[2], set=s[3]))\
                        .toDF()

        srs.write.mode('overwrite').parquet(hdfs_outpath)
        
    def close(self):
        if self.debug:
            print('dbg> close!')
        self.driver.close()

