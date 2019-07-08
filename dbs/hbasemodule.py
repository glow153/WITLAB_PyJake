import happybase
import pprint
from entries.cas_entry import CasEntry
from ..basemodule import Singleton


class HappybaseManager(Singleton):
    _server_addr = ''
    _tablename = ''
    _hb_conn = None
    _hb_table_conn = None

    def __init__(self, addr: str, tablename: str):
        self._server_addr = addr
        self._tablename = tablename

    def connect(self):  # HBase와 연결, connection은 객체변수로 유지
        self._hb_conn = happybase.Connection(self._server_addr)
        self._hb_table_conn = self._hb_conn.table(self._tablename)

    def disconnect(self):
        self._hb_conn.close()

    def _getmerge_nle_hbrow(self, datedir):  # insert를 위한 메소드, isd파일 들어있는 폴더를 HBase row 형태로 변환
        flist = CasEntry.search(datedir)
        dict_merged_sp = {}

        # make list and dict
        for fname in flist:

            # get nl entity obj
            nle = CasEntry(fname)

            try:
                datetime = nle.get_datetime(tostr=True)[:-3].replace(':', '')
            except TypeError or AttributeError:
                continue
            print(datetime)

            # get dict from nl entity
            mc = nle.get_category('measurement conditions')
            res = nle.get_category('results')
            gi = nle.get_category('general information')
            uv = nle.get_category('uv')
            sp = nle.get_category('data')

            # add column family to key
            dict_merged_sp[datetime] = dict()
            for k in mc.keys():
                dict_merged_sp[datetime]['measurement_conditions:' + str(k)] = str(mc[k])

            for wl in sp.keys():
                dict_merged_sp[datetime]['sp_ird:' + str(wl)] = str(sp[wl])

            for k in res.keys():
                dict_merged_sp[datetime]['results:' + str(k)] = str(res[k])

            for k in gi.keys():
                dict_merged_sp[datetime]['general_information:' + str(k)] = str(gi[k])

            for k in uv.keys():
                dict_merged_sp[datetime]['uv:' + str(k)] = str(uv[k])

        return dict_merged_sp

    def _hb_insert_nle(self, dict_nlentity: dict):  # merge 된 dictionary를 HBase에 insert
        rowkeyset = dict_nlentity.keys()
        for rowkey in rowkeyset:
            try:
                self._hb_table_conn.put(rowkey, dict_nlentity[rowkey])
            except Exception:
                self.create_table()
                self._hb_table_conn.put(rowkey, dict_nlentity[rowkey])

    def create_table(self):  # HBase table 생성
        dict_families = {'measurement_conditions': dict(),
                         'results': dict(),
                         'general_information': dict(),
                         'sp_ird': dict(),
                         'uv': dict()}

        self._hb_conn.create_table(self._tablename, dict_families)
        self._hb_table_conn = self._hb_conn.table(self._tablename)

    def insert_datedir(self, datedir: str):  # 하나의 일자에 대한 isd 파일을 모아놓은 디렉터리의 모든 자연광 데이터 insert
        dict_dailydata = self._getmerge_nle_hbrow(datedir)
        self._hb_insert_nle(dict_dailydata)

    def select_datehm(self, dhm: str):  # HBase로부터 분 단위 데이터 조회, dhm format: '%Y-%m-%d %H%M'
        row = self._hb_table_conn.row(dhm, ['sp_ird'])

        # print dict pretty
        pp = pprint.PrettyPrinter(indent=2)
        pp.pprint(row)

    def delete_day(self, date):  # 하루치 데이터 통째로 삭제
        from datetime import datetime, timedelta
        onemin = timedelta(minutes=1)
        startdt = datetime.strptime(date + ' 0000', '%Y-%m-%d %H%M')
        for j in range(1440):
            strdt = startdt.strftime('%Y-%m-%d %H%M')
            row = self._hb_table_conn.row(strdt, ['sp_ird'])
            count = len(row.keys())
            if count > 2000:
                print('delete row :', strdt)
                self._hb_table_conn.delete(strdt, ['sp_ird'])

            startdt += onemin


if __name__ == "__main__":
    nldmgr = HappybaseManager('210.102.142.14', 'natural_light')  # HBase 서버 설정
    nldmgr.connect()  # HBase 연결
    nldmgr.select_datehm('2018-11-20 1200')  # 11월 20일 12시 정각 데이터 가져오기
    nldmgr.disconnect()  # 종료 꼭 해줘야됨

