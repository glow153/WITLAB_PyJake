from debug_module import Log
from basemodule import PySparkManager
import os
import pandas as pd


locationcode = 132


# 현재 안면도 파일만 넣어야 작동함

class KmaUvab:
    def __init__(self):
        # unit : sum_uva = MJ/m2
        #        sum_uvb = KJ/m2
        #        max_uva, b = W/m2

        self.column_a = ('station_code', 'datehour', 'sum_uva', 'max_uva')
        self.column_b = ('station_code', 'datehour', 'sum_uvb', 'max_uvb')
        self.column = ('station_code', 'datehour', 'sum_uva', 'sum_uvb', 'max_uva', 'max_uvb')
        self._pdf_uva = pd.DataFrame(columns=self.column_a)
        self._pdf_uvb = pd.DataFrame(columns=self.column_b)
        self._pdf = pd.DataFrame(columns=self.column)

        self._spdf = None

        self._hdfs_path = 'hdfs:///nl/kma/uvab.parquet'

    def search(self, dirname):
        """
        dirname 디렉토리 내의 모든 파일과 디렉토리 이름을 리스트로 반환함
        :param dirname: ISD 파일 들어있는 디렉토리 경로, :type: str
        :return: 모든 isd 파일의 절대경로, :type: list[str]
        """
        filelist = []
        filenames = os.listdir(dirname)
        for filename in filenames:
            full_filename = os.path.join(dirname, filename)
            filelist.append(full_filename)
        return filelist

    def read_single_file(self, filepath, category, station_code):
        rows = []
        with open(filepath, 'rt', encoding='utf-8', errors='ignore') as file:
            line = file.readline()
            while line:
                split_row = line.split(',')

                try:
                    int(split_row[0])  # check whether first word is a number
                    datehour = split_row[0][:4] + '-' + split_row[0][4:6] + '-' + split_row[0][6:8] \
                                                + ' ' + split_row[0][8:]
                    sum_uv = float(split_row[1].strip())
                    max_uv = float(split_row[2].strip())

                    rows.append([station_code, datehour, sum_uv, max_uv])

                except ValueError:
                    pass

                line = file.readline()  # goto next line

            # repair wrong datehour
            tmp_row = rows.pop(len(rows) - 1)
            tmp_row[1] = tmp_row[1][:10] + ' 23'
            rows.append(tmp_row)

        if category == 'UVA':
            tmppdf = pd.DataFrame(rows, columns=self.column_a)
            self._pdf_uva = self._pdf_uva.append(tmppdf, ignore_index=True)

        if category == 'UVB':
            tmppdf = pd.DataFrame(rows, columns=self.column_b)
            self._pdf_uvb = self._pdf_uvb.append(tmppdf, ignore_index=True)

        # Log.d('read_single_file()', rows)
        # Log.d('read_single_file()', self._pdf_uva)
        # Log.d('read_single_file()', self._pdf_uvb)

    def yearly(self, dirname, year: int):
        dirlist = ku.search(dirname)
        Log.d('yearly',  'dirlist:', dirlist)

        for dirpath in dirlist:
            fnamelist = ku.search(dirpath + '/' + str(year))

            for fname in fnamelist:
                Log.d('yearly()', 'fname:', fname)
                ku.read_single_file(fname, fname.split('_')[1], fname.split('/')[4])

    def merge_uvab(self):
        self._pdf = pd.merge(self._pdf_uva, self._pdf_uvb, how='outer')

    def pdf2hdfs(self, mode='append', hdfs_path=''):
        """
        hdfs에 parquet 형식으로 저장
        :param mode: 저장 방식, 'append', 'overwrite'
        :param hdfs_path: hdfs 경로 지정, 입력 없으면 기본 저장경로
        :return: nothing
        """
        if hdfs_path == '':
            path = self._hdfs_path
        else:
            path = hdfs_path

        try:
            Log.d('pdf2hdfs()', 'pdf -> hdfs ::\n', self._pdf.iloc[:30])
        except Exception as e:
            Log.e('pdf is empty! : ', e.__class__.__name__)
            return

        # make spark dataframe
        self._spdf = PySparkManager().sqlctxt.createDataFrame(self._pdf)

        # append new data
        self._spdf.write.mode(mode).parquet(path)

        Log.d('pdf2hdfs()', 'parquet write completed.')

    def sort_pdf(self):
        self._pdf = self._pdf.sort_values(['station_code', 'datehour'], ascending=True)
        self._pdf = self._pdf.reset_index(drop=True)


if __name__ == '__main__':
    ku = KmaUvab()
    ku.yearly('/home/witlab/kma-uv-2017', 2017)
    ku.merge_uvab()
    ku.sort_pdf()
    ku.pdf2hdfs()




