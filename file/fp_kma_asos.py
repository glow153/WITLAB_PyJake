from ..sparkmodule import PySparkManager


def search(dirname):
    import os
    filelist = []
    filenames = os.listdir(dirname)
    for filename in filenames:
        full_filename = os.path.join(dirname, filename)
        filelist.append(full_filename)
    return filelist


def renameCols(spdf, old_columns, new_columns):
    for old_col, new_col in zip(old_columns, new_columns):
        spdf = spdf.withColumnRenamed(old_col, new_col)
    return spdf


def dropCols(spdf, drop_columns):
    for col in drop_columns:
        spdf = spdf.drop(col)
    return spdf


def read_asos(fname):
    old_columns = ['지점', '일시', '기온(°C)', '누적강수량(mm)', '풍향(deg)', '풍속(m/s)',
                   '현지기압(hPa)', '해면기압(hPa)', '습도(%)',
                   '일사(MJ/m^2)', '일조(Sec)']

    spdf_asos = PySparkManager().sqlctxt.read \
                                .option('header', 'true') \
                                .option('encoding', 'euc-kr') \
                                .option('mode', 'DROPMALFORMED') \
                                .csv('file://%s' % fname).cache()

    new_columns = ['station_code', 'datetime', 'temperature', 'accum_precipitation',
                   'wind_dir', 'wind_speed', 'local_air_pressure', 'sea_air_pressure',
                   'humidity', 'solar_radiation', 'solar_rad_time']
    spdf_asos_r = renameCols(spdf_asos, old_columns, new_columns)

    # drop_columns = ['station_code', 'precipitation', 'precipitation_qc', 'cloud_type', 'min_cloud_height']
    # spdf_asos_r = dropCols(spdf_asos_r, drop_columns)

    return spdf_asos_r


if __name__ == '__main__':

    fnamelist = search('/home/witlab/asos/data')
    print(fnamelist)

    # start integration
    print('asos file:', fnamelist[0].split('/')[5])
    spdf_asos = read_asos(fnamelist[0])
    fnamelist.pop(0)

    for fname in fnamelist:
        print('asos file:', fname.split('/')[5])
        spdf_asos = spdf_asos.union(read_asos(fname))

    spdf_asos.show()



