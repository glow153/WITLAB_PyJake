from pyspark.sql import Row


def isNumber(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def kmaSkyCsv2Parquet(hdfsCsvPath, hdfsParquetPath):
    pathHeader = 'hdfs://'
    df_sky = []
    ym = ''
    sky_raw = sc.textFile(pathHeader + hdfsCsvPath) \
        .collect()

    for line in sky_raw:

        if not line.replace(',', ' ').strip():
            break  # exit condition

        # print(line)  # for debug
        if 'Start' in line:
            print("line.split('Start : ')[1][:-3] =>", line.split('Start : ')[1][:-3])  # for debug
            ym = line.split('Start : ')[1][:-3]  # get 'yyyymm'
            ym = ym[:4] + '-' + ym[4:] + '-'  # format to 'yyyy-mm-'

        else:
            s = line.split(',')
            day = int(s[0])
            hour = int(int(s[1]) / 100)
            if '.' in s[2]:
                s[2] = s[2].split('.')[0]
            sky_val = int(s[2])

            datehour = ym + ('0' + str(day) if day < 10 else str(day)) + ' ' + \
                       ('0' + str(hour) if hour < 10 else str(hour))
            df_sky.append(Row(datehour=datehour, sky=sky_val))

    df_sky = sc.parallelize(df_sky) \
        .toDF().sort('datehour')

    # write parquet to hdfs
    # df_sky.write.mode('overwrite').parquet(pathHeader + hdfsParquetPath + '.parquet')

    return df_sky
