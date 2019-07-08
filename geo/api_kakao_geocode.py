from debugmodule import Log

import requests


def getLatLon(address: str):
    """
    * api info
    GET /v2/local/search/address.{format} HTTP/1.1
    Host: dapi.kakao.com
    Authorization: KakaoAK {app_key}
    request
    키	설명                            필수    	타입
    query	검색을 원하는 질의어(주소)     	O        	String
    page	결과 페이지 번호            	X(기본 1)	Integer
    size	한 페이지에 보여질 문서의 개수	X(기본 10)	1-30 사이 Integer

    * usage
    :param address: 위경도를 알아내고자 하는 대상의 주소 (도로명주소 권장)
    :return: tuple(lat: float, lon: float)

    * important issues
        1. API 호출횟수 제한이 있으므로 사용에 유의
    """
    # address_test = '천안대로 1223-24'
    apikey = 'cf606c4c1964ec437d0134cbb5a8deb9'

    url = 'https://dapi.kakao.com/v2/local/search/address.json'
    headers = {'Authorization': 'KakaoAK ' + apikey}
    params = {'query': address}

    response = requests.get(url, headers=headers, params=params)

    try:
        lat = response.json()['documents'][0]['road_address']['y']
        lon = response.json()['documents'][0]['road_address']['x']
    except TypeError:  # 지번주소로 써있는 경우
        lat = response.json()['documents'][0]['address']['y']
        lon = response.json()['documents'][0]['address']['x']
    except IndexError:  # 주소가 잘못되었으면
        Log.e('getLatLon()', 'wrong address:', address)

        lat = 0.0
        lon = 0.0

    Log.d('getLatLon()', 'latitude:', lat, ', longitude:', lon)

    return lat, lon


if __name__ == "__main__":
    import pandas as pd
    from pyspark.sql.types import StructField, StructType, StringType, DoubleType
    from sparkmodule import PySparkManager

    pdf = pd.read_csv('/home/witlab/uvmon_location.csv', encoding='utf-8')
    lat_col = []
    lon_col = []

    for i in range(len(pdf)):
        address = pdf.iloc[i]['address']
        Log.d('__main__', 'address:', address)
        lat, lon = getLatLon(address)
        lat_col.append(float(lat))
        lon_col.append(float(lon))

    pdf['lat'] = lat_col
    pdf['lon'] = lon_col

    Log.d('__main__', 'pdf:\n', pdf)

    # create spark dataframe
    # col : [﻿location,station_code,address ]
    schema = StructType([
        StructField('location', StringType()),
        StructField('station_code', StringType()),
        StructField('address', StringType()),
        StructField('lat', DoubleType()),
        StructField('lon', DoubleType()),
    ])
    spdf = PySparkManager().sqlctxt.createDataFrame(pdf, schema)
    spdf.write.mode('overwrite').parquet('hdfs:///nl/kma/uv_location.parquet')
