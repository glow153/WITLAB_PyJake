def erythemal_action_spectrum(wavelength):
    """
    홍반 가중함수
    :param wavelength: 파장
    :return: 가중함수값
    """
    if 250 <= int(wavelength) <= 298:
        return 1
    elif 298 < int(wavelength) <= 328:
        return pow(10, 0.094 * (298 - wavelength))
    elif 328 < int(wavelength) <= 400:
        return pow(10, 0.015 * (140 - wavelength))
    else:
        return 0


def vitd_weight_func(wavelength):
    """
    비타민 D 가중함수
    :param wavelength: 파장
    :return: 가중함수값
    """
    if 252 <= int(wavelength) <= 330:
        weight_table = [0.036, 0.039, 0.043, 0.047, 0.051, 0.056, 0.061, 0.066, 0.075, 0.084,
                        0.093, 0.102, 0.112, 0.122, 0.133, 0.146, 0.160, 0.177, 0.195, 0.216,
                        0.238, 0.263, 0.289, 0.317, 0.346, 0.376, 0.408, 0.440, 0.474, 0.543,
                        0.583, 0.617, 0.652, 0.689, 0.725, 0.763, 0.805, 0.842, 0.878, 0.903,
                        0.928, 0.952, 0.976, 0.983, 0.990, 0.996, 1.000, 0.977, 0.951, 0.917,
                        0.878, 0.771, 0.701, 0.634, 0.566, 0.488, 0.395, 0.306, 0.220, 0.156,
                        0.119, 0.083, 0.049, 0.034, 0.020, 0.0141, 0.00976, 0.00652, 0.00436,
                        0.00292, 0.00195, 0.00131, 0.000873, 0.000584, 0.000390, 0.000261,
                        0.000175, 0.000117, 0.000078]
        return weight_table[int(wavelength) - 252]
    else:
        return 0


def vitd_weight_func_interpolated(wavelength):
    """
    비타민 D 가중함수, 선형보간법 적용
    :param wavelength: 파장
    :return: 가중함수값
    """
    if 252 <= int(wavelength) < 330:
        weight_table = [0.036, 0.039, 0.043, 0.047, 0.051, 0.056, 0.061, 0.066, 0.075, 0.084,
                        0.093, 0.102, 0.112, 0.122, 0.133, 0.146, 0.160, 0.177, 0.195, 0.216,
                        0.238, 0.263, 0.289, 0.317, 0.346, 0.376, 0.408, 0.440, 0.474, 0.543,
                        0.583, 0.617, 0.652, 0.689, 0.725, 0.763, 0.805, 0.842, 0.878, 0.903,
                        0.928, 0.952, 0.976, 0.983, 0.990, 0.996, 1.000, 0.977, 0.951, 0.917,
                        0.878, 0.771, 0.701, 0.634, 0.566, 0.488, 0.395, 0.306, 0.220, 0.156,
                        0.119, 0.083, 0.049, 0.034, 0.020, 0.0141, 0.00976, 0.00652, 0.00436,
                        0.00292, 0.00195, 0.00131, 0.000873, 0.000584, 0.000390, 0.000261, 0.000175, 0.000117,
                        0.000078]
        x = wavelength
        x1 = int(wavelength)
        x2 = x1 + 1
        y1 = weight_table[x1 - 252]
        y2 = weight_table[x2 - 252]

        weighted_interpolated = y1 + (y2 - y1) * (x - x1) / (x2 - x1)
        return weighted_interpolated
    else:
        return 0


def actinic_uv_weight_func(wavelength, dbg=False):
    """
    자외선 위해 가중함수 (IEC 62471), 선형보간법 적용됨
    VERIFIED CALCULATION AND FUNCTION 180727
    :param wavelength: 파장
    :return: 가중함수값
    """
    if 200 <= int(wavelength) <= 400:
        wltable = [200, 205, 210, 215, 220, 225, 230, 235, 240, 245, 250,
                   254, 255, 260, 265, 270, 275, 280, 285, 290, 295, 297, 300,
                   303, 305, 308, 310, 313, 315, 316, 317, 318, 319, 320,
                   322, 323, 325, 328, 330, 333, 335, 340, 345, 350,
                   355, 360, 365, 370, 375, 380, 385, 390, 395, 400]
        weight_table = [0.030, 0.051, 0.075, 0.095, 0.120, 0.150, 0.190, 0.240, 0.300, 0.360, 0.430,  # 200~250
                        0.500, 0.520, 0.650, 0.810, 1.000, 0.960, 0.880, 0.770, 0.640, 0.540, 0.460, 0.300,  # 254~300
                        0.120, 0.060, 0.026, 0.015, 0.006, 0.003, 0.0024, 0.002, 0.0016, 0.0012, 0.001,  # 303~320
                        0.00067, 0.00054, 0.0005, 0.00044, 0.00041, 0.00037, 0.00034, 0.00028, 0.00024, 0.0002,  # 322~350
                        0.00016, 0.00013, 0.00011, 0.000093, 0.000077, 0.000064, 0.000053, 0.000044, 0.000036, 0.000030]

        for i in range(len(wltable) - 1):
            if wltable[i] <= wavelength < wltable[i+1]:
                x = wavelength
                x1 = wltable[i]
                x2 = wltable[i+1]
                y1 = weight_table[i]
                y2 = weight_table[i+1]

                weight_interpolated = y1 + (y2 - y1) * (x - x1) / (x2 - x1)  # linear interpolation

                if dbg:
                    print(str(wavelength) + '\t' + str(weight_interpolated))

                return weight_interpolated
        return 0
    else:
        return 0


if __name__ == '__main__':
    # 홍반가중함수 테이블 뽑기
    wls = [x for x in range(250, 401)]
    print(wls[:5], '...', wls[-5:])

    for wl in wls:
        print(wl, '\t', erythemal_action_spectrum(wl))

