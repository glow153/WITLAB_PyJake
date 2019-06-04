import datetime
import os


class JazSpectralEntity:

    # categories
    __sp_ird = {}
    _valid = False

    def __init__(self, fname):
        file = open(fname, 'rt', encoding='utf-8', errors='ignore')
        self._valid = self.__map_data(file)
        file.close()

    def __map_data(self, file):
        line = file.readline()
        if line == 'SpectralSuite Data File':
            valid = True
        else:
            valid = False

        while line:
            if valid:
                kv = line.strip().split()
                k = kv[0]
                v = kv[1]
                try:
                    # table.append(line.strip().split())
                    self.__sp_ird[k] = v
                except IndexError:
                    break
            if line.strip() == '>>>>>Begin Processed Spectral Data<<<<<':
                valid = True
            line = file.readline()

        return valid