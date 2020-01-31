from entries.cas_entry import CasEntry
import csv


dirname = 'C:/Users/WitLab-DaeHwan/Desktop/20190207'
fnamelist = CasEntry.search(dirname)
outfile = open(dirname + '/output.csv', 'w', encoding='utf-8', errors='ignore', newline='')
cw = csv.writer(outfile)

cw.writerow(['time', 'illum', 'cct', 'swr', 'uva', 'uvb', 'euva', 'euvb', 'uvi', 'auv'])

for fname in fnamelist:
    entry = CasEntry(fname, debug=True)
    if not entry.valid:
        continue
    time = entry.get_datetime(tostr=True)
    time = entry.get_attrib()
    uva = entry.get_ird(315, 400)
    uvb = entry.get_ird(280, 315)
    euva = entry.get_ird(315, 400, weight_func='ery')
    euvb = entry.get_ird(280, 315, weight_func='ery')
    uvi = entry.get_ird(280, 400, weight_func='ery')

    row = []

    cw.writerow(row)
    print(entry.get_attrib('file_name'), ':', row)

outfile.close()
