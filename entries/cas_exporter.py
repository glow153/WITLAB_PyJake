from entries.cas_entry import CasEntry
import csv


dirname = 'C:/Users/WitLab-DaeHwan/Desktop/ledtest'
fnamelist = CasEntry.search(dirname)
outfile = open(dirname + '/output_20190708.csv', 'w', encoding='utf-8', errors='ignore', newline='')
cw = csv.writer(outfile)

cw.writerow(['time', 'illum', 'cct', 'swr', 'narrow'])
for fname in fnamelist:
    entry = CasEntry(fname, debug=True)
    if not entry.valid:
        continue
    time = entry.get_datetime(tostr=True)
    illum = entry.get_attrib('Photometric')
    cct = entry.get_attrib('CCT')
    swr = entry.get_attrib('swr')
    narrow = entry.get_attrib('narr')

    row = [time, illum, cct, swr, narrow]

    cw.writerow(row)
    print(fname, ':', row)

outfile.close()
