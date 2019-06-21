from entries.cas_entry import CasEntry
import csv


dirname = 'D:/_nldw/20190421'
fnamelist = CasEntry.search(dirname)
outfile = open(dirname + '/output.csv', 'w', encoding='utf-8', errors='ignore', newline='')
cw = csv.writer(outfile)

cw.writerow(['time', 'illum', 'cct', 'swr', 'narrow', 'cri'])
for fname in fnamelist:
    entry = CasEntry(fname)
    if not entry.valid:
        continue
    time = entry.get_datetime(tostr=True)
    illum = entry.get_attrib('Photometric')
    cct = entry.get_attrib('CCT')
    swr = entry.get_attrib('swr')
    narrow = entry.get_attrib('narrow')
    cri = entry.get_attrib('CRI')

    row = [time, illum, cct, swr, narrow, cri]

    cw.writerow(row)
    print(fname, ':', row)

outfile.close()
