from .cas_entry import CasEntry
import csv


dirname = '/home/witlab/dw/raw/20170826'
fnamelist = CasEntry.search(dirname)
outfile = open('/home/witlab/output_20170826.csv', 'w', encoding='utf-8', errors='ignore', newline='')
cw = csv.writer(outfile)

cw.writerow(['time', 'illum', 'uvb', 'CCDTemperature'])
for fname in fnamelist:
    entry = CasEntry(fname)
    if not entry.valid:
        continue
    time = entry.get_datetime(tostr=True)
    illum = entry.get_attrib('Photometric')
    uvb = entry.get_attrib('uvb')
    ccd_temp = entry.get_attrib('CCDTemperature').split()[0]

    row = [time, illum, uvb, ccd_temp]

    cw.writerow(row)
    print(fname, ':', row)

outfile.close()
