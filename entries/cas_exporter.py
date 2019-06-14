from entries.cas_entry import CasEntry
import csv


dirname = 'D:/_nldw/20190324'
fnamelist = CasEntry.search(dirname)
outfile = open(dirname + '/output.csv', 'w', encoding='utf-8', errors='ignore', newline='')
cw = csv.writer(outfile)

cw.writerow(['time', 'nuv', 'auv'])
for fname in fnamelist:
    entry = CasEntry(fname)
    if not entry.valid:
        continue
    time = entry.get_datetime(tostr=True)
    nuv = entry.get_element('uva')
    auv = entry.get_element('auv')

    row = [time, nuv, auv]

    cw.writerow(row)
    print(fname, ':', row)

outfile.close()
