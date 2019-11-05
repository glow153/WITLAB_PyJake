from entries.cas_entry import CasEntry
import pandas as pd


dirname = 'C:/Users/WitLab-DaeHwan/Desktop/tmp/20191104_1'
fnamelist = CasEntry.search(dirname)

dfdict = {}

for fname in fnamelist:
    entry = CasEntry(fname, debug=True)
    if not entry.valid:
        continue

    sp = entry.get_weighted_spird('ery')

    dfdict[entry.get_datetime(tostr=True)] = sp

df = pd.DataFrame(dfdict)
df.to_csv(dirname + '/output.csv')

