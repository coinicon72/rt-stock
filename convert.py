import sys
import json, zlib, pickle

rt = pickle.load(open('rtdate_2017-11-09 110618.145808_2017-11-09 113316.201166.pickle', 'rb'))

ds = []
for data in rt:
    d = {'t': data.Times[0].timestamp(), 'd': []}

    for i in range(len(data.Codes)):
        # try:
        #     c = data.Codes[i]
        #     if c not in rtary:
        #         rtary[c] = None
        #     d = np.array(data.Data)
        #     if rtary[c] is None:
        #         rtary[c] = np.append(data.Times, d[:, i])
        #     else:
        #         np.vstack((rtary[c], np.append(data.Times, d[:, i])))
        # except:
        #     print(data, i)
        c = data.Codes[i]
        cd = {'c': c}
        for j in range(len(data.Fields)):
            idx = data.Fields[j]
            cd[idx] = data.Data[j][i]

        d['d'].append(cd)

    ds.append(d)

j = json.dumps(ds)
z = zlib.compress(j.encode())

print(f'size of rt {sys.getsizeof(rt)}, records: {len(rt)}')
print(f'size of ds {sys.getsizeof(ds)}, records: {len(ds)}')
print(f'size of json {sys.getsizeof(j)}')
print(f'size after compress {sys.getsizeof(z)}')
