import sys
from datetime import datetime
import json, zlib, pickle
import pandas as pd

rt = pickle.load(open('rtdate_2017-11-10 131455.092124_2017-11-10 132728.743309.pickle', 'rb'))

dfs = {}
for rd in rt:#[:10]:
    # d = {'t': rd.Times[0].timestamp(), 'd': []}
    ts = rd.Times[0].timestamp()

    for i in range(len(rd.Codes)):
        c = rd.Codes[i]
        if c not in dfs:
            dfs[c] = pd.DataFrame()

        cd = {'t': ts}
        for j in range(len(rd.Fields)):
            idx = rd.Fields[j]
            cd[idx] = rd.Data[j][i]
            
        dfs[c] = dfs[c].append(cd, ignore_index=True)
    

# if __name__ == '__main__':
#     argparser = argparse.ArgumentParser(prog='stock real time data emulator')
#     argparser.add_argument('port', nargs='?', type=int, default=5555)
#     argparser.add_argument('-s', type=float, default=2, dest='stop', metavar='stop',
#                             help='stop before publish')
#     argparser.add_argument('-i', type=float, default=0, dest='interval', metavar='interval', 
#                             help='interval of sending message')

#     args = argparser.parse_args()
#     print(args)
#     pub(args.port, args.stop, args.interval)
