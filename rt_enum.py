import sys
from datetime import datetime
import time
import argparse
import json, zlib, pickle
import zmq


IDX_FILTER = ['rt_last', 'rt_last_amt', 'rt_last_vol']

def pub(port, stop=2, sleep=0, test=0):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")
    print(f'running on port {port} ...')

    rt = pickle.load(open('rtdate_2017-11-10 131455.092124_2017-11-10 132728.743309.pickle', 'rb'))
    if test > 0:
        rt = rt[:test]

    time.sleep(stop)
    start = datetime.now()

    count = 0
    for data in rt:
        """
        1. convert each record to object
        {'t': 1510290895.005001, 'd': [{'c': '600276.SH', 'RT_VIP_SELL_AMT': 398652504.0}, {...}]}
          timestamp                      code              index                            another codes

        1.1 filter out index not in IDX_FILTER
        2. convert to json string
        3. compress with zlib
        4. publish via zmq
        """
        d = {'t': data.Times[0].timestamp(), 'd': []}

        for i in range(len(data.Codes)):
            c = data.Codes[i]
            cd = {'c': c}
            for j in range(len(data.Fields)):
                idx = data.Fields[j].lower()
                if idx in IDX_FILTER:
                    cd[idx] = data.Data[j][i]
            
            if len(cd) > 1:
                d['d'].append(cd)

        if len(d['d']) > 0:
            d['count'] = count
            j = json.dumps(d)
            z = zlib.compress(j.encode())

            if sleep > 0:
                time.sleep(sleep)
            socket.send(z)
            count += 1
            print(count, datetime.now().time())

    d = datetime.now() - start
    print(f'process {len(rt)} records takes {d}')


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='stock real time data emulator')
    argparser.add_argument('port', nargs='?', type=int, default=5555)
    argparser.add_argument('-s', type=float, default=2, dest='stop', metavar='stop',
                            help='stop before publish')
    argparser.add_argument('-i', type=float, default=0, dest='interval', metavar='interval', 
                            help='interval of sending message')
    argparser.add_argument('-t', type=int, default=0, dest='test', metavar='test', 
                            help='sending few messages for test')

    args = argparser.parse_args()
    print(args)
    pub(args.port, args.stop, args.interval, args.test)
