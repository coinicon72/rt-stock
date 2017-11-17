import sys
from datetime import datetime
import time
import argparse
import json, zlib, pickle
import zmq


IDX_FILTER = ['rt_last', 'rt_last_amt', 'rt_last_vol']

def pub(file, port, stop=2, sleep=0, test=0):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")
    print(f'running on port {port} ...\n')

    time.sleep(stop)
    start = datetime.now()

    count = 0

    for l in file:
        if test > 0 and count >= test:
            break

        """
        1. convert each record to object
        {'t': 1510290895.005001, 'd': [{'c': '600276.SH', 'RT_VIP_SELL_AMT': 398652504.0}, {...}]}
          timestamp                      code              index                            another codes

        1.1 filter out index not in IDX_FILTER
        2. convert to json string
        3. compress with zlib
        4. publish via zmq
        """

        if sleep > 0:
            time.sleep(sleep)

        z = zlib.compress(l.encode())
        socket.send(z)

        count += 1
        print(f'\r{count} {datetime.now().time()}', end='')

    d = datetime.now() - start
    print(f'\n\nprocess {count} records takes {d}')


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='stock real time data emulator')
    argparser.add_argument('file', type=argparse.FileType('r', encoding='UTF-8'))
    argparser.add_argument('port', nargs='?', type=int, default=5555)
    argparser.add_argument('-s', type=float, default=2, dest='stop', metavar='stop',
                            help='stop before publish')
    argparser.add_argument('-i', type=float, default=0, dest='interval', metavar='interval', 
                            help='interval of sending message')
    argparser.add_argument('-t', type=int, default=0, dest='test', metavar='test', 
                            help='sending few messages for test')

    args = argparser.parse_args()
    print(args)
    pub(args.file, args.port, args.stop, args.interval, args.test)
