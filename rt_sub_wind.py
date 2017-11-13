import sys
from datetime import datetime
import time
import argparse
import asyncio
import json, zlib, pickle
import zmq
from WindPy import w as wind


INDEX = ['rt_last', 'rt_last_amt', 'rt_last_vol']

tick_count = 0   # received tick
msg_count = 0    # sent messge via zmq
index_count = 0  # sent index via zmq

def windCallback(data):
    try:
        global tick_count
        global msg_count
        global index_count

        global socket

        """
        1. convert each record to object
        {'t': 1510290895.005001, 'd': [{'c': '600276.SH', 'RT_VIP_SELL_AMT': 398652504.0}, {...}]}
          timestamp                      code              index                            another codes

        1.1 filter out index not in IDX_FILTER
        2. convert to json string
        3. compress with zlib
        4. publish via zmq
        """
        tick_count += 1
        d = {'t': data.Times[0].timestamp(), 'd': []}

        for i in range(len(data.Codes)):
            c = data.Codes[i]
            cd = {}
            for j in range(len(data.Fields)):
                idx = data.Fields[j].lower()
                if idx in INDEX:
                    cd[idx] = data.Data[j][i]
            
            if len(cd) > 0:
                index_count += len(cd)
                cd['c'] = c
                d['d'].append(cd)

        if len(d['d']) > 0:
            # d['count'] = count
            j = json.dumps(d)
            z = zlib.compress(j.encode())

            socket.send(z)

            msg_count += 1
            # count += 1
            print(f'\rtick: {tick_count}, msg: {msg_count}, idx: {index_count}\t', datetime.now().time(), end='')

    except Exception as e:
        print(e)


socket = None
def sub(port):
    global socket
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind(f"tcp://*:{port}")

    print("starting wind")
    wind.start()

    start = datetime.now()
    print(f"running {start}\n")

    wind.wsq('000725.SZ, 000839.SZ, 601318.SH, 000001.SZ, 600030.SH, 000333.SZ, 601108.SH, 600050.SH, 000651.SZ, 002230.SZ, 000063.SZ, 002466.SZ, 002405.SZ, 002460.SZ, 600519.SH, 603019.SH, 600887.SH, 601766.SH, 601668.SH, 601688.SH, 600276.SH, 600690.SH, 000917.SZ, 000100.SZ, 600516.SH, 600036.SH, 300699.SZ, 002415.SZ, 601398.SH, 300059.SZ, 002797.SZ, 000538.SZ, 000858.SZ, 300088.SZ, 600807.SH, 603466.SH, 000868.SZ, 603533.SH, 002594.SZ, 603799.SH, 002340.SZ, 002240.SZ, 600028.SH, 002407.SZ, 601166.SH, 600518.SH, 600585.SH, 601336.SH, 000776.SZ, 601601.SH', INDEX, func=windCallback)

    try:
        while True:
            time.sleep(0.01)
    except KeyboardInterrupt:
        print('\n\n*** stopping ... ***')
        wind.cancelRequest(0)
        wind.stop()

    print("*** done ***")


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='subscribe Wind stock tick (real time) data')
    argparser.add_argument('port', nargs='?', type=int, default=5555)

    args = argparser.parse_args()
    print(args)
    sub(args.port)
