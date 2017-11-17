import sys
from datetime import datetime
import time
import argparse
import asyncio
import json, zlib, pickle
import pandas as pd
import zmq
from WindPy import w as wind


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

        # 1.1 filter out index not in IDX_FILTER
        2. convert to json string
        3. compress with zlib
        4. publish via zmq
        """
        tick_count += 1
        d = {'t': data.Times[0].timestamp(), 'd': []}
        # print(data.Times[0], data.Times[0].timestamp())
        # print(d)

        for i in range(len(data.Codes)):
            c = data.Codes[i]
            cd = {}
            for j in range(len(data.Fields)):
                idx = data.Fields[j].lower()
                # if idx in INDEX:
                cd[idx] = data.Data[j][i]
            
            # if len(cd) > 0:
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

    print("get stock list")
    all_stocks = wind.wset("listedsecuritygeneralview","sectorid=a001010100000000;field=wind_code,sec_name,trade_status")
    stocks = ','.join(all_stocks.Data[0])
    # df = pd.DataFrame(all_stocks.Data).T
    # df = df[df[2] == '正常交易']
    # stocks = ','.join(df[0])

    start = datetime.now()
    print(f"running {start}\n")

    wind.wsq(stocks, 'rt_last_vol', func=windCallback)

    print('press ctrl+c to exit')
    try:
        while True:
            input()
    except KeyboardInterrupt:
        print('\n\n*** stopping ... ***')
        wind.cancelRequest(0)
        wind.stop()
    except Exception as e:
        print(e)

    print("*** done ***")


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='subscribe Wind stock tick (real time) data')
    argparser.add_argument('port', nargs='?', type=int, default=5555)

    args = argparser.parse_args()
    print(args)
    sub(args.port)
