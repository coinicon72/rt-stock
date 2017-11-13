"""
receive tick data from zmq and store it in sqlite

tick date is zlib-compressed json string
{'t': 1510290895.005001, 'd': [{'c': '600276.SH', 'RT_VIP_SELL_AMT': 398652504.0}, {...}]}
  timestamp                      code              index                            another codes

"""
from datetime import datetime
import argparse
import asyncio
import json, zlib, pickle
import sqlite3
import mysql.connector
import zmq
from zmq.asyncio import Context
zmq.asyncio.install()

ctx = Context.instance()


DB_NAME = 'rt.sqlite'
# def get_db():
#     conn = sqlite3.connect(DB_NAME)

def get_socket(server, ports):
    s = ctx.socket(zmq.SUB)
    s.set_hwm(10_000)
    for p in ports:
        s.connect(f'tcp://{server}:{p}')
    
    return s


SQL_INSERT = 'insert or replace into tick (ts, code, last, last_vol, last_amt) values (?, ?, ?, ?, ?)'
# SQL_INSERT = 'replace into tick (ts, code, last, last_vol, last_amt) values (from_unixtime(%s), %s, %s, %s, %s)'
async def recv(socket):
    conn = sqlite3.connect(DB_NAME)
    # conn = mysql.connector.connect(host='localhost', user='ky', password='ky', database='stock')
    cur = conn.cursor()
    # s = ctx.socket(zmq.SUB)
    # s.set_hwm(10_000)
    # for p in ports:
    #     s.connect(f'tcp://{server}:{p}')
    socket.subscribe(u'')

    def tick_generator(d):
        ts = d['t']
        for c in d['d']:
            yield (ts, c['c'], c['rt_last'], c['rt_last_vol'], c['rt_last_amt'])

    try:
        count = 0
        while True:
            z = await socket.recv()
            j = zlib.decompress(z)
            d = json.loads(j.decode())

            v = []
            ts = d['t']
            for c in d['d']:
                v.append((ts, c['c'], c['rt_last'], c['rt_last_vol'], c['rt_last_amt']))
            
            cur.executemany(SQL_INSERT, v)
            if count % 1000 == 0:
                conn.commit()
                print('commit')

            count += 1
            print(count, d['count'], datetime.now().time())
    except KeyboardInterrupt:
        socket.close()
        cur.close()
        conn.close()
    except Exception as e:
        print(e)


async def timer(interval=3):
    while True:
        await asyncio.sleep(interval)
        print('...')
        

def main(server, ports):
    loop = asyncio.get_event_loop()

    try:
        tasks = [
            asyncio.ensure_future(timer()),
            asyncio.ensure_future(recv(get_socket(server, ports)))
        ]

        loop.run_until_complete(asyncio.wait(tasks))
        # loop.run_until_complete(recv(get_socket(server, ports)))
    except KeyboardInterrupt:
        print('done')
    except Exception as e:
        print(e)

    # loop.run_until_complete(loop.close())


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='stock real time data parser')
    argparser.add_argument('ports', nargs='*', type=int, default=[5555])
    argparser.add_argument('-s', nargs='?', dest='server', metavar='server', default='127.0.0.1')
    args = argparser.parse_args()
    print(args)

    main(args.server, args.ports)