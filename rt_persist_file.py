"""
receive tick data from zmq and store it in sqlite

tick date is zlib-compressed json string
{'t': 1510290895.005001, 'd': [{'c': '600276.SH', 'RT_VIP_SELL_AMT': 398652504.0}, {...}]}
  timestamp                      code              index                            another codes

"""
from datetime import datetime
import inspect
import argparse
import asyncio
import json, zlib, pickle
import zmq
from zmq.asyncio import Context
zmq.asyncio.install()

ctx = Context.instance()


class TickStorage():
    def __init__(self, fn):
        self.file = open(fn, 'a')

    def persist(self, gen):
        if inspect.isgenerator(gen):
            for r in gen:
                self.file.write(r + '\n')

            self.file.flush()
        else:
            raise Exception('argumment gen not a generator')

    def close(self):
        self.file.close()


def get_socket(server, ports):
    s = ctx.socket(zmq.SUB)
    s.set_hwm(10_000)
    for p in ports:
        s.connect(f'tcp://{server}:{p}')
    
    return s


# SQL_INSERT = 'insert or replace into tick (ts, code, last, last_vol, last_amt) values (?, ?, ?, ?, ?)'
# SQL_INSERT = 'replace into tick (ts, code, last, last_vol, last_amt) values (from_unixtime(%s), %s, %s, %s, %s)'
async def recv(socket):
    storage = TickStorage('tick.json')
    socket.subscribe(u'')

    def tick_generator(d):
        ts = d['t']
        for c in d['d']:
            yield f"'ts': {ts}, 'code': '{c['c']}', 'rt_last': {c['rt_last']}, 'rt_last_vol': {c['rt_last_vol']}, 'rt_last_amt': {c['rt_last_amt']}"

    try:
        count = 0
        while True:
            z = await socket.recv()
            j = zlib.decompress(z)
            d = json.loads(j.decode())

            # v = []
            # ts = d['t']
            # for c in d['d']:
            #     v.append((ts, c['c'], c['rt_last'], c['rt_last_vol'], c['rt_last_amt']))

            storage.persist(tick_generator(d))

            count += 1
            print(f'\r{count}\t\t{datetime.now().time()}', end='')
    except KeyboardInterrupt:
        socket.close()
        storage.close()
    except Exception as e:
        print(e)


async def timer(interval=1):
    while True:
        await asyncio.sleep(interval)
        # print('...')
        

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
        print('\ndone')
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