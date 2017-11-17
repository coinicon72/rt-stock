from datetime import datetime
import logging
from collections import deque
import argparse
import asyncio
import json, zlib, pickle
import zmq
from zmq.asyncio import Context
zmq.asyncio.install()


logger = logging.getLogger(__name__)
hdlr = logging.FileHandler('log.txt')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
logger.addHandler(hdlr) 
logger.setLevel(logging.DEBUG)


HIST_WINDOW = 20
LAST_WINDOW = 3
THRESHOLD = 2

class Statistics:
    def __init__(self, window=5):
        self.window = window
        self.data = deque()
        self.total = 0
        self.mean = 0

    def push(self, d):
        self.data.append(d)
        self.total += d
        if self.window > 0 and len(self.data) > self.window:
            self.total -= self.data.popleft()
        self.mean = self.total / len(self.data)

    def is_full(self):
        return len(self.data) >= self.window

    def __str__(self):
        return f'mean: {self.mean} data: {self.data}'


class Stock:
    def __init__(self, threshold):
        self.hist = Statistics(HIST_WINDOW)
        self.last = Statistics(LAST_WINDOW)
        self.threshold = threshold

    def _is_over_threshold(self):
        if self.last.is_full() and  (self.last.mean - self.hist.mean) / self.hist.mean >= self.threshold:
            print(self.hist, self.last)
            logger.info(str(self.hist) + ' ## ' + str(self.last))
            return True
        else:
            return False
        
    def push(self, d):
        self.hist.push(d)
        self.last.push(d)

        return self._is_over_threshold()


stocks = {}

ctx = Context.instance()

async def recv(server, ports, pub_port):
    sub = ctx.socket(zmq.SUB)
    sub.set_hwm(10_000)
    for p in ports:
        sub.connect(f'tcp://{server}:{p}')
    sub.subscribe(u'')

    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://*:{pub_port}")

    count = 0

    while True:
        z = await sub.recv()

        try:
            j = zlib.decompress(z).decode()
            d = json.loads(j)

            """
            data format:
            {'t': 1510290895.005001, 'd': [{'c': '600276.SH', 'RT_VIP_SELL_AMT': 398652504.0}, {...}]}
            timestamp                      code              index                            another codes
            """
            # count += 1
            # print(count, d['count'], datetime.now().time())
            ts = datetime.fromtimestamp(d['t'])
            for s in d['d']:
                c = s['c']
                if c not in stocks:
                    stocks[c] = Stock(THRESHOLD)
                if stocks[c].push(s['rt_last_vol']):
                    pub.send_string(f"{str(ts)},{c},{s['rt_last_vol']}")
        except Exception as e:
            print(e)


async def timer(interval=1):
    while True:
        await asyncio.sleep(interval)
        # print('...')
        

def main(server, ports, pub_port):
    loop = asyncio.get_event_loop()

    tasks = [
        asyncio.ensure_future(timer()),
        asyncio.ensure_future(recv(server, ports, pub_port))
    ]

    try:
        loop.run_until_complete(asyncio.wait(tasks))
    except KeyboardInterrupt:
        pass
        
    print('*** done ***')


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='stock real time data parser')
    argparser.add_argument('ports', nargs='*', type=int, default=[5555])
    argparser.add_argument('-s', dest='server', metavar='server', default='127.0.0.1')
    argparser.add_argument('-pub', dest='pub_port', metavar='pub_port', default=5556)
    argparser.add_argument('-t', type=float, dest='threshold', metavar='threshold', default=1.5)
    args = argparser.parse_args()
    print(args)

    THRESHOLD = args.threshold
    main(args.server, args.ports, args.pub_port)