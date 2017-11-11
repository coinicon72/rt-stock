from datetime import datetime
import argparse
import asyncio
import json, zlib, pickle
import zmq
from zmq.asyncio import Context
zmq.asyncio.install()

ctx = Context.instance()

async def recv(server, ports):
    s = ctx.socket(zmq.SUB)
    s.set_hwm(10_000)
    for p in ports:
        s.connect(f'tcp://{server}:{p}')
    s.subscribe(u'')

    count = 0

    while True:
        z = await s.recv()
        j = zlib.decompress(z)
        d = json.loads(j.decode())

        count += 1
        print(count, d['count'], datetime.now().time())


async def timer():
    while True:
        await asyncio.sleep(5)
        print('timing ...')
        

def main(server, ports):
    loop = asyncio.get_event_loop()

    tasks = [
        asyncio.ensure_future(timer()),
        asyncio.ensure_future(recv(server, ports))
    ]

    loop.run_until_complete(asyncio.wait(tasks))


if __name__ == '__main__':
    argparser = argparse.ArgumentParser(prog='stock real time data parser')
    argparser.add_argument('ports', nargs='*', type=int, default=[5555])
    argparser.add_argument('-s', nargs='?', dest='server', metavar='server', default='127.0.0.1')
    args = argparser.parse_args()
    print(args)

    main(arg.server, args.ports)