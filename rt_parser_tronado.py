from datetime import datetime
import time
import json, zlib, pickle
import asyncio
from tornado import gen#, ioloop
import zmq
from zmq.eventloop.future import Context
from zmq.eventloop import ioloop
ioloop.install()
loop = ioloop.IOLoop.instance()
# from zmq.eventloop.ioloop import IOLoop
# loop = IOLoop.current()

ctx = Context.instance()


async def recv():
    s = ctx.socket(zmq.SUB)
    s.set_hwm(10_000)
    s.connect('tcp://127.0.0.1:5556')
    s.subscribe(b'')

    count = 0
    while True:
        z = await s.recv()
        j = zlib.decompress(z)
        d = json.loads(j.decode())

        count += 1
        print(count, d['count'], datetime.now().time())

    s.close()


async def timer(interval=3):
    count = 0
    def timeout():
        nonlocal count
        count += 1
        print('...', count)
        loop.call_later(interval, timeout)

    loop.call_later(interval, timeout)


loop.spawn_callback(recv)
loop.spawn_callback(timer)
loop.start()
