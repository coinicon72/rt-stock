from datetime import datetime
import json, zlib, pickle
from tornado import gen#, ioloop
import zmq
from zmq.eventloop.future import Context
from zmq.eventloop import ioloop
ioloop.install()
loop = ioloop.IOLoop.instance()
# from zmq.eventloop.ioloop import IOLoop
# loop = IOLoop.current()

ctx = Context.instance()

@gen.coroutine
def recv():
    s = ctx.socket(zmq.SUB)
    # s.set_hwm(10_000)
    s.connect('tcp://127.0.0.1:5556')
    s.connect('tcp://127.0.0.1:5555')
    s.subscribe(b'')

    count = 0
    while True:
        z = yield s.recv()
        j = zlib.decompress(z)
        d = json.loads(j.decode())

        count += 1
        print(count, d['count']) #, datetime.now().time())

    s.close()

loop.spawn_callback(recv)
loop.start()
