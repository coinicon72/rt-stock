from datetime import datetime
import json, zlib, pickle
import zmq

with zmq.Context() as context:
    with context.socket(zmq.SUB) as socket:
        socket.set_hwm(10_000)
        socket.connect("tcp://localhost:5556")
        socket.subscribe(u'')

        count = 0
        while True:
            z = socket.recv()
            j = zlib.decompress(z)
            d = json.loads(j.decode())

            count += 1
            print(count, d['count']) #, datetime.now().time())
