from datetime import datetime
import time
import pickle
import numpy as np
import pandas as pd
from WindPy import w as wind

rtdata = []
rtcount = 0
idxcount = 0

def windCallback2(data):
    try:
        global rtcount
        global idxcount
        rtdata.append(data)
        rtcount += 1
        idxcount += len(data.Codes)
    except Exception as e:
        print(e)

    print('\r', rtcount, idxcount, datetime.now() - start, end='')


wind.start()

start = datetime.now()
print(f"running {start}\n")

wsq = wind.wsq('000725.SZ, 000839.SZ, 601318.SH, 000001.SZ, 600030.SH, 000333.SZ, 601108.SH, 600050.SH, 000651.SZ, 002230.SZ, 000063.SZ, 002466.SZ, 002405.SZ, 002460.SZ, 600519.SH, 603019.SH, 600887.SH, 601766.SH, 601668.SH, 601688.SH, 600276.SH, 600690.SH, 000917.SZ, 000100.SZ, 600516.SH, 600036.SH, 300699.SZ, 002415.SZ, 601398.SH, 300059.SZ, 002797.SZ, 000538.SZ, 000858.SZ, 300088.SZ, 600807.SH, 603466.SH, 000868.SZ, 603533.SH, 002594.SZ, 603799.SH, 002340.SZ, 002240.SZ, 600028.SH, 002407.SZ, 601166.SH, 600518.SH, 600585.SH, 601336.SH, 000776.SZ, 601601.SH', 'rt_last,rt_last_amt,rt_last_vol,rt_delta,rt_bidvol,rt_askvol,rt_insti_buy_amt,rt_insti_sell_amt,rt_vip_buy_amt,rt_vip_sell_amt', func = windCallback2)


try:
    while True:
        time.sleep(0)
except KeyboardInterrupt:
    print("\nstopping ...")
    wind.cancelRequest(0)

    fn = f'rtdate_{str(start)}_{str(datetime.now())}.pickle'.replace(':', '')
    pickle.dump(rtdata, open(fn, 'wb'))
    print('data dumped to', fn)

    wind.stop()

print("*** done ***")
