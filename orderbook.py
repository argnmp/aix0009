import sys
from enum import Enum
import urllib3
import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta

from requests.adapters import HTTPAdapter, Retry

ob_acc = pd.DataFrame(columns=["price", "quantity", "type", "timestamp"]);

class EXCHANGE(Enum):
    BITHUMB = 1
    UPBIT = 2
class ORDER(Enum):
    BTC = 1
    ETH = 2
class PAYMENT(Enum):
    KRW = 1

class Env():
    def __init__(self):
        self.data = {}
    def set_str(self, key, value):
        self.data[str(key)] = str(value)
    def set(self, key, value):
        self.data[str(key)] = value
    def get(self, key):
        return self.data[str(key)]

class Tool():
    def target_url():
        if env.get("exchange") is EXCHANGE.BITHUMB:
            return "https://api.bithumb.com/public/orderbook/%s_%s/?count=%s" % (env.get("order").name, env.get("payment").name, env.get("count"))
        
        # print(env.get("exchange"))
        return ""

    def target_csv_file():
        cur_time = datetime.now()
        return "book-%s-%s-%s" % (cur_time.date(), env.get("exchange").name.lower(), env.get("order").name.lower())

def create_session():
    session = requests.Session()
    retry = Retry(connect=1, backoff_factor=0.1)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    # session.mount('https://', adapter)
    return session
        

def fetch_order_book(url, cur_time):
    try:
        ob_res = env.get("session").get(url, headers={ 'User-Agent': 'Mozilla/5.0' }, verify=False, timeout=1)
        if ob_res is None or not ob_res:
            raise Exception("empty body")
    except Exception as e:
        raise e
    except :
        raise Exception("fetch error")

    ob_data = ob_res.json()['data']
    # cur_time = datetime.fromtimestamp(int(ob_data['timestamp'])/1000.0, timezone(timedelta(hours=9)))
    # cur_time = datetime.now()
    cur_time.strftime('%Y-%m-%d %H:%M:%S.%f')
    bids = pd.DataFrame(ob_data['bids'])
    bids.sort_values('price', ascending=False)
    bids['type'] = 0;
    asks = pd.DataFrame(ob_data['asks'])
    asks.sort_values('price', ascending=True)
    asks['type'] = 1;

    df = pd.concat([bids, asks], ignore_index=True)
    df['timestamp'] = cur_time 
    
    return df

def write_order_book(filename, ob):
    if os.path.isfile(filename):
        ob.to_csv(filename, sep="|", index=False, header=False, mode='a')
    else:
        ob.to_csv(filename, sep="|", index=False, header=True, mode='a')
        

def collect_loop():
    base_time = datetime.now()
    while True:
        # start_time = datetime.now()
        # if((start_time-prev_time).total_seconds() < 1.0):
        #     continue 
        # prev_time = start_time

        start_time = datetime.now();
        sleep_weight = timedelta(microseconds=start_time.microsecond - base_time.microsecond);
        print(base_time.microsecond, start_time.microsecond, sleep_weight.total_seconds())

        try:
            order_book = fetch_order_book(Tool.target_url(), start_time)
        except Exception as e:
            print("[dbg] exception:", e)
            continue

        print(order_book)
        write_order_book(Tool.target_csv_file(), order_book) 
        end_time = datetime.now();
        
        sleep_duration = env.get("interval") - (end_time-start_time).total_seconds() - sleep_weight.total_seconds()
        if(sleep_duration <= 0):
            print("[dbg] processing time exceeded interval", sleep_duration, datetime.now())
            base_time = datetime.now()
            continue
        
        time.sleep(sleep_duration) 

def arg_verify(argv):
    if len(argv) != 6:
        print("Usage: python3 %s [exchange] [order] [payment] [count] [interval]" % argv[0])
        raise Exception("argument parse error")
        
def main(argv):
    arg_verify(argv)
    
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    global env;
    env = Env()
    global tool;
    tool = Tool() 
    
    env.set("session", create_session())
    env.set("exchange", EXCHANGE[argv[1]])
    env.set("order", ORDER[argv[2]])
    env.set("payment", PAYMENT[argv[3]])
    env.set("count", argv[4])
    env.set("interval", int(argv[5]))

    collect_loop()
    
if __name__ == "__main__":
    main(sys.argv)
