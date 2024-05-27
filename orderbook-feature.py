from enum import Enum
import sys
import os
import pandas as pd

class Env():
    def __init__(self):
        self.data = {}
    def set_str(self, key, value):
        self.data[str(key)] = str(value)
    def set(self, key, value):
        self.data[str(key)] = value
    def get(self, key):
        return self.data[str(key)]

class MID_TYPE(Enum):
    DEFAULT = 1
    WT = 2
    MKT = 3

def read_book():
    df = pd.read_csv(env.get("book_path"), sep="|")
    df['price'].apply(pd.to_numeric)
    df['quantity'].apply(pd.to_numeric)
    df['type'].apply(pd.to_numeric)
    return df

def write_feature(df):
    df.to_csv(env.get("feature_fn"), sep='|', index=False, header=True, mode='a')


def calc_mid_feature(bid_df, ask_df, mid_type):
    bid_top_price = None 
    bid_top_qty = None
    for i in range(0, 5):
        if bid_df.iloc[i].quantity > 0:
            bid_top_price = bid_df.iloc[i].price
            bid_top_qty = bid_df.iloc[i].quantity
            break
        
    ask_top_price = None
    ask_top_qty = None
    for i in range(0, 5):
        if ask_df.iloc[i].quantity > 0:
            ask_top_price = ask_df.iloc[i].price
            ask_top_qty = ask_df.iloc[i].quantity
            break
    if bid_top_qty is None or ask_top_qty is None:
        raise Exception("bid or ask quantity is all zero")
    
    if mid_type is MID_TYPE.DEFAULT:
        return (bid_top_price + ask_top_price) * 0.5
    elif mid_type is MID_TYPE.WT:
        return (bid_df['price'].mean() + ask_df['price'].mean()) * 0.5
    elif mid_type is MID_TYPE.MKT:
        return ((bid_top_price*ask_top_qty) + (ask_top_price*bid_top_qty))/(bid_top_qty+ask_top_qty)

def calc_book_imbalance(bid_df, ask_df, ratio, interval, mid_price):
    bid_quantity_df = bid_df.quantity ** ratio
    bid_price_df = bid_df.price * bid_quantity_df

    ask_quantity_df = ask_df.quantity ** ratio
    ask_price_df = ask_df.price * ask_quantity_df

    ask_qty = ask_quantity_df.values.sum()
    bid_px = bid_price_df.values.sum()
    bid_qty = bid_quantity_df.values.sum()
    ask_px = ask_price_df.values.sum()

    if bid_qty == 0.0 or ask_qty == 0.0:
        raise Exception("bid or ask quantity is all zero")
    
    book_price = (((ask_qty*bid_px)/bid_qty) + ((bid_qty*ask_px)/ask_qty)) / (bid_qty+ask_qty)
    
    book_imbalance = (book_price - mid_price) / interval
    return book_imbalance

def calc_order_flow_imbalance(prev_bid_df, prev_ask_df, bid_df, ask_df):
    # if previous orderbook does not exist, return 0
    if prev_bid_df is None or prev_ask_df is None:
        return (0,0,0)

    # best bid
    # (price, quantity)
    prev_best_bid = None
    best_bid = None
    prev_best_ask = None
    best_ask = None

    for i in range(0, 5):
        if prev_bid_df.iloc[i].quantity > 0:
            prev_best_bid = (prev_bid_df.iloc[i].price, prev_bid_df.iloc[i].quantity)
            break
    for i in range(0, 5):
        if bid_df.iloc[i].quantity > 0:
            best_bid = (bid_df.iloc[i].price, bid_df.iloc[i].quantity)
            break
    for i in range(0, 5):
        if prev_ask_df.iloc[i].quantity > 0:
            prev_best_ask = (prev_ask_df.iloc[i].price, prev_ask_df.iloc[i].quantity)
            break
    for i in range(0, 5):
        if ask_df.iloc[i].quantity > 0:
            best_ask = (ask_df.iloc[i].price, ask_df.iloc[i].quantity)
            break

    bid_volume_delta = None
    ask_volume_delta = None
    if best_bid[0] > prev_best_bid[0]:
        bid_volume_delta = best_bid[1]
    elif best_bid[0] == prev_best_bid[0]:
        bid_volume_delta = best_bid[1] - prev_best_bid[1]
    else:
        bid_volume_delta = -prev_best_bid[1]

    if best_ask[0] > prev_best_ask[0]:
        ask_volume_delta = best_ask[1]
    elif best_ask[0] == prev_best_ask[0]:
        ask_volume_delta = best_ask[1] - prev_best_ask[1]
    else:
        ask_volume_delta = -prev_best_ask[1]

    ofi = bid_volume_delta - ask_volume_delta

    return (bid_volume_delta, ask_volume_delta, ofi)
    
    

def arg_set(argv):
    if len(argv) != 2:
        print("Usage: python3 %s [orderbook file path]" % argv[0])
        raise Exception("argument parse error")
    env.set("book_path", str(argv[1]))
    book_fn, book_ext = os.path.splitext(os.path.basename(env.get("book_path")))
    book_param = book_fn.split("-")
    if book_ext != '.csv' or len(book_param) != 6 or book_param[0] != 'book':
        raise Exception("orderbook file name incorrect")
    env.set("feature_fn", "%s-%s-%s-%s-%s-feature.csv" % (book_param[1], book_param[2], book_param[3], book_param[4], book_param[5]))

def main(argv):
    global env;
    env = Env()
    arg_set(argv)
    
    book_df = read_book()
    book_df_grp = book_df.groupby('timestamp')

    feature_df = pd.DataFrame(columns=[
        'midprice',
        'midprice_wt', 
        'midprice_mkt',
        'book-imbalance-0.3-5-1',
        'book-imbalance-0.3-5-1-wt',
        'book-imbalance-0.3-5-1-mkt',
        'book-imbalance-0.5-5-1',
        'book-imbalance-0.5-5-1-wt',
        'book-imbalance-0.5-5-1-mkt',
        'book-imbalance-0.7-5-1',
        'book-imbalance-0.7-5-1-wt',
        'book-imbalance-0.7-5-1-mkt',
        'bid_volume_delta',
        'ask_volume_delta',
        'order_flow_imbalance',
        'timestamp'])

    prev_bid_df = None
    prev_ask_df = None
    for (timestamp, partial_book) in book_df_grp:
        type_df = [None, None]
        type_grp = partial_book.groupby("type")
        for (type, type_book) in type_grp:
            type_df[type] = type_book

        bid_df = type_df[0]
        ask_df = type_df[1]
        bid_df.sort_values("price", ascending=False)
        ask_df.sort_values("price", ascending=True)

        midprice = calc_mid_feature(bid_df, ask_df, MID_TYPE.DEFAULT)
        midprice_wt = calc_mid_feature(bid_df, ask_df, MID_TYPE.WT)
        midprice_mkt = calc_mid_feature(bid_df, ask_df, MID_TYPE.MKT)
        row = []
        row.append(midprice)
        row.append(midprice_wt)
        row.append(midprice_mkt)
        row.append(calc_book_imbalance(bid_df, ask_df, 0.3, 1, midprice))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.3, 1, midprice_wt))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.3, 1, midprice_mkt))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.6, 1, midprice))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.6, 1, midprice_wt))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.6, 1, midprice_mkt))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.9, 1, midprice))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.9, 1, midprice_wt))
        row.append(calc_book_imbalance(bid_df, ask_df, 0.9, 1, midprice_mkt))
        
        (bvd, avd, ofi) = calc_order_flow_imbalance(prev_bid_df, prev_ask_df, bid_df, ask_df)
        row.append(bvd)
        row.append(avd)
        row.append(ofi)
        row.append(timestamp)

        feature_df.loc[len(feature_df)] = row
        
        prev_bid_df = bid_df
        prev_ask_df = ask_df

    print(feature_df)
    write_feature(feature_df)
    
if __name__ == "__main__":
    main(sys.argv)
