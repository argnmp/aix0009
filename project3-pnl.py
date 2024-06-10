import sys
import pandas as pd

def main(argv):
    df = pd.read_csv("ai-crypto-project-3-live-btc-krw.csv")
    df['quantity'].apply(pd.to_numeric)
    df['price'].apply(pd.to_numeric)
    df['fee'].apply(pd.to_numeric)
    df['amount'].apply(pd.to_numeric)
    df['side'].apply(pd.to_numeric)
    
    qty_threshold = 1000 / df['price'].mean()
    
    pnl_acc = 0
    pnl_acc_last_timestamp = ""
    pnl = 0
    pnl_sup = 0

    qty_acc = 0
    price_current = 0

    for (_, row) in df.iterrows():
        if row['side'] == 0:
            qty_acc += row['quantity']
        else:
            qty_acc -= row['quantity']

        pnl += row['amount']
        price_current = row['price']
            
        if -qty_threshold <= qty_acc and qty_acc <= qty_threshold :
            # assume that we bid or ask remaining quantity in current price
            pnl_acc += pnl
            pnl = 0
            pnl_sup += row['price'] * qty_acc
            qty_acc = 0
            pnl_acc_last_timestamp = row['timestamp']

    print("PnL until ", pnl_acc_last_timestamp, ": ", pnl_acc + pnl_sup, sep="")
    pnl_sup += price_current * qty_acc
    print("left quantity: ", qty_acc, sep="")
    print("PnL after selling left quantity in current price: ", pnl_acc + pnl + pnl_sup)
    
if __name__ == "__main__":
    main(sys.argv)
