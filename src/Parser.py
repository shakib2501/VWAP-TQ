import gzip
import os
import struct
import datetime
import pandas as pd

class Parser:
    def __init__(self, file):
        self.all_trades = dict()
        self.filename = gzip.open(file, 'rb')

    def parseTime(self, timestamp):
        return struct.unpack('>Q', struct.pack('2s6s', b'\x00\x00', timestamp))[0];

    def calculate_trading_hour(self, timestamp):
        time = datetime.datetime.fromtimestamp(timestamp / 1e9).strftime('%H:%M:%S')
        return time.hour

    def initialize(self, stock_ids):
        ns_perhour = (60*60)*(10**9)
        for stock_id in stock_ids:
            hourly_trades = dict()
            for hr in range(10, 17):  # market is opened from 1000Hrs to 1600Hrs
                hourly_trades[hr] = (0,0)
            self.all_trades[stock_id] = hourly_trades

    def parse_total_orders(self, total_orders, close_time):
        for order in total_orders:
            stock_id, time, price , quantity = order.split(',')
            hr = self.calculate_trading_hour(close_time)
            value = price*quantity
            self.all_trades[stock_id][hr] == (value,quantity)


    def messageEventMap(self):
        msg_type = {'S':11,'R':38,'H':24,'Y':19,'L':25,'V':34, 'W':11, 'K':27,'J':34,'h':20, 'A':35, 'F':39, 'E':30, 'C':35,'X':22, 'D':18, 'U':34, 'P':43, 'Q':39,'B':18,'I':49,'N':19}
        return msg_type


    def parseTrade(self):
        open_time = 0
        close_time = 0
        stocks = dict()
        total_orders = []
        orders = dict()
        msg_type = self.filename.read(1)
        # print('msg_type', msg_type)
        # print(type(msg_type))
        market_opened = False
        me_map = self.messageEventMap()
        # me_map = self.messageMap()

        mb = 1024*1024
        freq = 100*1024*1024
        total_bytes = 0
        running_bytes = 0
        cnt = 0
        while msg_type:
            msg_type = msg_type.decode()
            # print(cnt)
            if(running_bytes > freq):
                print("%d MB data parsed " % (total_bytes/mb))
                running_bytes = running_bytes/freq
            if msg_type in me_map.keys():
                tot_offset = me_map[msg_type]
                msg = self.filename.read(tot_offset)
                total_bytes += tot_offset
                running_bytes += tot_offset
                if msg_type == "S":
                    data = struct.unpack('>HH6sc', msg)
                    if(data[3].decode() == 'Q'):
                        open_time = self.parseTime(data[2])
                        market_opened = True
                    elif data[3].decode() == "M":
                        close_time = self.parseTime(data[2])
                        break
                elif msg_type == 'R':
                    data = struct.unpack('>HH6s8sccIcc2scccccIc', msg)
                    stock_id = data[0]
                    stocks[stock_id] = data[3].decode().strip()
                elif msg_type == 'A':
                    data = struct.unpack('>HH6sQcI8sI', msg)
                    reference = data[3]
                    orders[reference] = data[7]/(10**4)
                    # orders[reference] = price
                elif msg_type == 'F':
                    data = struct.unpack('>HH6sQcI8sI4s', msg)
                    reference = data[3]
                    price = data[7]/(10**4)
                    orders[reference] = price
                elif msg_type == 'U':
                    data = struct.unpack('>HH6sQQII', msg)
                    reference = data[4]
                    price = data[6]/(10**4)
                    orders[reference] = price
                elif msg_type == 'E' and market_opened:
                    data = struct.unpack('>HH6sQIQ', msg)
                    stock_id = data[0]
                    time = self.parseTime(data[2])
                    reference = data[3]
                    price = orders[reference]
                    no_of_shares = data[4]
                    total_orders.append([stock_id, time, price, no_of_shares])
                elif msg_type == 'C' and market_opened:
                    data = struct.unpack('>HH6sQIQcI', msg)
                    is_printable = data[6].decode().strip()
                    if(is_printable == 'Y'):
                        stock_id = data[0]
                        time = self.parseTime(data[2])
                        no_of_shares = data[4]
                        price = data[7]/(10**4)
                        total_orders.append([stock_id, time, price, no_of_shares])
                elif msg_type == 'P' and market_opened:
                    data = struct.unpack('>HH6sQcIQIQ', msg)
                    stock_id = data[0]
                    time = self.parseTime(data[2])
                    no_of_shares = data[4]
                    price = data[7]/(10**4)
                    total_orders.append([stock_id, time, price, no_of_shares])
            msg_type = self.filename.read(1)
            total_bytes += 1
            running_bytes += 1
            # cnt += 1
        return stocks, total_orders, close_time


    def VWAP(self, trade, c_val=0, c_qty=0):
        trade_price, trade_quantity = trade
        trade_price += c_val
        trade_quantity += c_qty
        if trade_quantity != 0:
            average_price = trade_price / trade_quantity
        else:
            average_price = 0
        return c_val, c_qty, average_price

    def calculate_vwap(self):
        stock_map, total_orders, close_time = self.parseTrade()
        self.initialize(stock_map.keys())
        self.parse_total_orders(total_orders, close_time)
        output = pd.DataFrame()
        VWAP_value_map = dict()
        stocks = []
        for n in range(10, 17):
            VWAP_value_map[n] = []

        for ids in self.all_trades.keys():
            stocks.append(stock_map[ids])
            current_value = 0
            current_quantity = 0
            for n in range(10, 17):
                current_value, current_quantity, vwap = self.VWAP(total_orders[ids], current_value, current_quantity)
                VWAP_value_map[n].append(vwap)
        output['Stock'] = stocks
        for n in range(10, 17):
            vwaps = VWAP_value_map[n]
            output["%s Running VWAP" % n] = vwaps

        output.to_csv(os.path.join('..', 'output', 'output_vwap.csv'), index=False)



if __name__ == '__main__':
    # data = gzip.open(os.path.join('..', 'datafile', '01302019.NASDAQ_ITCH50.gz'), 'rb')
    filename = os.path.join('..', 'datafile', '01302019.NASDAQ_ITCH50.gz')
    parser = Parser(filename)
    parser.calculate_vwap()
