import gzip
import struct
import os
from datetime import timedelta
from collections import defaultdict
import csv


def first_three(msg):
    stock_locate = msg[0]
    tracking_number = msg[1]
    time_ns = convert_time(msg[2])
    return stock_locate, tracking_number, time_ns
def system_event_message(msg):
    time_ns = convert_time(msg[2])
    event_code = msg[3]
    return time_ns, event_code

# Stock Related Message
# msg_type == 'R'
def stock_directory(msg):
    stock_symbol = msg[3].decode('ascii').strip()
    shares = msg[6]
    return stock_symbol, shares

# msg_type = 'H'
def stock_trading_action(msg):
    stock_symbol = msg[3].decode('ascii').strip()
    trading_state = msg[4]
    return stock_symbol, trading_state

# Add order message

# No MPID
# msg_type = 'A'
def add_order_no_mpid(msg):
    reference = msg[3]
    buy_sell_indicator = msg[4]
    shares = msg[5]
    stock_symbol = msg[6].decode('ascii').strip()
    price = msg[7]/10000
    return reference, buy_sell_indicator, shares, stock_symbol, price


# With MPID
# msg_type = 'F'
def add_order_with_mpid(msg):
    reference = msg[3]
    buy_sell_indicator = msg[4]
    shares = msg[5]
    stock_symbol = msg[6].decode('ascii').strip()
    price = msg[7]/10000
    return reference, buy_sell_indicator, shares, stock_symbol, price


# Modify Order Message
# msg_type = 'E'
def order_executed_message(msg):
    reference = msg[3]
    shares = msg[4]
    match_number = msg[5]
    return reference, shares, match_number

# msg_type = 'C'
def order_executed_with_price_message(msg):
    reference = msg[3]
    shares = msg[4]
    match_number = msg[5]
    is_printable = msg[6]
    execution_price = msg[7]/10000
    return reference, shares, match_number, is_printable, execution_price
# msg_type = 'X'
def order_cancel_message(msg):
    reference = msg[3]
    cancelled_shares = msg[4]
    return (reference, cancelled_shares)

# msg_type = 'D'
def order_delete_message(msg):
    reference = msg[3]
    return reference

# msg_type = 'U'
def order_replace_message(msg):
    old_reference = msg[3]
    new_reference = msg[4]
    shares = msg[5]
    price = msg[6]/10000
    return old_reference, new_reference, shares, price


# Trade Messages
# msg_type = 'P'
def trade_message(msg):
    reference = msg[3]
    buy_sell_indicator = msg[4]
    shares = msg[5]
    stock_symbol = msg[6].decode('ascii').strip()
    price = msg[7]/10000
    match_number = msg[8]
    return reference, buy_sell_indicator, shares, stock_symbol, price, match_number

# msg_type = 'Q'
def cross_trade_message(msg):
    shares = msg[3]
    stock_symbol = msg[4].decode('ascii').strip()
    cross_price = msg[5]/10000
    match_number = msg[6]
    return shares, stock_symbol, cross_price, match_number

# msg_type = 'B'
def broken_trade_execution_message(msg):
    match_number = msg[3]
    return match_number

# Net Order Imbalance Message (NOII)
# msg_type = 'I'
def noii_message(msg):
    paired_shares = msg[3]
    imbalance = msg[4]
    imbalance_dir = msg[5]
    stock_symbol = msg[6].decode('ascii')
    far_price = msg[7]/10000
    near_price = msg[8]/10000
    current_reference_price = msg[9]/10000
    cross_type = msg[10]
    return paired_shares, imbalance, imbalance_dir, stock_symbol, far_price, near_price, current_reference_price,cross_type







# Utility methods
def convert_time(timestamp):
    temp_time = struct.unpack('!Q',b'\x00\x00' + timestamp)
    seconds = temp_time[0] * 1e-9
    hours = seconds/3600
    return hours
def hour(timestamp):
    temp_time = struct.unpack('!Q',b'\x00\x00' + timestamp)
    x = '{0}'.format(timedelta(seconds=temp_time[0]*1e-9))
    hr = int(x.split(':')[0])
    return hr

# Market timings
open_time = 0
close_time = 0

# Data Structures
order_book = dict() # reference -> (stock_symbol, shares, price)
stock_map = dict() # stock_symbol -> (msg_type, time, reference, price, shares)
exe_orders = dict() # match_number -> (msg_type, time, reference, stock_symbol)



# Message Parsing method
def parse(file):
    global open_time
    global close_time
    f = gzip.open(file, 'rb')
    msg_type = f.read(1)
    mb = 1024*1024
    freq = 100*1024*1024
    total_bytes, running_bytes = 0,0

    cnt = 0
    while msg_type:
        msg_len = 0
        if(running_bytes > freq):
            print('%d MB data parsed' % (total_bytes/mb))
            running_bytes = running_bytes/freq
        if msg_type == b'S':
            msg_len = 11
            msg = struct.unpack('!HH6sc', f.read(msg_len))
            if msg[3] == b'S':
                open_time = hour(msg[2])
            elif msg[3] == b'M':
                close_time = hour(msg[2])
                break
        elif msg_type == b'R':
            msg_len = 38
            msg = struct.unpack('!HH6s8sccIcc2scccccIc',f.read(msg_len))
        elif msg_type == b'H':
            msg_len = 24
            msg = struct.unpack('!HH6s8scc4s',f.read(msg_len))
        elif msg_type == b'Y':
            msg_len = 19
            msg = struct.unpack('!HH6s8sc',f.read(msg_len))
        elif msg_type == b'L':
            msg_len = 25
            msg = struct.unpack('!HH6s4s8sccc',f.read(msg_len))
        elif msg_type == b'V':
            msg_len = 34
            msg = struct.unpack('!HH6sQQQ',f.read(msg_len))
        elif msg_type == b'W':
            msg_len = 11
            msg = struct.unpack('!HH6sc',f.read(msg_len))
        elif msg_type == b'K':
            msg_len = 27
            msg = struct.unpack('!HH6s8sIcL',f.read(msg_len))
        elif msg_type == b'J':
            msg_len = 34
            msg = struct.unpack('!HH6s8sLLLI',f.read(msg_len))
        elif msg_type == b'h':
            msg_len = 20
            msg = struct.unpack('!HH6s8scc',f.read(msg_len))
        elif msg_type == b'A':
            msg_len = 35
            msg = struct.unpack('!HH6sQcI8sL',f.read(msg_len))
            reference, buy_sell_indicator, shares, stock_symbol, price = add_order_no_mpid(msg)
            if buy_sell_indicator == b'B':
                order_book[reference] = (stock_symbol, shares, price)
        elif msg_type == b'F':
            msg_len = 39
            msg = struct.unpack('!HH6sQcI8sL4s',f.read(msg_len))
            reference, buy_sell_indicator, shares, stock_symbol, price = add_order_with_mpid(msg)
            if buy_sell_indicator == b'B':
                order_book[reference] = (stock_symbol, shares, price)
        elif msg_type == b'E':
            msg_len = 30
            msg = struct.unpack('!HH6sQIQ',f.read(msg_len))
            reference, shares, match_number = order_executed_message(msg)
            time = hour(msg[2])
            if reference in order_book:
                stock_symbol, share_vol, share_price = order_book[reference]
                if stock_symbol not in stock_map:
                    stock_map[stock_symbol] = [(msg_type, time, reference, shares, share_price)]
                else:
                    stock_list = stock_map[stock_symbol]
                    stock_list.append((msg_type, time, reference, shares, share_price))
                    stock_map[stock_symbol] = stock_list
                exe_orders[match_number] = (msg_type, time, reference, stock_symbol)
                tot_shares = share_vol-shares
                if tot_shares <= 0:
                    tot_shares = 0
                    del order_book[reference]
                    pass
                order_book[reference] = (stock_symbol, tot_shares, price)
        elif msg_type == b'C':
            msg_len = 35
            msg = struct.unpack('!HH6sQIQcL',f.read(msg_len))
            reference, shares, match_number, is_printable, execution_price = order_executed_with_price_message(msg)
            time = hour(msg[2])
            if reference in order_book.keys():
                stock_symbol, share_vol, share_price = order_book[reference]
                if stock_symbol not in stock_map:
                    stock_map[stock_symbol] = [(msg_type, time, reference, shares, execution_price)]
                else:
                    stock_list = stock_map[stock_symbol]
                    stock_list.append((msg_type, time, reference, shares, execution_price))
                    stock_map[stock_symbol] = stock_list
                exe_orders[match_number] = (msg_type, time, reference, stock_symbol)
                tot_shares = share_vol - shares
                if tot_shares <= 0:
                    tot_shares = 0
                    del order_book[reference]
                    pass
                order_book[reference] = (stock_symbol, tot_shares, price)
        elif msg_type == b'X':
            msg_len = 22
            msg = struct.unpack('!HH6sQI',f.read(msg_len))
            reference, cancelled_shares = order_cancel_message(msg)
            if reference in order_book:
                stock_symbol_temp, temp_shares, price_temp = order_book[reference]
                tot_shares = temp_shares - cancelled_shares
                if tot_shares <= 0:
                    tot_shares = 0
                    del order_book[reference]
                    pass
                else:
                    order_book[reference] = (stock_symbol_temp, tot_shares, price_temp)
        elif msg_type == b'D':
            msg_len = 18
            msg = struct.unpack('!HH6sQ',f.read(msg_len))
            reference = order_delete_message(msg)
            if reference in order_book.keys():
                del order_book[reference]
        elif msg_type == b'U':
            msg_len = 34
            msg = struct.unpack('!HH6sQQIL',f.read(msg_len))
            old_reference, new_reference, shares, price = order_replace_message(msg)
            if old_reference in order_book.keys():
                stock_symbol, old_shares, old_price = order_book[old_reference]
                del order_book[old_reference]
                order_book[new_reference] = (stock_symbol, shares, price)
        elif msg_type == b'P':
            msg_len = 43
            msg = struct.unpack('>HH6sQsI8sIQ',f.read(msg_len))
            reference, buy_sell_indicator, shares, stock_symbol, price, match_number = trade_message(msg)
            time = hour(msg[2])
            if stock_symbol not in stock_map:
                stock_map[stock_symbol] = [(msg_type, time, reference, shares, price)]
            else:
                stock_list = stock_map[stock_symbol]
                stock_list.append((msg_type, time, reference, shares, price))
                stock_map[stock_symbol] = stock_list
            exe_orders[match_number] = (msg_type, time, reference, stock_symbol)

        elif msg_type == b'Q':
            msg_len = 39
            msg = struct.unpack('!HH6sQ8sLQc',f.read(msg_len))
            shares, stock_symbol, cross_price, match_number = cross_trade_message(msg)
            time = hour(msg[2])
            if shares == 0:
                continue
            elif stock_symbol not in stock_map:
                stock_map[stock_symbol] = [(msg_type, time, match_number, shares, cross_price)]
            else:
                stock_list = stock_map[stock_symbol]
                stock_list.append((msg_type, time, match_number, shares, cross_price))
                stock_map[stock_symbol] = stock_list
            exe_orders[match_number] = (msg_type, time, match_number, stock_symbol)
        elif msg_type == b'B':
            msg_len = 18
            msg = struct.unpack('!HH6sQ',f.read(msg_len))
            match_number = broken_trade_execution_message(msg)
            msg_type, time, reference, stock_symbol = exe_orders[match_number]
            if stock_symbol in stock_map:
                stock_list = stock_map[stock_symbol]
                for index, trade in enumerate(stock_list):
                    if trade[0] == msg_type and trade[2] == match_number:
                        del stock_list[index]
                        break
                stock_map[stock_symbol] = stock_list
        elif msg_type == b'I':
            msg_len = 49
            msg = struct.unpack('!HH6sQQc8sLLLcc',f.read(msg_len))
        total_bytes += msg_len
        running_bytes += msg_len
        msg_type = f.read(1)
        total_bytes += 1
        running_bytes += 1



def calculate_hourly_vwap(i_stock_map):
    hourly_data = defaultdict(lambda: defaultdict(lambda: {'volume': 0, 'price': 0}))

    # Aggregate data by hour and stock symbol
    for stock_symbol, trades in i_stock_map.items():
        for trade in trades:
            msg_type, time, reference, shares, price = trade
            hour = time
            hourly_data[stock_symbol][hour]['volume'] += shares
            hourly_data[stock_symbol][hour]['price'] += price * shares

    # Calculate VWAP
    vwap_data = []
    for stock_symbol, hours in hourly_data.items():
        for hour, data in hours.items():
            if data['volume'] > 0:
                vwap = data['price'] / data['volume']
                vwap_data.append((stock_symbol, hour, vwap, data['volume']))

    return vwap_data

def write_vwap_to_file(vwap_data, output_dir, filename="hourly_vwap.csv"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    file_path = os.path.join(output_dir, filename)
    with open(file_path, 'w', newline='') as csvfile:
        fieldnames = ['Stock Symbol', 'Hour', 'VWAP', 'Volume']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        writer.writeheader()
        for stock_symbol, hour, vwap, volume in vwap_data:
            writer.writerow({
                'Stock Symbol': stock_symbol,
                'Hour': hour,
                'VWAP': vwap,
                'Volume': volume
            })

#input file
input_filename = os.path.join('..', 'datafile', '01302019.NASDAQ_ITCH50.gz')

#output directory
output_dir = os.path.join('..', 'output')

# Parse data to create stock_map
parse(input_filename)

# Calculate VWAP
vwap_data = calculate_hourly_vwap(stock_map)

# # Write to file
write_vwap_to_file(vwap_data, output_dir)
