import websocket
import json
import csv
import rel
import threading
import os

# Define the CSV file name
csv_path = "_data.csv"

# Replace with yaml later
trading_pairs = ["BTC_USD", "LTC_USD", "ETH_USD", "XCH_USD"]
channels = ["ticker", "book", "candlestick", "trade"]

# Generate data structure
data_pairs = {}
for pair in trading_pairs:
    data_pairs[pair] = {}
    for channel in channels:
        data_pairs[pair][channel] = []


def clear_line(n=1):
    line_up = '\033[1A'
    line_clear = '\x1b[2K'
    for i in range(n):
        print(line_up, end=line_clear)


def save_to_csv(data, channel):
    with open("{}_{}".format(data[0][1], channel) + csv_path, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        for row in data:
            csv_writer.writerow(row)


def store_data(data):
    pair = data['result']['instrument_name']
    channel = data['result']['channel']
    global data_pairs
    if 'ticker' in channel:
        data_pairs[pair][channel].append([
            data['result']['data'][0]['t'],
            data['result']['instrument_name'],
            data['result']['data'][0]['a'],
            data['result']['data'][0]['b'],
            data['result']['data'][0]['k']
        ])
    elif channel == "book":
        # Flatten data to store in single row
        flat_list = [
            item for sublist in
            data['result']['data'][0]['asks'] + data['result']['data'][0]['bids']
            for item in sublist
        ]
        # Append data
        data_pairs[pair][channel].append([
            data['result']['data'][0]['tt'],
            data['result']['instrument_name'],
        ])
        for f in flat_list:
            data_pairs[pair][channel][-1].append(f)
    elif channel == "candlestick":
        data_pairs[pair][channel].append([
            data['result']['data'][0]['t'],
            data['result']['instrument_name'],
            data['result']['data'][0]['o'],
            data['result']['data'][0]['h'],
            data['result']['data'][0]['l'],
            data['result']['data'][0]['c'],
            data['result']['data'][0]['v']
        ])
    elif channel == "trade":
        # 0 for sell, 1 for buy
        if data['result']['data'][0]['s'] == "SELL":
            data_pairs[pair][channel].append([
                data['result']['data'][0]['t'],
                data['result']['instrument_name'],
                0,
                data['result']['data'][0]['p'],
                data['result']['data'][0]['q']
            ])
        else:
            data_pairs[pair][channel].append([
                data['result']['data'][0]['t'],
                data['result']['instrument_name'],
                1,
                data['result']['data'][0]['p'],
                data['result']['data'][0]['q']
            ])
    ldp = len(data_pairs[pair][channel])
    # Change ldp check to yaml config too
    if ldp >= 100:
        threading.Thread(target=save_to_csv(data_pairs[pair][channel], channel)).start()
        data_pairs[pair][channel] = []


def on_message(ws, message):
    data = json.loads(message)
    os.system('clear' if os.name == 'posix' else 'cls')
    if data["method"] == "public/heartbeat":
        payload = {
            "id": data["id"],
            "method": "public/respond-heartbeat"
        }
        ws.send(json.dumps(payload))
        print("Heartbeat Sent. ID {}".format(data['id']))
    else:
        store_data(data)
        global data_pairs
        global trading_pairs
        global channels
        for pair in trading_pairs:
            for channel in channels:
                print("{}:{} | {}/100".format(
                    pair,
                    channel,
                    len(data_pairs[pair][channel])
                ))


def on_error(ws, error):
    print(f"Error: {error}")


def on_close(ws, close_status_code, close_msg):
    print("### closed ###")


def on_open(ws):
    global channels
    c = False
    for i in range(len(channels)):
        if 'candlestick' in channels[i]:
            channels[i] = channels[i] + ".1m"
            c = i
    subscribes = [f"{channel}.{pair}" for channel in channels for pair in trading_pairs]
    payload = {
        "id": 123454321,
        "method": "subscribe",
        "params": {
            "channels": subscribes
        }
    }
    ws.send(json.dumps(payload))
    if c:
        channels[c] = channels[c][:-3]
    print("Payload sent")


def generate_csv(trading_pairs, channels):
    for pair in trading_pairs:
        for channel in channels:
            try:
                with open("{}_{}".format(pair, channel) + csv_path, 'r') as file:
                    pass
            except FileNotFoundError:
                with open("{}_{}".format(pair, channel) + csv_path, 'w', newline='') as file:
                    writer = csv.writer(file)
                    if channel == 'ticker':
                        writer.writerow(['Timestamp', 'Trading_Pair', 'Trade_Price', 'Best_Bid', 'Best_Ask'])
                    elif channel == 'book':
                        ar = ['Timestamp', 'Trading_Pair']
                        for i in range(1, 151):
                            ar.append("Asks_Price_{}".format(i))
                            ar.append("Asks_Volume_{}".format(i))
                            ar.append("Asks_Orders_{}".format(i))
                        for i in range(1, 151):
                            print(i)
                            ar.append("Bids_Price_{}".format(i))
                            ar.append("Bids_Volume_{}".format(i))
                            ar.append("Bids_Orders_{}".format(i))
                        writer.writerow(ar)
                    elif channel == 'candlestick':
                        writer.writerow(['Timestamp', 'Trading_Pair', 'Open', 'High', 'Low', 'Close', 'Volume'])
                    elif channel == 'trade':
                        writer.writerow(['Timestamp', 'Trading_Pair', 'Sell_Buy', 'Price', 'Quantity'])


if __name__ == "__main__":
    generate_csv(trading_pairs, channels)
    websocket.enableTrace(False)
    ws = websocket.WebSocketApp(
        "wss://stream.crypto.com/v2/market",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    # Attempt to reconnect after 5 seconds if session closed unexpectedly 
    ws.run_forever(dispatcher=rel, reconnect=5)
    rel.signal(2, rel.abort)  # Keyboard Interrupt
    rel.dispatch()
