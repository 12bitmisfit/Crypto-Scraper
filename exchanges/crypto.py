import csv
import json
import os
import ssl
import time
from datetime import datetime
import mysql.connector
import psycopg2
import websocket
import requests


class CryptoWeb:

    def __init__(self, config):
        self.config = config
        self.data_pairs = self.generate_data_structure()
        self.HEARTBEAT_METHOD = "public/heartbeat"
        self.SUBSCRIBE_METHOD = "subscribe"
        self.RESPOND_HEARTBEAT_METHOD = "public/respond-heartbeat"
        self.mysql_conn = mysql.connector.connect(
            user=self.config['mysql']['user'],
            password=self.config['mysql']['password'],
            host=self.config['mysql']['host'],
            database=self.config['mysql']['database'],
            raise_on_warnings=False
        )
        self.postgresql_conn = psycopg2.connect(
            dbname=self.config['postgresql']['database'],
            user=self.config['postgresql']['user'],
            password=self.config['postgresql']['password'],
            host=self.config['postgresql']['host'],
            port=self.config['postgresql']['port']
        )
        self.csv_files = self.init_csv_files()
        websocket.enableTrace(config['debugging'])
        self.ws = websocket.WebSocketApp(
            self.config['url'],
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )

    def init_csv_files(self):
        csv_files = {}
        for pair in self.config['trading_pairs']:
            for channel in self.config['channels']:
                directory_path = os.path.join(os.getcwd(), "Crypto", pair)
                os.makedirs(directory_path, exist_ok=True)
                file_name = f"{pair}_{channel}{self.config['csv']['file_suffix']}"
                full_path = os.path.join(directory_path, file_name)
                csv_headers = ['Time_difference', 'Weekday', 'Hour', 'Minute',
                               *self.get_columns(channel)[1:-1].split(', ')]
                csv_file = open(full_path, 'a', newline='')
                csv_writer = csv.writer(csv_file)
                if os.path.getsize(full_path) == 0:
                    csv_writer.writerow(csv_headers)
                csv_files[(pair, channel)] = (csv_file, csv_writer)
        return csv_files

    def post_stats(self, stats):
        requests.post(
            f"http://{self.config['webserver']['url']}:{self.config['webserver']['json_port']}",
            json=stats
        )

    def generate_data_structure(self):
        return {
            pair: {channel: [] for channel in self.config['channels']}
            for pair in self.config['trading_pairs']
        }

    def convert_timestamp_to_readable_date(self, timestamp):
        return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d_%H-%M-%S')

    def get_columns(self, channel):
        columns = {
            'ticker': '(' + ', '.join([
                'Timestamp',
                'Trading_Pair',
                'Trade_Price',
                'Best_Bid',
                'Best_Ask'
            ]) + ')',
            'book': '(' + 'Timestamp, Trading_Pair, ' + ', '.join([
                f"{label}_{i}" for label in [
                    "Asks_Price",
                    "Asks_Volume",
                    "Asks_Orders",
                    "Bids_Price",
                    "Bids_Volume",
                    "Bids_Orders"
                ]
                for i in range(1, 151)
            ]) + ')',
            'candlestick': '(' + ', '.join([
                'Timestamp',
                'Trading_Pair',
                'Open',
                'High',
                'Low',
                'Close',
                'Volume'
            ]) + ')',
            'trade': '(' + ', '.join([
                'Timestamp',
                'Trading_Pair',
                'Sell_Buy',
                'Price',
                'Quantity'
            ]) + ')'
        }

        return columns[channel]

    def save_to_csv(self, data, channel, pair):
        csv_file, csv_writer = self.csv_files[(pair, channel)]
        csv_writer.writerow(data)
        csv_file.flush()

    def _execute_db_commands(self, cursor, data, channel):
        table_name = f"crypto_{channel}_data"
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} {self.get_columns(channel)}"
        cursor.execute(create_table_query)
        placeholders = ', '.join(['%s' for _ in data[0]])
        insert_query = f"INSERT INTO {table_name} {self.get_columns(channel)} VALUES ({placeholders})"
        cursor.execute(insert_query, data)

    def save_to_mysql(self, data, channel):
        with self.mysql_conn.cursor() as cursor:
            self._execute_db_commands(cursor, data, channel)
            self.mysql_conn.commit()

    def save_to_postgresql(self, data, channel):
        with self.postgresql_conn.cursor() as cursor:
            self._execute_db_commands(cursor, data, channel)
            self.postgresql_conn.commit()

    def store_data(self, data):
        pair = data['result']['instrument_name']
        channel = data['result']['channel']
        previous_timestamp = self.data_pairs[pair][channel][-1][0] if self.data_pairs[pair][channel] else None
        current_timestamp = data['result']['data'][0]['t']
        time_difference = (current_timestamp - previous_timestamp) / 1000 if previous_timestamp else None

        if 'ticker' in channel:
            dt = datetime.utcfromtimestamp(data['result']['data'][0]['t'] / 1000)

            self.data_pairs[pair][channel].append([
                data['result']['data'][0]['t'],
                time_difference,
                dt.weekday(),
                dt.hour,
                dt.minute,
                data['result']['instrument_name'],
                data['result']['data'][0]['a'],
                data['result']['data'][0]['b'],
                data['result']['data'][0]['k']
            ])
        elif channel == "book":
            flat_list = [
                item for sublist in
                data['result']['data'][0]['asks'] + data['result']['data'][0]['bids']
                for item in sublist
            ]
            dt = datetime.utcfromtimestamp(data['result']['data'][0]['tt'] / 1000)
            self.data_pairs[pair][channel].append([
                data['result']['data'][0]['tt'],
                time_difference,
                dt.weekday(),
                dt.hour,
                dt.minute,
                data['result']['instrument_name'],
                ] + flat_list
            )
        elif channel == "candlestick":
            dt = datetime.utcfromtimestamp(data['result']['data'][0]['t'] / 1000)
            self.data_pairs[pair][channel].append([
                data['result']['data'][0]['t'],
                time_difference,
                dt.weekday(),
                dt.hour,
                dt.minute,
                data['result']['instrument_name'],
                data['result']['data'][0]['o'],
                data['result']['data'][0]['h'],
                data['result']['data'][0]['l'],
                data['result']['data'][0]['c'],
                data['result']['data'][0]['v']
            ])
        elif channel == "trade":
            sell_or_buy = [0, 1] if data['result']['data'][0]['s'] == "SELL" else [1, 0]
            dt = datetime.utcfromtimestamp(data['result']['data'][0]['t'] / 1000)
            self.data_pairs[pair][channel].append([
                data['result']['data'][0]['t'],
                time_difference,
                dt.weekday(),
                dt.hour,
                dt.minute,
                data['result']['instrument_name'],
                sell_or_buy,
                data['result']['data'][0]['p'],
                data['result']['data'][0]['q']
            ])

            # Save to CSV
            row_data = self.data_pairs[pair][channel][-1]
            self.csv_files[pair][channel].writerow(row_data)

            # Save to MySQL
            self._execute_db_commands(self.mysql_conn.cursor(), [row_data], channel)
            self.mysql_conn.commit()

            # Save to PostgreSQL
            self._execute_db_commands(self.postgresql_conn.cursor(), [row_data], channel)
            self.postgresql_conn.commit()

    def on_message(self, ws, message):
        data = json.loads(message)

        if data["method"] == self.HEARTBEAT_METHOD:
            payload = {
                "id": data["id"],
                "method": self.RESPOND_HEARTBEAT_METHOD
            }
            ws.send(json.dumps(payload))
        else:
            self.store_data(data)
            stats = {
                "results": [
                    {
                        f"{pair}:{channel}": f"{len(self.data_pairs[pair][channel])}/{self.config['csv']['max_length']}"
                    }
                    for pair in self.config['trading_pairs']
                    for channel in self.config['channels']
                ]
            }
            self.post_stats(stats)

    def on_error(self, ws, error):
        comms = {
            "Error": str(error)
        }
        requests.post(
            f"http://{self.config['webserver']['url']}:{self.config['webserver']['json_port']}",
            json=comms
        )

    def close_connections(self):
        self.mysql_conn.close()
        self.postgresql_conn.close()
        for file, writer in self.csv_files.values():
            file.close()

    def on_close(self, ws, close_status_code, close_msg):
        self.close_connections()
        comms = {
            "Info": "Closed"
        }
        requests.post(
            f"http://{self.config['webserver']['url']}:{self.config['webserver']['json_port']}",
            json=comms
        )

    def on_open(self, ws):
        channels = self.config['channels']
        intervals = self.config['intervals']

        subscribes = []
        for channel in channels:
            interval = intervals[channel]
            channel_with_interval = channel + interval if interval else channel
            for pair in self.config['trading_pairs']:
                subscribes.append(f"{channel_with_interval}.{pair}")

        payload = {
            "id": 123454321,
            "method": self.SUBSCRIBE_METHOD,
            "params": {"channels": subscribes}
        }
        ws.send(json.dumps(payload))

        comms = {
            "results": "Subscribed",
            "Channels": self.config['channels'],
            "Trading_pairs": self.config['trading_pairs']
        }
        requests.post(
            f"http://{self.config['webserver']['url']}:{self.config['webserver']['json_port']}",
            json=comms
        )

    def start(self):
        while True:
            try:
                self.ws.run_forever(reconnect=5)
            except ssl.SSLError as e:
                comms = {"results": str(e)}
                requests.post(
                    f"http://{self.config['webserver']['url']}:{self.config['webserver']['json_port']}",
                    json=comms
                )
                time.sleep(5)
            except KeyboardInterrupt:
                break
            except Exception as e:
                comms = {"results": str(e)}
                requests.post(
                    f"http://{self.config['webserver']['url']}:{self.config['webserver']['json_port']}",
                    json=comms
                )
