import json
from multiprocessing import Process
import yaml
from http.server import BaseHTTPRequestHandler, HTTPServer
from exchanges import *

# Constants
CONFIG_PATH = "config.yaml"


class WebServer(BaseHTTPRequestHandler):

    def do_POST(self):
        content_length = int(self.headers['Content-Length'])
        post_data = self.rfile.read(content_length)
        data = json.loads(post_data.decode())
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'OK')
        print(data)

    def start(self, config):
        with HTTPServer((config['url'], config['json_port']), WebServer) as server:
            server.serve_forever()


def load_config():
    with open(CONFIG_PATH, 'r') as config_file:
        return yaml.safe_load(config_file)


def prepare_exchange_config(conf):
    exchange_configs = {}
    for exchange, config in conf['exchanges'].items():
        exchange_configs[exchange] = {
            **config,
            'debugging': conf['debugging'],
            'storage': conf['storage']
        }
    return exchange_configs


def launch_exchange(exchange_config, class_name):
    exchange_class = globals()[class_name]
    exchange_instance = exchange_class(exchange_config)
    exchange_instance.start()


def main():
    conf = load_config()  # Your code to load the configuration
    webby = WebServer()
    conf = prepare_exchange_config(conf)

    processes = []
    for exchange_name, exchange_config in conf.items():
        if exchange_config['enabled']:
            class_name = exchange_name.capitalize() + 'Web'
            p = Process(target=launch_exchange, args=(exchange_config, class_name))
            p.start()
            processes.append(p)
    webby.start(conf['webserver'])


if __name__ == "__main__":
    main()
