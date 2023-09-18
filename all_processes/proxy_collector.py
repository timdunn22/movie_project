import asyncio
from proxybroker import Broker
from all_processes.load_yaml_vars import LoadYamlVars
import pandas as pd
from common_methods import flatten
import time
import signal
from multiprocessing import Process, Pool


class KeepData:
    def __init__(self):
        self.data = list()

    def add_data(self, data):
        self.data.append(data)

async def show(proxies, class_object):
    while True:
        proxy = await proxies.get()
        if proxy is None: break
        class_object.add_data({'host': proxy.host, 'port': proxy.port})
        # print(f'class object data has {len(class_object.data)} items')
        print('Found proxy: %s' % proxy)


def get_proxies(limit, class_object):
    proxies = asyncio.Queue()
    broker = Broker(proxies)
    tasks = asyncio.gather(broker.find(types=[( 'HTTP', 'HIGH' ), 'HTTPS'], strict=True, 
                                       limit=limit, timeout=0.5, max_tries=1), 
                                       show(proxies, class_object))
    loop = asyncio.get_event_loop()
    loop.run_until_complete(tasks)
    return class_object.data
    # loop.run_until_complete(tasks)

def gather_proxies(limit):
    try:
        data = list()
        class_object = KeepData()
        with Pool(processes=4) as pool:
            async_object = pool.apply_async(get_proxies, (limit, class_object))
            data = async_object.get(timeout=30)
        return data
    except:
        print('hit internal exception')
        return list()


def main():
    proxy_file = LoadYamlVars(yaml_file_path='/Users/timdunn/movie_project/all_processes/movie_configuration.yaml').proxies_path
    try:
        data = gather_proxies(limit=10)
    except:
        data = list()
    while len(data) < 200:
        try:
            print(f'*********************** data has {len(data)} items')
            data = flatten([data, gather_proxies(limit=10)])
        except:
            print('hit exception')
            pass
    pd.DataFrame(data=data).to_csv(proxy_file, index=False)

if __name__ == '__main__':
    main()