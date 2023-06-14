# import pandas as pd
# import asyncio
# from common_methods import sample, BeautifulSoup, requests
# from proxybroker import Broker

# class ProxyCollector:

#     def __init__(self, df_path, amount):
#         self.amount = amount
#         self.tasks = None
#         self.proxies = asyncio.Queue()
#         self.broker = Broker(self.proxies)
#         self.working_proxies = list()
#         self.verified_proxies = list()
#         self.df = pd.read_csv(df_path, nrows=100)

#     async def find_proxies(self, proxies):
#         while True:
#             proxy = await proxies.get()
#             if proxy is None: break
#             self.working_proxies.append(proxy)
#             print('Found proxy: %s' % proxy)

#     async def find_all_proxies(self):
#         return await asyncio.gather(self.broker.find(types=['HTTPS'], limit=self.amount), 
#                                     self.find_proxies(self.proxies))

#     def double_verify(self):
#         for proxy in self.working_proxies:
#             try:
#                 indexes = sample(list(self.df.index), 3)
#                 for index, row in self.df.loc[indexes].iterrows():
#                     request_obj = requests.get(row['link_url'], proxies={'http': f'http://{proxy.host}'})
#                     BeautifulSoup(request_obj.content.decode('utf-8', 'ignore'))
#                 self.verified_proxies.append(proxy.host)
#             except:
#                 pass

#     async def find_verify(self):
#         # try:
#         #     asyncio.get_event_loop().stop()
#         # except:
#         #     pass
#         await self.find_all_proxies()
#         self.double_verify()
#         return self.verified_proxies

# async def get_verify_proxies(df_path, amount):
#     return await ProxyCollector(df_path, amount).find_verify()

# async def show(working_proxies, proxies):
#     while True:
#         proxy = await proxies.get()
#         if proxy is None: break
#         working_proxies.append(proxy)
#         print('Found proxy: %s' % proxy)

# def get_proxies(amount):

#     working_proxies = list()
#     proxies = asyncio.Queue()
#     broker = Broker(proxies)
#     tasks = asyncio.gather(
#         broker.find(types=['HTTPS'], limit=amount),
#         show(working_proxies, proxies))
#     loop = asyncio.get_event_loop()
#     return broker, tasks, loop
import asyncio
from proxybroker import Broker
from all_processes.load_yaml_vars import LoadYamlVars
import pandas as pd


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
        print('Found proxy: %s' % proxy)



def main():
    proxy_file = LoadYamlVars(yaml_file_path='/Users/timdunn/movie_project/all_processes/movie_configuration.yaml').proxies_path
    proxies = asyncio.Queue()
    broker = Broker(proxies)
    data = list()
    class_object = KeepData()
    # Check proxy in spam databases (DNSBL). By default is disabled.
    # more databases: http://www.dnsbl.info/dnsbl-database-check.php
    df = pd.DataFrame()
    tasks = asyncio.gather(broker.find(types=[( 'HTTP', 'HIGH' ), 'HTTPS'], limit=100, timeout=0.5, max_tries=1), show(proxies, class_object))
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(tasks)
    except:
        pass
    pd.DataFrame(data=class_object.data).to_csv(proxy_file, index=False)


    # print('hello')

if __name__ == '__main__':
    main()