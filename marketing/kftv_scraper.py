import pysnooper
from multiprocessing import Pool, Process
from threading import Event
from movie_project.requirements import (By, time, np, threading)
from movie_project.all_processes.load_yaml_vars import LoadYamlVars
from movie_project.common_methods import ( get_selenium_soup, pd, flatten, 
                                          common_null_values, file_w_non_null, 
                                          get_undetected_chromedriver, 
                                          not_null_value, merge_many_dicts, 
                                          get_stripped_text_value, sample, FreeProxy, 
                                          divide_chunks, os, listdir_nohidden )
from selenium.webdriver.firefox.options import Options
from selenium import webdriver

class CombineProgressData:

    def __init__(self, progress_directory, output_file, 
                 extraction_column, unique_identifier):
        self.progress_directory = progress_directory
        self.extraction_column = extraction_column
        self.unique_identifier = unique_identifier
        self.output_file = output_file
        self.data = list()

    def get_output_data(self):
        try:
            return file_w_non_null(self.output_file, self.extraction_column)
        except:
            pass

    def get_progress_dfs(self):
        return [file_w_non_null(file, self.extraction_column) 
                for file in listdir_nohidden(self.progress_directory)]

    def get_progress_data(self):
        try:
            dfs = self.get_progress_dfs()
            if len(dfs) > 1:
                main_df = pd.concat(dfs)
                main_df.drop_duplicates(subset=[self.extraction_column, 
                                                self.unique_identifier])
                return main_df 
            return dfs[0]
        except:
            return list()

    def df_w_dropped_nulls(self, file):
        return file_w_non_null(file, self.extraction_column) 

    def drop_index_columns(self):
        columns_to_drop = [column for column in list(self.data.columns) 
                           if 'nnamed' in column]
        self.data.drop(columns=columns_to_drop, inplace=True)

    @pysnooper.snoop(depth=2)
    def get_data(self):
        try:
            dfs = list()
            output_data = self.get_output_data()
            if output_data is not None:
                dfs.append(output_data)
            dfs.append(self.get_progress_data())
            if len(dfs) > 1:
                self.data = pd.concat(dfs)
            elif len(dfs) == 1:
                self.data = dfs[0]
            self.drop_index_columns()
            self.data.drop_duplicates(subset=[self.extraction_column], inplace=True)
            return self.data
        except:
            pass

class ItemManagement:

    def __init__(self, item_file, item_file_col, instances, unique_identifier, 
                 output_file, progress_directory, not_found_file=None, 
                 other_not_found=None):
        self.output_file = output_file
        self.unique_identifier = unique_identifier
        self.set_not_found_items(not_found_file, other_not_found)
        self.files = list()
        self.progress_directory = progress_directory
        self.instances = instances
        self.item_file = item_file
        self.item_file_col = item_file_col
        self.set_items()

    def set_not_found_items(self, not_found_file, other_not_found):
        not_found_items = list()
        try:
            df = pd.read_csv(not_found_file)
            not_found_items.append(list(df[df.columns[0]]))
        except:
            pass
        finally:
            if other_not_found is not None:
                not_found_items.append(other_not_found)
            not_found_items.append(list())
            self.not_found_items = flatten(not_found_items)

    def set_items(self):
        self.set_all_items()
        self.exclude_already_scraped()
        self.exclude_not_found_items()

    def set_all_items(self):
        try:
            self.items =  list( file_w_non_null(self.item_file, self.item_file_col
                                              )[self.item_file_col] )
        except:
            self.items = list()
            
    def exclude_not_found_items(self):
        self.items = [item for item in self.items 
                        if item not in self.not_found_items]

    def exclude_already_scraped(self):
        already_scraped = self.get_already_scraped()
        self.items = [item for item in self.items 
                        if item not in already_scraped]

    def items_into_sections(self):
        if len(self.items) > 0:
            return [item for item in divide_chunks(self.items, round(len(self.items)/ 
                                                                    self.instances))]
    
    def assign_files(self):
        self.files = listdir_nohidden(self.progress_directory)

    def get_output_items(self):
        try:
            return list( file_w_non_null(self.output_file, 
                                                    self.unique_identifier
                                                    )[self.unique_identifier] )
        except:
            return list()

    def get_progress_items(self):
        progress_items =  [list(file_w_non_null(file, self.unique_identifier
                                                )[self.unique_identifier]) 
                           for file in self.files]
        if len( progress_items ) > 0:
            return flatten(progress_items)
        return progress_items

    def get_already_scraped(self):
        try:
            self.assign_files()
            items = list()
            items.append(self.get_output_items())
            items.append(self.get_progress_items())
            return flatten(items)
        except:
            return list()

    
class ProxyManagement:

    def __init__(self, proxy_file):
        self.get_proxies(proxy_file)
        self.set_proxy_port()


    def get_proxies(self, proxy_file):
        if proxy_file:
            self.proxies = [dict(row) for _, row in pd.read_csv(proxy_file).iterrows()]
        else:
            self.proxies = list()

    def set_proxy_port(self):
        current_proxy = sample(self.proxies, 1)[0]
        self.proxy_host = current_proxy.get('host')
        self.proxy_port = current_proxy.get('port')

    def make_new_proxy(self):
        return FreeProxy().get()
    
    def reset_proxy(self):
        if len(self.proxies) < 2:
            proxy = self.make_new_proxy()
            self.proxy_port = proxy.split('://')[-1].split(':')[-1]
            self.proxy_host = ":".join(proxy.split(':')[:-1])
            self.proxies = [proxy]
        else:
            self.proxies =  [proxy for proxy in self.proxies 
                             if proxy != f'{self.proxy_host}:{self.proxy_port}']
            self.set_proxy_port()

class ProcessManagement:

    def __init__(self, instances, item_sections, target_class, target_method):
        self.instances = instances
        self.item_sections = item_sections
        self.target_method = target_method
        self.target_class = target_class
        self.add_processes_to_queue()

    def add_processes_to_queue(self):
        self.process_list = [ProcessSingleton(instance_id=instance_id, 
                                              target_class=self.target_class, 
                                              target_method=self.target_method, 
                                              items=self.item_sections[instance_id]) 
                                              for instance_id in range(self.instances)]

    def start_processes(self):
        for process_object in self.process_list:
            process_object.start_process()

    def join_processes(self):
        for process_object in self.process_list:
            process_object.join_process()

    # def start_pool(self, result_iterable):
    #     for result in result_iterable:
    #         return pool.starmap_async(self.target_method, error_callback=self.start_pool)

    # def async_start(self):
    #     with Pool(processes=self.instances) as pool:
    #         pool.starmap_async(self.target_method, error_callback=self.start_pool())
            
    def start(self):
        self.start_processes()
        self.join_processes()

class ProcessSingleton:

    def __init__(self, instance_id, items, target_method, target_class):
        self.instance_id = instance_id
        self.target_method = target_method
        self.target_class = target_class
        self.items = items
        self.create_process()

    def restart_process(self):
        try:
            print('The type of the process is', type(self.process))
            if type(self.process) != 'NoneType':
                self.stop_process()
            self.create_process()
            self.start_process()
            self.join_process()
        except Exception as e:
            print('exception on restart is', e)
            self.restart_process()

    def stop_process(self):
        try:
            if type(self.process) != 'NoneType':
                self.process.terminate()
        except Exception as e:
            print('exception on stop is', e)
        finally:
            self.process = None

    def create_process(self):
        self.process = Process(target=self.target_method, args=(self.instance_id, self.items))

    def log_process_start(self):
        print(f'{self.target_class.__name__} {self.instance_id} started!')

    def join_process(self):
        self.process.join()

    def start_process(self):
        try:
            self.process.start()
            self.log_process_start()
        except Exception as e:
            print('exception on start is', e)
            self.restart_process()
    
class ThreadManagement:

    def __init__(self, instances, item_sections, target_class, target_method):
        self.instances = instances
        self.thread_list = list()
        self.item_sections = item_sections
        self.target_method = target_method
        self.target_class = target_class
    
    def restart_thread(self, thread_dict):
        try:
            # thread_dict.get('event').set()
            print('************into thread restarting')
            new_thread_dict = self.create_thread_dict(thread_dict.get('instance_id'), 
                                                      thread_dict.get('items'))
            self.set_thread_list(new_thread_dict, thread_dict)
            print('************after thread list')
            new_thread_dict = self.thread_list[-1]
            thread = new_thread_dict.get('thread')
            thread.start()
            print('after thread starting')
            print(thread.name + ' started!')
            thread.join()
            return True
        except:
            return True

    def set_thread_list(self, new_thread_dict, thread_dict):
        self.thread_list = [thread for thread in self.thread_list 
                            if thread != thread_dict]
        self.thread_list.append(new_thread_dict)
    
    def create_thread_dict(self, instance_id, items):
        return {'thread': self.create_thread(instance_id, items), 
                'event': Event(),
                'instance_id': instance_id, 'items': items}
    
    def create_thread(self, instance_id, items):
        thread_name = f'{self.target_class.__name__} instance {instance_id}'
        return threading.Thread(name=thread_name, target=self.target_method,
                                args=(instance_id, items))
    
    def start_threads(self):
        for instance_id in range(self.instances):
            try:
                thread = self.create_thread_dict(instance_id=instance_id, 
                                                 items=self.item_sections[instance_id])
                self.thread_list.append(thread)
                thread = thread.get('thread')
                thread.start()
                print(thread.name + ' started!')
            except:
                print(f"********enter start thread instance {instance_id} exception")
    
    def join_threads(self):
        for thread in self.thread_list:
            completed = False
            while not completed:
                try:
                    thread = thread.get('thread')
                    thread.join()
                    completed = True
                except:
                    print('*********went into join threads exception block')
                    completed = self.restart_thread(thread)

    def start(self):
        self.start_threads()
        self.join_threads()

class MultiThreadedSeleniumScraper:

    def __init__(self, instances, progress_directory, scraper_class, output_file, 
                 item_file, proxy_file, extraction_column=None, 
                 not_found_item_file=None, thread_class=ThreadManagement):
        self.proxy_file = proxy_file
        self.instances = instances
        self.progress_directory = progress_directory
        self.scraper_class = scraper_class
        self.output_file = output_file
        self.item_file = item_file
        self.extraction_column = self.scraper_class.confirmed_extracted_col
        self.unique_identifier = self.scraper_class.unique_identifier
        self.item_file_col = self.scraper_class.item_file_col
        self.not_found_item_file = not_found_item_file
        self.data = list()
        self.files = self.set_files()
        self.thread_class = thread_class
        self.set_items()
        self.set_thread_object()

    def set_files(self):
        try:
            return listdir_nohidden(self.progress_directory)
        except:
            return list()

    def set_thread_object(self):
        if len(self.items_object.items) >= self.instances:
            self.thread_object = self.thread_class(instances=self.instances, 
                                                item_sections=self.items_object.items_into_sections(), 
                                                target_class=self.scraper_class, 
                                                target_method=self.get_all_data)
        else:
            self.thread_object = None

    def run_instances_scraper(self):
        if self.thread_object is not None:
            self.thread_object.start()

    def get_process_object(self, instance_id):
        return [process_object for process_object in self.thread_object.process_list 
                if process_object.instance_id == instance_id][0]

    def get_all_data(self, instance_id, items):
        return self.scraper_class(proxy_file=self.proxy_file, 
                                #   event=self.thread_object.thread_list[instance_id].get('event'),
                                  not_found_data_file=self.not_found_item_file,
                                  progress_directory=self.progress_directory, 
                                #   process_object=self.get_process_object(instance_id),
                                  items=items,
                                  instance_id=instance_id).get_all_data()

    def set_items(self):
        self.items_object = ItemManagement(item_file=self.item_file, 
                                           item_file_col=self.item_file_col, 
                                           instances=self.instances, 
                                           unique_identifier=self.unique_identifier, 
                                           output_file=self.output_file, 
                                           not_found_file=self.not_found_item_file,
                                           progress_directory=self.progress_directory)
        self.items_object.set_items()

    def run_instances_and_concat(self):
        if len(self.items_object.items) > 0:
            while len(self.items_object.items) > 0:
                try:
                    self.run_instances_scraper()
                    self.data = self.concat_data()
                    self.save_output_data()
                    self.remove_temp_data()
                    self.set_items()
                except:
                    pass
        else:
            try:
                self.data = self.concat_data()
                self.save_output_data()
                self.remove_temp_data()
                self.set_items()
            except:
                pass

    def remove_temp_data(self):
        print('entering remove data')
        for file in self.files:
            try:
                os.remove(file)
            except:
                pass

    def save_output_data(self):
            print('entering save output data')
            with open(self.output_file, "w") as f:
                self.data.to_csv(f, index=False)

    def concat_data(self):
        print('entering concat data')
        return CombineProgressData(self.progress_directory, self.output_file, 
                                   self.extraction_column, self.unique_identifier
                                   ).get_data()


class UndetectedSeleniumScraper:

    def __init__(self, unique_identifier, item_file_col, progress_directory, event=None,
                 confirmed_extracted_col=None, proxy_file=None, items=None, 
                 instance_id=None, process_object=None, 
                 page_scraper_class=None, not_found_data_file=None):
        self.unique_identifier = unique_identifier
        self.item_file_col = item_file_col
        self.not_found = False
        # self.confirmed_extracted_col = confirmed_extracted_col
        self.items = items
        self.data = list()
        self.instance_id = instance_id
        self.not_found_data_file = not_found_data_file
        self.last_update = time.time()
        self.not_found_data = list()
        self.event = event
        self.starting_time = time.time()
        self.progress_directory = progress_directory
        self.process_object = process_object
        self.page_scraper_class = page_scraper_class
        self.initialize_proxies(proxy_file)
        self.start_driver()

    def initialize_proxies(self, proxy_file):
        self.proxy_object = ProxyManagement(proxy_file)

    def go_to_page(self, url):
        self.driver.get(url)
        time.sleep(3)

    def set_not_found(self, val):
        self.not_found = val
    
    def quit_driver(self):
        try:
            self.driver.quit()
        except:
            pass
        finally:
            self.driver = None
        # self.event.set()

    def reset_proxy(self):
        self.proxy_object.reset_proxy()

    def add_to_not_found(self, item):
        self.not_found_data.append(item)

    def reset_driver_proxies(self):
        self.reset_proxy()
        self.quit_driver()
        # self.restart_process()
        self.start_driver()

    def restart_process(self):
        self.process_object.restart_process()

    def get_driver(self):
        options = Options()
        options.add_argument("--window-size=1920,1080")

        # options.add_argument(f'--user-agent={test_ua}')

        options.add_argument('--no-sandbox')
        options.add_argument("--disable-extensions")
        # uc.install()
        # proxy_num = sample(list(range(1,3)), 1)[0]
        # if proxy_num == 1:
        options.add_argument(f"--proxy-server={self.proxy_object.proxy_host}:{self.proxy_object.proxy_port}")

        
        options.add_argument("--headless")
        # return uc.Chrome(executable_path='/Users/timdunn/Downloads/chromedriver_mac_arm64-1/chromedriver', options=options)
        # webdriver.Safari(options=options)
        # return get_driver_from_options(options)
        return webdriver.Firefox(options=options,) 

    def remove_item(self, current_item):
        self.items = [item for item in self.items if item != current_item]

    def start_driver(self):
        # print('inside start driver')
        completed = False
        count = 1
        while not completed:
            try:
                # print('at start of try')
                self.driver = self.get_driver()
                # print('got past driver')
                completed = True
            except:
                self.quit_driver()
                if count > 3:
                    completed = True
                count += 1
        # print('driver has been started')
    
    def print_data_size(self):
        data_size =  np.unique([item[self.unique_identifier] 
                                for item in self.data]).size 
        return print(f'data length of {self.instance_id} instance is {data_size}')

    def refresh_last_update(self):
        self.last_update = time.time()

    def data_extracted(self, current_data):
        try:
            val = current_data.get(self.confirmed_extracted_col)
            return val not in common_null_values()
        except:
            return False

    def updated_frequently(self):
        return ( time.time() - self.last_update ) < 30

    @pysnooper.snoop(depth=3)
    def get_all_data(self):
        # print('into get all data')
        # print('length of items is', len(self.items))
        # print('before for loop')
        for item in self.items:
            # print('inside get all data')
            # if self.event.is_set():
            #     self.save_filtered_data()
            #     break
            try:
                current_data = self.get_item_data(item)
                got_data = self.data_extracted(current_data)
                if self.not_found:
                    self.add_to_not_found(item)
                    break
                if not self.updated_frequently():
                    break

                # print(f'got data in try is {got_data} for {self.instance_id} for item {item}')
                self.get_append_data(item, got_data, current_data)
                # print('got past current data')
                try:
                    # if ( ( np.unique([item[self.unique_identifier] 
                    #                 for item in self.data]).size )  %  10) == 0:
                        self.save_filtered_data()
                except:
                    pass
                self.print_data_size()
                self.refresh_last_update()
            except Exception as e:
                # print('went into except on instance', self.instance_id)
                # print('exception is', e)
                self.get_append_data(item)
                # self.save_filtered_data()
                self.reset_items()
                self.print_data_size()
                self.get_all_data()
        if ( not self.updated_frequently() ) or (self.not_found):
            self.reset_items()
            self.reset_last_update()
            self.set_not_found(False)
            self.get_all_data()
        self.save_filtered_data()
        self.save_not_found_items()
        self.quit_driver()
        return self.data

    def close_popups(self):
        return

    def is_not_found(self, soup=None):
        return False

    def verify_data(self, soup):
        try:
            verify_value = self.page_scraper_class(soup=soup).verify_method()
            return verify_value not in common_null_values()
        except:
            return False

    def company_info(self, soup, item):
        try:
            basic_dict = self.page_scraper_class(soup=soup, url=item).company_info()
            basic_dict[self.unique_identifier] = item
            return basic_dict
        except:
            return None

    def set_not_found_vars(self, item):
        self.set_not_found(True)
        self.remove_item(item)
        return

    def get_soup_vars(self, current_time):
        self.close_popups()
        current_time = time.time()
        soup = get_selenium_soup(self.driver)
        sleeped_time = time.time() - current_time
        return soup, sleeped_time

    # @pysnooper.snoop(depth=2, watch=('self.data', 'len(self.items)', 
    #                                  'len(self.not_found_data)', 'len(self.links)'))
    def get_item_data(self, item):
        try:
            current_time = time.time()
            self.go_to_page(item)
            soup, sleeped_time = self.get_soup_vars(current_time)
            if self.is_not_found(soup):
                return self.set_not_found_vars(item)
            else:
                while (not self.verify_data(soup)) and (sleeped_time < 20):
                    time.sleep(1)
                    soup, sleeped_time = self.get_soup_vars(current_time)
                    if self.is_not_found(soup):
                        return self.set_not_found_vars(item)
                return self.company_info(soup, item)
        except:
            pass

    def save_not_found_items(self):
        if len(self.not_found_data) > 0:
            dfs = [pd.DataFrame(data={'item': self.not_found_data})]
            try:
                dfs.append(pd.read_csv(self.not_found_data_file))
            except:
                pass
            if len(dfs) == 1:
                df = dfs[0]
            else:
                df = pd.concat(dfs)
                df.drop_duplicates(inplace=True)
            df.to_csv(self.not_found_data_file, index=False)

    def reset_last_update(self):
        self.last_update = time.time()

    def reset_items(self):
        already_scraped = set([item.get(self.unique_identifier) for item in self.data])
        self.items = [item for item in self.items if ( item not in already_scraped ) 
                      and (item not in self.not_found_data)]

    # @pysnooper.snoop(depth=3)
    def get_append_data(self, item, got_data=False, current_data=None):
        count = 1
        while not got_data:
            self.reset_driver_proxies()
            current_data = self.get_item_data(item)
            if self.not_found:
                break
            # print('current data is', current_data)
            got_data = self.data_extracted(current_data)
            count += 1
            # if count == 10:
            #     return
        if ( current_data is not None ) and got_data:
            self.data.append(current_data)

        
    def filter_data(self):
        self.data = [item for item in self.data 
                     if item.get(self.confirmed_extracted_col) 
                     not in common_null_values()]

    def save_filtered_data(self):
        self.filter_data()
        if len(self.data) > 0:
            file_name = f'{self.progress_directory}{self.instance_id}_{self.starting_time}.csv'
            df = pd.DataFrame(data=self.data)
            with open(file_name, "w") as f:
                df.to_csv(f)



class KftvPageScraper:

    def __init__(self, soup):
        self.soup = soup
    
    def get_stripped_text_value(self, *args):
        try:
            return self.soup.find(args).text.strip()
        except:
            return None

    def get_company_name(self):
        return self.get_stripped_text_value('h1', {'class': "HeadingSection"})

    def get_profile(self):
        return self.get_stripped_text_value('p', {'class': 'Bodypositive'})

    def phone_number(self):
        try:
            return self.soup.find('div', 
                                  {'class': 'Bodypositive'}
                                  ).find('a')["href"].split("tel:")[-1]
        except:
            return None

    def email(self):
        try:
            return [span.text.strip() for span in self.contact_spans() 
                    if '@' in span.text.strip()][0]
        except:
            return None

    def contact_spans(self):
            return [heading_title for heading_title in 
                    self.soup.find_all("h5", {"class": "HeadingTitlePositive"}) 
                    if heading_title.text == "Contact"][0].parent.find(
                'p', {'class': "Bodypositive"}).find_all('span')

    def website(self):
        try:
            return self.soup.find('div', {'class': 'Bodypositive'}
                                  ).find_all('a')[-1]["href"]
        except:
            try:
                return [span.text.strip() for span in self.contact_spans() 
                        if 'www' in span.text.strip()][0]
            except:
                return None

    def team_members(self):
        try:
            links = [heading_title for heading_title in 
                     self.soup.find_all("h5", {"class": "HeadingTitlePositive"}) 
                    if heading_title.text == "Team"][0].parent.find_all('a')
            dicts = [{f'Team Member {index+1}': link.text.strip()} 
                     for index, link in enumerate(links)]
            return merge_many_dicts(dicts)
        except:
            return None

    def extended_profile(self):
        try:
            return [heading_title for heading_title in 
                    self.soup.find_all("h5", {"class": "HeadingTitlePositive"}) 
                    if heading_title.text == "More info"][0].parent.find(
                'p', {'class': "Bodypositive"}).text.strip()
        except:
            return None

    def locations_worked(self):
        try:
            return [heading_title for heading_title in 
                    self.soup.find_all("h5", {"class": "HeadingTitlePositive"}) 
                    if heading_title.text == "Locations Worked"
                    ][0].parent.find('span').text.strip()
        except:
            return None

    def languages(self):
        try:
            language_tags = [heading_title for heading_title 
                             in self.soup.find_all("h5", 
                                                   {"class": "HeadingTitlePositive"}) 
                    if heading_title.text == "Languages"][0].parent.find_all(
                'div', {'class': 'col'})
            dicts = [{f'Language {index+1}': language_tag.text.strip()} 
                     for index, language_tag in enumerate(language_tags)]
            return merge_many_dicts(dicts)
        except:
            return None

    def credits(self):
        try:
            credit_table = [heading_title for heading_title in 
                            self.soup.find_all("h5", {"class": "HeadingTitlePositive"}) 
                            if heading_title.text == "Credits"][0].parent
            dicts = [self.get_production_data(index, tr) for index, tr in 
                    enumerate(credit_table.find('tbody').find_all('tr'))]
            return merge_many_dicts(dicts)
        except:
            return None

    def get_production_data(self, index, tr):
        try:
            tds = tr.find_all("td")
            adjusted_index = index + 1
            return {f'Production {adjusted_index}': tds[0].text.strip(), 
                    f'Year {adjusted_index}': tds[1].text.strip(),
                f'Type {adjusted_index}': tds[2].text.strip(),
                f'Role {adjusted_index}': tds[3].text.strip()}
        except:
            return None

    def company_info(self):
        return merge_many_dicts(self.company_info_methods())

    def company_info_methods(self):
        return [self.credits(), self.languages(), self.team_members(), 
                self.main_company_info()]
    
    def verify_data(self):
        return self.get_company_name() not in common_null_values()

    def main_company_info(self):
        return {'Email': self.email(),
                'Company Name': self.get_company_name(), 
                'Locations Worked': self.locations_worked(), 
                'Extended Profile': self.extended_profile(), 
                'Website': self.website(), 
                'Simple Profile': self.get_profile(), 
                "Phone Number": self.phone_number()}

    
class KftvScraper(UndetectedSeleniumScraper):
    unique_identifier = 'KFTV URL'
    item_file_col = 'kftv_link'
    confirmed_extracted_col = 'Company Name'

    def __init__(self, proxy_file, process_object=None, event=None, 
                 progress_directory=None, items=None, 
                 not_found_data_file=None, instance_id=None):
        super().__init__(unique_identifier=self.unique_identifier, 
                         event=event,
                         item_file_col=self.item_file_col, 
                         proxy_file=proxy_file, items=items, 
                         process_object = process_object,
                         not_found_data_file=not_found_data_file,
                         progress_directory=progress_directory, 
                         page_scraper_class=KftvPageScraper,
                         instance_id=instance_id)
    
    def close_popups(self):
        try:
            self.driver.find_element(By.XPATH, 
                                     "//button[@data-dismiss='modal']").click()
        except:
            pass

    def is_not_found(self, soup):
        try:
            return '404' in soup.find('div', {'class': 
                                    'not-found-404-text'}).text
        except:
            return False


class ExtractItemInfo:

    def __init__(self, driver, item, soup, page_scraper_class) -> None:
        self.driver = driver
        self.item = item
        self.soup = soup
        self.page_scraper_class = page_scraper_class
    
    def close_popups(self):
        return

    def is_not_found(self, soup):
        return False

    def verify_data(self, soup):
        try:
            verify_value = self.page_scraper_class(soup=soup).verify_method()
            return verify_value not in common_null_values()
        except:
            return False

    def company_info(self):
        return self.page_scraper_class(soup=self.soup, url=self.item).company_info()

    def set_not_found_vars(self, item):
        return

    def go_to_page(self, item):
        return

    def get_item_data(self, item):
        try:
            self.go_to_page(item)
            self.close_popups()
            current_time = time.time()
            soup = get_selenium_soup(self.driver)
            sleeped_time = time.time() - current_time
            if self.is_not_found(soup):
                return self.set_not_found_vars(item)
            else:
                while (not self.verify_data(soup)) and (sleeped_time < 20):
                    time.sleep(1)
                    self.close_popups()
                    soup = get_selenium_soup(self.driver)
                    if self.is_not_found(soup):
                        return self.set_not_found_vars(item)
                    sleeped_time = time.time() - current_time
                return self.company_info(soup, item)
        except:
            pass

    
class ThreadedScraperYaml(MultiThreadedSeleniumScraper):
    
    def __init__(self, progress_directory, output_file, 
                 item_file, proxy_file,  not_found_item_file=None, 
                 thread_class=ThreadManagement, yaml_file=None, 
                 maketing_key=None, instances=5, scraper_class=None):
        if yaml_file is not None:
            vars = LoadYamlVars(yaml_file)
            marketing_file_info = vars.marketing.get(maketing_key)
            progress_directory = marketing_file_info.get('Progress Directory')
            output_file = marketing_file_info.get('Profiles')
            not_found_item_file = marketing_file_info.get('Not Found File')
            item_file = marketing_file_info.get('Links')
            proxy_file = vars.proxies_path

        super().__init__(instances=instances, 
                         progress_directory=progress_directory,  
                         output_file=output_file, 
                         item_file=item_file, 
                         proxy_file=proxy_file, 
                         not_found_item_file=not_found_item_file, 
                         thread_class=thread_class, scraper_class=scraper_class)

class KftvThreadedScraper(ThreadedScraperYaml):

    def __init__(self, progress_directory, output_file, 
                 item_file, proxy_file,  not_found_item_file=None, 
                 thread_class=ThreadManagement, yaml_file=None, instances=5):
        
        super().__init__(instances=instances, 
                         maketing_key='Kftv',
                         yaml_file=yaml_file,
                         progress_directory=progress_directory,  
                         output_file=output_file, 
                         item_file=item_file, 
                         proxy_file=proxy_file, 
                         not_found_item_file=not_found_item_file, 
                         thread_class=thread_class, scraper_class=KftvScraper)

def main():
    yaml_file = os.environ.get('YAML_FILE', 
                               './movie_project/all_processes/movie_configuration.yaml')
    KftvThreadedScraper(instances=5, yaml_file=yaml_file).run_instances_and_concat()

if __name__ == '__main__':
    main()