from common_methods import *
from scraping.single_selenium_scraper import SingleSeleniumScraper
from concurrent.futures import ThreadPoolExecutor

class ManyScraper:

    def __init__(self, instances=5, output_directory=None, output_file=None, vars=None, unique_key='link_url'):
        self.output_directory = output_directory
        self.output_file = output_file
        self.instances = instances
        self.vars = None
        self.unique_key = unique_key
        self.excluded_list = self.get_excluded_list()
        if self.excluded_list is None:
            self.excluded_list = list()
        self.proxies = None
        self.vars = None

    def get_excluded_list(self):
        self.combine_files()
        self.remove_checkpoint_files()
        self.excluded_list = list()
        self.exclude_combined_file()
        self.exclude_directory_files()

    def combine_files(self):
        main_df = None
        dfs = None
        try:
            main_df = pd.read_csv(self.output_file)
        except:
            pass
        try:
            dfs = [try_df(file) for file in listdir_nohidden(self.output_directory) if try_df(file)]
        except:
            pass
        if main_df is not None:
            if dfs is not None:
                dfs.append(main_df)
        if dfs is not None:
            pd.concat(dfs).to_csv(self.output_file, index=False)

    def remove_checkpoint_files(self):
        for file in listdir_nohidden(self.output_directory):
            os.remove(file)

    def exclude_combined_file(self):
        try:
            self.excluded_list = flatten([self.excluded_list, list(pd.read_csv(self.output_file)[self.unique_key])])
        except:
            pass

    def exclude_directory_files(self):
        try:
            files = listdir_nohidden(self.output_directory)
            dfs = list()
            for file in files:
                try:
                    dfs.append(pd.read_csv(file))
                except:
                    pass
            self.excluded_list = flatten([self.excluded_list, list(pd.concat(dfs)[self.unique_key])])
        except:
            pass

    def run_scraper(self, instance_id, vars):
        pass

    def get_page_url(self):
        pass

    def run_instances_scraper(self):
        # thread_list = list()
        single_selenium_instance = SingleSeleniumScraper(proxies=self.proxies)
        self.set_vars(single_selenium_instance)
        chunked_vars = list( self.get_chunked_vars() )
        # for instance_id in range(self.instances):
        #     thread = threading.Thread(name='Test {}'.format(instance_id), target=self.run_scraper,
        #                               args=(instance_id, chunked_vars[instance_id]))
        #     thread_list.append(thread)
        #     thread.start()
        #     print(thread.name + ' started!')
        # for thread in thread_list:
        #     thread.join()
        with ThreadPoolExecutor(max_workers=self.instances) as executor:
            for instance_id in range(self.instances):
                try:
                    executor.submit(self.run_scraper, instance_id, chunked_vars[instance_id])
                except:
                    executor.shutdown()



    def get_chunked_vars(self):
        return divide_chunks(self.vars, round(len(self.vars) / self.instances))

    def save_data_checkpoint(self, data, instance_id, starting_time):
        pd.DataFrame(data=data).to_csv(f'{self.output_directory}{instance_id}_{starting_time}.csv', index=False)
    
    def set_vars(self, driver):
        pass

    def run_and_combine_data(self):
        self.excluded_list()
        self.run_instances_scraper()