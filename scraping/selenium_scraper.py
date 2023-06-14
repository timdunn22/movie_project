from common_methods import *
from scraping.single_selenium_scraper import SingleSeleniumScraper
from scraping.many_scraper import ManyScraper

class SeleniumScraper(ManyScraper):

    def __init__(self, base_url, page_url_format, page_logging=True, proxies=None, proxy_file=None,
                 instances=5, output_directory=None, output_file=None, checking_function=None, unique_key='link_url'):
        super().__init__(instances=instances, output_directory=output_directory, 
                         output_file=output_file, unique_key=unique_key)
        self.page_logging = page_logging
        self.base_url = base_url
        self.page_url_format = page_url_format
        self.vars = None
        self.checking_function = checking_function
        self.use_var = False
        self.data_dict = self.initialize_data_dict()
        if proxies is not None:
            self.proxies = proxies
        else:
            self.proxies = self.get_proxies_from_file(proxy_file)

    def initialize_data_dict(self):
        data_dict = dict()
        for instance_id in range(self.instances):
            data_dict[instance_id] = list()
        return data_dict

    def get_proxies_from_file(self, proxy_file):
        return [{'host': row['host'], 'port': row['port'] } for _, row in pd.read_csv(proxy_file).iterrows()]

    def run_scraper(self, instance_id, pages):
        selenium_instance = SingleSeleniumScraper(proxies=self.proxies, instance_id=instance_id, 
                                                  checking_function=self.checking_function)
        starting_time = int(time.time())
        try:
            for index, page in enumerate( pages ):
                count = 1
                # if page == 1:
                #     count = 33
                # print('past url part which is', url)
                if self.use_var:
                    extra_data = self.movies_info(page, selenium_instance)
                else:
                    url = self.get_page_url(page)
                    soup = selenium_instance.get_soup_url(url, count=count)
                    extra_data = self.movies_info(soup)
                self.data_dict[instance_id] = flatten([self.data_dict[instance_id], extra_data])
                # print('past movie info part')
                if ( index % 100 == 0 ) and self.page_logging:
                    print(index)
                    self.save_data_checkpoint(instance_id, starting_time)
            self.save_data_checkpoint(instance_id, starting_time)
        except Exception as e:
            print('into exception')
            print(e)
            self.save_data_checkpoint(self.data_dict[instance_id], instance_id, starting_time)
    
    def get_page_url(self, page):
        return self.page_url_format.format(page)

    def set_vars(self, soup):
        pass

    def save_data_checkpoint(self, instance_id, starting_time):
        pd.DataFrame(data=self.data_dict[instance_id]).to_csv(f'{self.output_directory}{instance_id}_{starting_time}.csv', index=False)

    def get_movie_tags(self, soup):
        pass

    def movies_info(self, soup, selenium_instance):
        tags = self.get_movie_tags(soup)
        print('past movie tags')
        print('excluded links are', len(self.excluded_list))
        return [self.movie_info(tag, selenium_instance) for tag in tags if self.movie_link_url(tag) not in self.excluded_list]

    def movie_info(self, tag):
        pass
    
    def movie_link_url(self, tag):
        pass