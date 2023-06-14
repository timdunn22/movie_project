from common_methods import *
from scraping.just_watch_scraper import *

class JustWatchLinksScraper:
    
    def __init__(self, data_directory, output_directory='/Users/timdunn/Desktop/just_watch_data', number_proxies=100, link_type='tubi', split_string='tubitv.com%2Fmovies%2F', already_downloaded=[]):
        self.data_directory = data_directory
        self.data = []
        self.output_directory = output_directory
        self.proxies = get_list_proxies(number_proxies, https=True)
        self.link_type = link_type
        self.split_string = split_string
        self.already_downloaded = already_downloaded
        self.output_file_name = "{}/just_watch_{}_data.csv".format(self.output_directory, self.link_type)
        
    def just_watch_url(self, link_tag):
        return link_tag.get('href')
    
    def just_watch_links(self, soup):
        initial_links = [self.just_watch_url(link_tag) for link_tag in soup.find_all('a', {'class': 'title-list-grid__item--link'})]
        return list(filter(lambda link: link not in self.already_downloaded, initial_links))
    
    def get_html_files(self):
        return list(filter(lambda file: '.html' in file, listdir_nohidden(self.data_directory)))
    
    def get_all_links(self):
        links = []
        for file in self.get_html_files():
            soup = soup_from_path(file)
            links.append(self.just_watch_links(soup))
        return flatten(links)
    
    def save_data_to_file(self):
        current_df = pd.DataFrame(data=self.data).drop_duplicates()
        if len(self.data) > 0:
            if self.output_file_name in listdir_nohidden(self.output_directory):
                file_df = pd.read_csv(self.output_file_name)
                current_df = pd.concat([file_df, current_df]).drop_duplicates()    
            return current_df.to_csv(self.output_file_name, index=False)
        return current_df
    
    def get_all_movies_info(self):
        for link in self.get_all_links():
            self.data.append(JustWatchScraper(link, proxies=self.proxies, link_type=self.link_type, split_string=self.split_string).get_movie_info())
            if len(self.data) % 100 == 0:
                print('this much data', len(self.data))
                self.save_data_to_file()      
        return self.save_data_to_file()