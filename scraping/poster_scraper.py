import re
from common_methods import get_soup_url, divide_chunks, flatten, listdir_nohidden
import threading
import pandas as pd
import time
import os
from all_processes.load_yaml_vars import LoadYamlVars

class PosterScraper(LoadYamlVars):

    def __init__(self, yaml_file_path, instances=5):
        super().__init__(yaml_file_path=yaml_file_path)
        self.years = list(range(1912, 2023))
        self.instances = instances
        self.link_df = None
        self.files = self.get_files()
        self.excluded_links = self.filter_links()

    def get_page_year(self, year):
        return f'http://www.impawards.com/{year}/alpha1.html'
    
    def get_poster_soup(self, year, page=1):
        return get_soup_url(f'http://www.impawards.com/{year}/alpha{page}.html')

    def get_poster_divs(self, soup, year):
        divs = soup.find_all('div', {'class': 'constant_thumb'})
        return [self.get_poster_info(div, year) for div in divs if self.div_matches_condition(div, year)]

    def div_matches_condition(self, div, year):
        try:
            not_excluded = ( self.get_poster_link(div, year) not in self.excluded_links  )
            return not_excluded and ( '_ver' not in div.find('img')['src'] )
        except:
            return False

    def get_poster_info(self, div, year):
        link_url = self.get_poster_link(div, year)
        return {'link_url': link_url, 'image_link': self.get_image_link(div, year), 'movie': self.get_tconst(link_url)}

    def get_poster_link(self, div, year):
        return f"http://www.impawards.com/{year}/{div.find('a')['href']}"

    def get_image_link(self, div, year):
        return f"http://www.impawards.com/{year}/{div.find('img')['src']}"

    def get_tconst(self, link_url):
        new_soup = get_soup_url(link_url)
        return f"tt{new_soup.find('a', {'href': re.compile('imdb.com')})['href'].split('tt')[-1]}"

    def get_page_count(self, soup):
        div = soup.find('div', {'class': 'container hidden-xs'})
        page_count = div.text.split('of')[-1].split(']')[0].strip()
        return int(page_count)

    def process_links(self, instance_id, years):
        starting_time = int(time.time())
        main_data = list()
        print('length of years is', len(years))
        for year in years:
            print('on year', year)
            try:
                first_soup = self.get_poster_soup(year, 1)
                page_count = self.get_page_count(first_soup)
                for page in list(range(1, page_count +1)):
                    try:
                        soup = self.get_poster_soup(year, page)
                        divs = self.get_poster_divs(soup, year)
                        main_data = flatten([main_data, divs])
                    except:
                        pass
                self.save_data_checkpoint(main_data, instance_id, starting_time)
            except Exception as e:
                print('exception was', e)
                self.save_data_checkpoint(main_data, instance_id, starting_time)
        self.save_data_checkpoint(main_data, instance_id, starting_time)

    def save_data_checkpoint(self, data, instance_id, starting_time):
        pd.DataFrame(data=data).to_csv(f'{self.poster_directory}{instance_id}_{starting_time}.csv', index=False)

    def get_all_years(self):
        thread_list = list()
        chunked_years = self.get_instance_years()
        for instance_id in range(self.instances):
            thread = threading.Thread(name='Test {}'.format(instance_id), target=self.process_links,
                                      args=(instance_id, chunked_years[instance_id]))
            thread_list.append(thread)
            thread.start()
            print(thread.name + ' started!')
        for thread in thread_list:
            thread.join()

    def filter_links(self):
        try:
            self.combine_data()
            self.excluded_links = list(self.link_df['link_url'])
        except:
            return list()

    def get_files(self):
        try:
            return listdir_nohidden(self.poster_directory)
        except:
            return None

    def try_file(self, file):
        try:
            pd.read_csv(file)
            return True
        except:
            return False

    def combine_data(self):
        if self.files:
            dfs = [self.try_file(file) for file in self.files if self.try_file(file)]
        else:
            return None
        try:
            df = pd.read_csv(self.poster_output_file)
            dfs.append(df)
        except:
            pass
        if dfs:
            df = pd.concat(dfs)
        else:
            try:
                df = pd.read_csv(self.poster_output_file)
            except:
                pass
        try:
            df.drop_duplicates()
            df = df.loc[~df['link_url'].isnull()]
            df.to_csv(self.poster_output_file)
            self.link_df = df
        except:
            pass

    def process_posters(self):
        self.get_all_years()
        self.remove_unncessary_files()
        return self.combine_data()

    def remove_unncessary_files(self):
        for file in self.files:
            os.remove(file)

    def get_instance_years(self):
        return [year for year in divide_chunks(self.years, round(len(self.years) / self.instances))]

if __name__ == '__main__':
    PosterScraper(yaml_file_path='/Users/timdunn/movie_project/all_processes/movie_configuration.yaml').process_posters()
    