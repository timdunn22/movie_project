from scraping.selenium_scraper import SeleniumScraper
from scraping.single_selenium_scraper import SingleSeleniumScraper
import pandas as pd
from common_methods import *
from all_processes.load_yaml_vars import LoadYamlVars

class ImdbPosterScraper(SeleniumScraper):

    def __init__(self, proxy_file, output_directory, output_file, instances=5, movie_path=None):
        super().__init__(base_url=None, page_url_format=None,proxy_file=proxy_file, output_directory=output_directory, output_file=output_file, 
                         instances=instances, checking_function=self.get_img_url, unique_key='tconst')
        self.movie_df = pd.read_csv(movie_path, usecols=['tconst'])
        self.vars = None
        self.use_var = True

    def movie_info(self, movie_id, selenium_instance):
        poster = self.get_img_url(selenium_instance, movie_id)
        return {'tconst': movie_id, 'poster': poster}

    def get_movie_url(self, movie_id):
        return f'https://www.imdb.com/title/{movie_id}'

    def get_poster_url(self, soup):
        poster_href = soup.find('a', {'class': 'ipc-lockup-overlay', 'aria-label': re.compile('Poster')})
        if poster_href is not None:
            return f"https://www.imdb.com{poster_href['href']}"
        return None

    def get_img_url(self, selenium_instance, movie_id):
        final_soup = None
        try:
            movie_url = self.get_movie_url(movie_id)
            # print(f'movie url for movie {movie_id} is {movie_url}')
            try:
                movie_soup = selenium_instance.get_soup_url(movie_url, checking_function=self.home_page_valid)
            except:
                selenium_instance, movie_soup = self.reset_selenium_instance(movie_url, checking_function=self.home_page_valid)
            poster_url = self.get_poster_url(movie_soup)

            # print(f'poster url for movie {movie_id} is {poster_url}')
            if poster_url is not None:
                # print('into not none poster url')
                try:
                    final_soup = selenium_instance.get_soup_url(poster_url, checking_function=self.poster_valid)
                except:
                    selenium_instance, final_soup = self.reset_selenium_instance(poster_url, 
                                                                                 checking_function=self.poster_valid)
                # print('after the soup part')
                return self.poster_valid(final_soup)
            return None
        except Exception as e:
            # print('went into img url exception')
            # print('exception was', e)
            return None

    def reset_selenium_instance(self, url, checking_function):
        selenium_instance = SingleSeleniumScraper(proxies=self.proxies, checking_function=checking_function)
        return selenium_instance, selenium_instance.get_soup_url(url, checking_function=checking_function)

    def home_page_valid(self, soup):
        return soup.find('h1', {'data-testid': "hero__pageTitle"})

    def poster_valid(self, soup):
        return soup.find('div', {'data-testid': 'media-viewer'}).find_all('img')[0]['src']

    def movies_info(self, movie_id, selenium_instance):
        return [self.movie_info(movie_id=movie_id, selenium_instance=selenium_instance)]

    def set_vars(self, *args):
        self.vars = [tconst for tconst in self.movie_df['tconst'] if 'tt' in str(tconst)]


if __name__ == '__main__':
    configuration = LoadYamlVars('/Users/timdunn/movie_project/all_processes/movie_configuration.yaml')
    imdb_posters_object = ImdbPosterScraper( 
        output_file=configuration.scraper_paths.get('Imdb Poster').get('Combined Data Path'), 
        output_directory=configuration.scraper_paths.get('Imdb Poster').get('Data Directory'), 
        movie_path=configuration.scraper_paths.get('Imdb Poster').get('Tconst Path'), 
        instances=4, proxy_file=configuration.proxies_path)
    imdb_posters_object.run_instances_scraper()    