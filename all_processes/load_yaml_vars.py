from common_methods import *


class LoadYamlVars:

    def __init__(self, yaml_file_path=None, configuration=None):
        if configuration is None:
            self.configuration = load_yaml_file(yaml_file_path)
        else:
            self.configuration = configuration
        self.imdb_paths = self.configuration.get('Imdb')
        self.imdb_directory = self.imdb_paths.get('Latest Data Directory')
        self.imdb_title_path = f'{self.imdb_directory}title.basics.tsv'
        self.output_data_directory = self.imdb_paths.get('Converted Data Directory')
        self.converted_movie_title_path = f'{self.output_data_directory}title.csv'
        self.converted_aka_path = f'{self.output_data_directory}aka.csv'
        self.merged_movie_title_path = f'{self.output_data_directory}merged_imdb.csv'
        self.chromedriver_path = self.configuration.get('ChromedriverPath').strip()
        self.scraper_paths = self.configuration.get('Scraper')
        self.extra_data_directory = self.imdb_paths.get('Extra Data Directory')
        self.extra_data_path = self.imdb_paths.get('Extra Data Path')
        self.extra_data_output_path = f'{self.output_data_directory}extra.csv'
        self.soundmix_path = self.imdb_paths.get('Soundmix Path')
        self.certificate_path = f'{self.output_data_directory}certificate.csv'
        self.country_path = f'{self.output_data_directory}country.csv'
        self.genre_path = f'{self.output_data_directory}genres.csv'
        self.box_office_path = f'{self.output_data_directory}box_office.csv'
        self.videos_path = f'{self.output_data_directory}videos.csv'
        self.keywords_path = f'{self.output_data_directory}keywords.csv'
        self.links_path = self.configuration.get('Links')
        self.merged_links_path = f'{self.output_data_directory}merged_imdb_links.csv'
        self.all_links_path = f'{self.output_data_directory}all_links.csv'
        self.dubbed_path = self.links_path.get('Dubbed Path')
        self.everything_path = self.configuration.get('Everything Path')
        self.links_extra_path = self.links_path.get('Extra Path')
        self.links_extra_progress_path = self.links_path.get('Extra Progress Path')
        self.proxies_path = self.configuration.get('Proxies')
        self.poster_directory = self.configuration.get('Posters').get('Progress Path')
        self.poster_output_file = self.configuration.get('Posters').get('Output Path')

    def get_scraper_progress_path(self, scraper):
        scraper = self.get_scraper_name(scraper)
        return self.scraper_paths.get(scraper).get('Combined Data Path').strip()

    def get_scraping_directory(self, scraper):
        scraper = self.get_scraper_name(scraper)
        return self.scraper_paths.get(scraper).get('Data Directory').strip()

    def get_scraper_name(self, scraper):
        return scraper.__name__.split('Scraper')[0]

    def get_scraper_tconst_path(self, scraper):
        scraper = self.get_scraper_name(scraper)
        return self.scraper_paths.get(scraper).get('Tconst Path')

