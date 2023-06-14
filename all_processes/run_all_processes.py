from all_processes.run_all_scrapers import RunAllScrapers
from all_processes.convert_all_data import ConvertAllData
from all_processes.add_all_imdb_info import AddAllImdbInfo
from all_processes.load_yaml_vars import LoadYamlVars
from all_processes.update_popular_imdb import UpdatePopularImdb
from all_processes.process_okru_links import ProcessOkruLinks
from scraping.okru_scraper import OkruScraper, ScrapeAllImdb
import pandas as pd
import numpy as np
from lingua import Language, LanguageDetectorBuilder
from common_methods import reset_and_copy
from guessit import api
from imdb import Cinemagoer

class RunAllProcesses:
    
    def __init__(self, configuration_file):
        self.configuration = LoadYamlVars(configuration_file)
        self.configuration_file = configuration_file
        self.update_top_movie_object = UpdatePopularImdb()
        self.only_tconsts = list()
        self.process_okru_object = list()
        self.links = list()
        self.cinemagoer_object = Cinemagoer()
        languages = Language.__members__.values()
        self.detector = LanguageDetectorBuilder.from_languages(*languages).build()

    def run_a_scraper(self, scraper_object):
        try:
            return scraper_object().run_scraper()
        except:
            print('There was an exception running the {} scraper'.format(scraper_object().main_link))
    
    def run_all_scrapers(self):
        RunAllScrapers().run_all_scrapers() 
        
    def update_imdb_info(self):
        AddAllImdbInfo(configuration_file=self.configuration_file).run_with_exceptions()

    def convert_data(self):
        ConvertAllData(self.configuration_file).convert_data()

    def run_everything(self):
        self.update_top_movies()
        self.convert_data()
        self.update_imdb_info()
        self.run_all_scrapers()
        self.merge_links_imdb()

    def merge_links_imdb(self):
        pass

    def update_top_movies(self):
        self.run_generate_top_movies()
        self.scrape_okru_for_top_movies()
        self.run_all_scrapers()
        self.verify_links_and_extract_info()
        self.update_top_language_info()
        self.add_top_dubbed_info()
        self.merge_top_movies()

    def set_links(self):
        self.links = pd.read_csv(self.configuration.merged_links_path)
        self.links = reset_and_copy(self.links.loc[self.links['movie'].isin(self.only_tconsts)])

    def update_top_language_info(self):
        self.set_links()
        self.links['maybe_language'] = self.links['fulltitle'].apply(self.detect_language)
        self.links['audio_language'] = self.links.apply(self.get_language, axis=1)

    def detect_language(self, title):
        try:
            return ",".join(np.unique([result.language.name.lower().capitalize() 
                            for result in self.detector.detect_multiple_languages_of(title)]))
        except:
            return None

    def get_language(self, link):
        try:
            maybe_splits = link['maybe_language'].split(',')
            if len(maybe_splits) == 1 and maybe_splits[0] != "English":
                return maybe_splits[0]
            elif len(maybe_splits) == 2 and link['language'] in maybe_splits:
                return [language for language in maybe_splits if language != link['language']][0]
            return None 
        except:
            return None

    def merge_top_movies(self):
        pass

    def verify_links_and_extract_info(self):
        self.process_okru_object = ProcessOkruLinks(only_tconsts=self.only_tconsts, 
                                                    data_path='./top_movie_links.csv')
        self.process_okru_object.update_okru_extra_info()
        data = self.process_data(self.process_okru_object.data)
        df = pd.DataFrame(data=data)
        movie_file = pd.read_csv(self.configuration.merged_movie_title_path)
        link_file = pd.read_csv(self.configuration.everything_path)
        merged_file = pd.merge(movie_file, link_file, left_on='tconst', right_on='tconst', how='inner')
        merged_file = pd.merge(merged_file, df, left_on="new link url", right_on="webpage_url", how='inner')
        merged_file['audio_language'] = merged_file.apply(self.detect_audio_language, axis=1)
        return merged_file

    def process_data(self, data):
        return [{**link_data, **{"active": not ('error' in list(link_data.keys()))}}  for link_data in data]

    def detect_language(self, title):
        try:
            return np.unique([result.language.name.lower().capitalize() 
                            for result in self.detector.detect_multiple_languages_of(title)])
        except:
            return list()

    def title_language(self, link):
        return self.detect_language(link['fulltitle'])

    def description_language(self, link):
        return self.detect_language(link['description'])

    def detect_audio_language(self, movie_link_df):
        try:
            description_languages = self.description_language(movie_link_df)
            count_description_languages = len(description_languages)
            if count_description_languages == 0:
                title_languages = self.title_language(movie_link_df)
                if len(title_languages) == 1:
                    if api.guessit(movie_link_df['fulltitle'])['title'] == movie_link_df['primary_title']:
                        return self.get_original_language(movie_link_df)
                    return title_languages[0]
                elif len(title_languages) == 2:
                    language = self.get_original_language(movie_link_df)
                    if language in title_languages:
                        return [lan for lan in title_languages if lan != language][0]
                    return None
                return None
            else:
                descriptions_max = self.get_frequent_language(movie_link_df['description'])
                if descriptions_max is None:
                    title_languages = self.title_language(movie_link_df)
                    common_languages = [lan for lan in title_languages if lan in description_languages]
                    if len(common_languages) == 1:
                        return common_languages[0]
                    elif len(title_languages) == 1:
                        if title_languages[0] in common_languages:
                            return title_languages[0]
                    else:
                        return self.get_frequent_language(movie_link_df['description'])
        except:
            return None

    def get_frequent_language(self, title):
        title_language_objects = self.detector.detect_multiple_languages_of(title)
        lang_objs = [{lang.language.name.lower().capitalize(): lang.word_count} for lang in title_language_objects]
        new_dict = dict()
        different_keys = np.unique([list(lang_obj.keys())[0] for lang_obj in lang_objs])
        max_val = 0
        max_keys = list()
        for key in different_keys:
            new_dict[key] = sum([lang_obj.get(key) for lang_obj in lang_objs if list(lang_obj.keys())[0] == key])
            if new_dict[key] > max_val:
                max_val = new_dict[key]
                max_keys.append(key)
            elif new_dict[key] == max_val:
                max_keys.append(key)
        if len(max_keys) == 1:
            return max_keys[0]
        return None

    def get_original_language(self, movie):
        try:
            if movie.get('language') not in ['None', None] and not pd.isnull(movie.get('language')):
                return movie.get('language')
            else:
                return self.cinemagoer_object.get_movie(movie.get('tconst').split('tt')[-1]).guessLanguage()
        except:
            return None

    def add_top_dubbed_info(self):
        self.process_okru_object.update_dubbed_data()

    def run_generate_top_movies(self):
        self.update_top_movie_object.run_and_generate_all()
        self.set_tconsts()

    def set_tconsts(self):
        tconsts_paths = [self.update_top_movie_object.top_indian_destination, 
                         self.update_top_movie_object.top_imdb_destination, 
                         self.update_top_movie_object.top_popular_destination]
        dfs = [pd.read_csv(path) for path in tconsts_paths]
        genre_df = pd.read_csv(self.update_top_movie_object.genres_destination)
        genre_df['tconst'] = genre_df['movie']
        dfs.append(genre_df)
        self.only_tconsts = np.unique(pd.concat(dfs)['tconst'])
    
    def scrape_okru_for_top_movies(self, instances=6, use_proxy=False):
        scraper_progress_path = self.configuration.get_scraper_progress_path(OkruScraper)
        scraping_directory = self.configuration.get_scraping_directory(OkruScraper)
        ScrapeAllImdb(instances, self.configuration.imdb_directory, self.configuration.chromedriver_path,
                                                scraper_progress_path, OkruScraper, scraping_directory,
                                                only_tconsts=self.only_tconsts,
                                                use_proxy=use_proxy).run_instances_scraper()
