from all_processes.load_yaml_vars import LoadYamlVars
from all_processes.update_popular_imdb import UpdatePopularImdb
from lingua import Language, LanguageDetectorBuilder
from common_methods import reset_and_copy, empty_column_value
from guessit import api
from imdb import Cinemagoer
from scraping.okru_scraper import OkruScraper, ScrapeAllImdb
import pandas as pd
import numpy as np
from all_processes.process_okru_links import ProcessOkruLinks

class ProcessTopMovies(LoadYamlVars):

    def __init__(self, configuration_file='./movie_configuration.yaml', instances=5):
        super().__init__(yaml_file_path=configuration_file)
        self.instances = instances
        self.update_top_movie_object = UpdatePopularImdb(destination_directory=self.top_movies_directory)
        self.only_tconsts = list()
        self.process_okru_object = list()
        self.links = list()
        self.movie_df = None
        self.links_df = None
        self.cinemagoer_object = Cinemagoer()
        self.detector = LanguageDetectorBuilder.from_languages(*Language.__members__.values()).build()

    def update_top_movies(self):
        self.run_generate_top_movies()
        self.verify_links_and_extract_info()
        self.scrape_okru_for_top_movies()
        # self.add_top_dubbed_info()
        # self.merge_top_movies()
    
    def run_generate_top_movies(self):
        self.update_top_movie_object.run_and_generate_all()
        self.set_tconsts()
    
    def merge_top_movies(self):
        self.set_link_df()
        self.set_movie_df()
        self.apply_guessit()
        self.set_merged_df()

    def set_movie_df(self):
        pd.read_csv(self.converted_aka_path)

    def set_merged_df(self):
        movie_title_column = 'title'
        movie_year_column = 'startYear'
        movie_duration_column = 'runtimeMinutes'
        link_title_column = 'guessit_title'
        link_year_column = 'guessit_year'
        link_duration_column = 'duration'
        link_url_column = 'link_url'
        self.merged_df = pd.merge(self.movie_df, self.link_df, left_on=[movie_title_column, 
                                                                        movie_year_column, movie_duration_column], 
                                  right_on=[link_title_column, link_year_column, link_duration_column])
        self.merged_df = self.filter_out_link_duplicates(self.merged_df, link_url_column=link_url_column)
        links_filtered = self.exclude_links(self.link_df, [self.merged_df], link_url_column)
        more_advanced_merge = pd.merge(self.movie_df, links_filtered,
                                       left_on=[movie_title_column,movie_year_column], 
                                       right_on=[link_title_column, link_year_column])
        more_advanced_merge = more_advanced_merge.loc[self.duration_within_range(more_advanced_merge, 10, 
                                                                                 movie_duration_column, 
                                                                                 link_duration_column)]
        more_advanced_merge = self.filter_out_link_duplicates(more_advanced_merge, link_url_column=link_url_column)
        links_filtered = self.exclude_links(self.link_df, [self.merged_df, more_advanced_merge], link_url_column)
        very_advanced_merge = pd.merge(self.movie_df, links_filtered, left_on=[movie_title_column], 
                                       right_on=[link_title_column])
        very_advanced_merge = very_advanced_merge.loc[self.year_within_range(very_advanced_merge, 1, 
                                                                             movie_year_column, link_year_column) & 
                                                      self.duration_within_range(very_advanced_merge, 10, 
                                                                                 movie_duration_column, 
                                                                                 link_duration_column)]
        very_advanced_merge = self.filter_out_link_duplicates(very_advanced_merge, link_url_column=link_url_column)
        self.merged_df = pd.concat(self.merged_df, more_advanced_merge, very_advanced_merge)
        self.merged_df = self.filter_out_link_duplicates(self.merged_df, link_url_column)

    def filter_out_link_duplicates(self, df, link_url_column):
        return df.loc[df.duplicated(subset=[link_url_column])]

    def duration_within_range(self, df, amount, movie_duration_column, link_duration_column):
        movie_duration = df[movie_duration_column]
        link_duration = df[link_duration_column]
        within_greater_amount = (movie_duration >= ( link_duration - amount ))
        within_lesser_amount = (movie_duration <= ( link_duration + amount ))
        return  within_greater_amount & within_lesser_amount

    def year_within_range(self, df, amount, movie_year_column, link_year_column):
        movie_year = df[movie_year_column]
        link_year = df[link_year_column]
        within_greater_amount = (  movie_year >=  link_year - amount)
        within_lesser_amount = ( movie_year <= link_year + amount)
        return within_greater_amount & within_lesser_amount


    def exclude_links(self, df, link_dfs, link_url_column):
        for link in link_dfs:
            df = df.loc[~df[link_url_column].isin(link[link_url_column])]
        return df

    def apply_guessit(self):
        self.guessit_values = self.link_df.apply(self.guessit_data)
        guessit_df = pd.DataFrame(data=self.guessit_values)
        self.link_df = pd.merge(self.link_df, guessit_df, on='link_url')
    
    def guessit_data(self, row):
        guessit_object = self.get_guessit_object(row['fulltitle'])
        return {'link_url': row['link_url'], 'year': self.guessit_year(guessit_object), 
                'guessit_title':self.guessit_year(guessit_object) }

    def guessit_title(self, guessit_object):
        return guessit_object['title']
    
    def guessit_year(self, guessit_object):
        return guessit_object['year']

    def get_guessit_object(self, title):
        return api.guessit(title)

    def set_link_df(self):
        self.link_df = pd.read_csv(self.all_links_path)

    def scrape_okru_for_top_movies(self, use_proxy=False):
        scraper_progress_path = self.get_scraper_progress_path(OkruScraper)
        scraping_directory = self.get_scraping_directory(OkruScraper)
        ScrapeAllImdb(self.instances, self.imdb_directory, self.chromedriver_path,
                                                scraper_progress_path, OkruScraper, scraping_directory,
                                                only_tconsts=self.only_tconsts,
                                                use_proxy=use_proxy).run_instances_scraper()
    
    def verify_links_and_extract_info(self):
        self.process_okru_object = ProcessOkruLinks(only_tconsts=self.only_tconsts, 
                                                    data_path='./top_movie_links.csv')
        self.process_okru_object.update_okru_extra_info()
        data = self.process_data(self.process_okru_object.data)
        df = pd.DataFrame(data=data)
        movie_file = pd.read_csv(self.merged_movie_title_path)
        link_file = pd.read_csv(self.everything_path)
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
            if not empty_column_value(movie, 'language'):
                return movie.get('language')
            else:
                return self.get_movie_language(self, movie)
        except:
            return None

    def get_movie_tconst(self, movie):
        return self.cinemagoer_object.get_movie(movie.get('tconst').split('tt')[-1])

    def get_movie_language(self, movie):
        return self.get_movie_tconst(self, movie).guessLanguage()

    def add_top_dubbed_info(self):
        self.process_okru_object.update_dubbed_data()

    def run_generate_top_movies(self):
        self.update_top_movie_object.run_and_generate_all()
        self.set_tconsts()

    def set_tconsts(self):
        tconsts_paths = [
            # self.update_top_movie_object.top_indian_destination, 
                         self.update_top_movie_object.top_imdb_destination, 
                         self.update_top_movie_object.top_popular_destination]
        dfs = [pd.read_csv(path) for path in tconsts_paths]
        genre_df = pd.read_csv(self.update_top_movie_object.genres_destination)
        genre_df['tconst'] = genre_df['movie']
        dfs.append(genre_df)
        self.only_tconsts = np.unique(pd.concat(dfs)['tconst'])
    

