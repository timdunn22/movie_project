import pandas as pd
from common_methods import ( reset_and_copy, get_soup_url, minutes_from_string, sample,
                            divide_chunks, listdir_nohidden, get_list_proxies, empty_column_value,
                            try_except_method, not_empty_column, flatten, get_soup_url_w_proxy)
import json
import threading
import time
import os
import numpy as np
from guessit import api
from imdb import Cinemagoer
from lingua import Language, LanguageDetectorBuilder
import asyncio
from all_processes.load_yaml_vars import LoadYamlVars

class BasicProcessOkru(LoadYamlVars):

    def __init__(self, yaml_file_path, instances=5):
        super().__init__(yaml_file_path=yaml_file_path)
        self.proxies = None
        self.df = None
        self.final_df = None
        self.instances = instances
        self.links = pd.read_csv(self.merged_links_path, usecols=['link_url', 'movie'])
        self.links = reset_and_copy(self.links[self.links['link_url'].str.contains('ok.ru') 
                                               & ~self.links['movie'].isnull()])
        self.filter_links()
        self.dfs = self.get_dfs()

    def get_proxies(self):
        return [{'host': row['host'], 'port': row['port']} for index, row in pd.read_csv(self.proxies_path).iterrows()]

    def get_all_info(self, row, current_proxy):
        return ProcessOkruLink(row, current_proxy).get_all_info()

    def get_directory_df(self, file):
        try:
            return pd.read_csv(file)
        except:
            return None

    def get_directory_dfs(self):
        return [self.get_directory_df(file) for file in listdir_nohidden(self.links_extra_progress_path) 
                if self.get_directory_df(file) is not None]

    def set_final_df(self):
        self.final_df = self.get_combined_data()

    def filter_links(self):
        self.set_final_df()
        self.links = self.links[~self.links['link_url'].isin(self.final_df['link_url'])]

    def get_combined_data(self):
        dfs = self.get_directory_dfs()
        try:
            final_df = pd.read_csv(self.links_extra_path)
        except:
            pass
        if dfs:
            main_df = None
            try:
                main_df = pd.read_csv(self.links_extra_path)
                dfs.append(main_df)
            except:
                pass
            final_df = pd.concat(dfs)
            final_df.drop_duplicates(inplace=True)
            final_df.to_csv(self.links_extra_path, index=False)
        return final_df

    def process_links(self, instance_id, links, proxies):
        data = list()
        starting_time = int(time.time())
        for index, row in links.iterrows():
            current_proxy = sample(proxies, 1)[0]
            try:
                current_data = self.get_all_info(row, current_proxy)
                data.append(current_data)
                if index % 1000 == 0:
                    print(index)
                    self.save_checkpoint_data(instance_id, data, starting_time)
            except Exception as e:
                print('there was an exception')
                print(e)
                self.switch_out_proxy(current_proxy)
                self.save_checkpoint_data(instance_id, data, starting_time)
        self.save_checkpoint_data(instance_id, data, starting_time)

    def save_checkpoint_data(self, instance_id, data, starting_time):
        pd.DataFrame(data=data).to_csv(f'{self.links_extra_progress_path}{instance_id}_{starting_time}.csv', index=False)

    def switch_out_proxy(self, current_proxy):
        self.proxies = [proxy for proxy in self.proxies if proxy != current_proxy]

    def process_links_async(self):
        thread_list = list()
        self.proxies = self.get_proxies()
        for instance_id in range(self.instances):
            thread = threading.Thread(name='Test {}'.format(instance_id), target=self.process_links,
                                      args=(instance_id, self.dfs[instance_id], self.proxies))
            thread_list.append(thread)
            thread.start()
            print(thread.name + ' started!')
        for thread in thread_list:
            thread.join()

    def remove_unncessary_files(self):
        files = listdir_nohidden(self.links_extra_progress_path)
        for file in files:
            os.remove(file) 

    def process_and_combine(self):
        self.process_links_async()
        self.set_final_df()
        self.remove_unncessary_files()

    def get_dfs(self):
        return [self.links.loc[indexes] for indexes in divide_chunks(self.links.index,
                                                                      round(self.links.index.size / self.instances))]


class ProcessOkruLink:

    def __init__(self, row, current_proxy):
        self.link = row['link_url']
        self.soup = get_soup_url_w_proxy(self.link, current_proxy)
    
    def get_quality(self):
        try:
            div = self.soup.find('div', {'class': "vid-card_cnt h-mod"})
            div_object = json.loads( div.get('data-options'))
            flashvars = div_object['flashvars']
            metadata = json.loads(flashvars['metadata'])
            return metadata['videos'][-1]['name']
        except:
            return None

    def get_description(self):
        try:
            div = self.soup.find('div', {"class": 'vp-layer-description'})
            return div.text
        except:
            return None
    
    def get_duration(self):
        try:
            div = self.soup.find('div', {'class': "vid-card_cnt h-mod"})
            return div.find('div', {'class': 'vid-card_duration'}).text
        except:
            return None

    def get_all_info(self):
        try:
            description = self.get_description()
            quality = self.get_quality()
            return {'link_url': self.link, 
                    'description': description, 
                    'quality': quality, 
                    'duration': minutes_from_string(self.get_duration()),
                    'active': (quality is not None)}
        except:
            return {'link_url': self.link, 'active': False}


class GuessAudio:

    def __init__(self, link):
        self.link = link   
        self.description = self.link['description']
        self.link_title = self.link['fulltitle']
        languages = Language.__members__.values()
        self.detector = LanguageDetectorBuilder.from_languages(*languages).build()
        self.cinemagoer_object = Cinemagoer()

    def get_frequent_language(self, title):
        title_language_objects = self.detect_language_objects(title)
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

    def detect_language_objects(self, title):
        return self.detector.detect_multiple_languages_of(title)

    def detect_language(self, title):
        return try_except_method(np.unique, [result.language.name.lower().capitalize() 
                        for result in self.detect_language_objects(title)], return_value=[])

    def description_language(self):
        return self.detect_language(self.description)

    def title_language(self):
        return self.detect_language(self.link_title)

    def get_original_language(self, movie):
        return movie.get('language') or self.cinemagoer_object.get_movie(self.movie_id(movie)).guessLanguage()

    def movie_id(self, movie):
        return movie.get('tconst').split('tt')[-1]

    def guessit_title(self):
        return api.guessit(self.link_title).get('title')

    def detect_audio_language(self):
        try:
            title_languages = self.title_language()
            description_languages = self.description_language()
            if not description_languages and title_languages:
                if len(title_languages) == 1:
                    if self.guessit_title() == self.link['primary_title']:
                        return self.get_original_language(self.link)
                    return title_languages[0]
                elif len(title_languages) == 2:
                    language = self.get_original_language(self.link)
                    if language in title_languages:
                        return [lan for lan in title_languages if lan != language][0]
            else:
                descriptions_max = self.get_frequent_language(self.description)
                if not descriptions_max:
                    common_languages = list(set(description_languages).intersection(title_languages))
                    if len(common_languages) == 1:
                        return common_languages[0]
                    elif len(title_languages) == 1 and (title_languages[0] in common_languages):
                        return title_languages[0]
                    return self.get_frequent_language(self.description)
                return descriptions_max
        except:
            print(self.link['fulltitle'])
            print('went into exception')
            return None

def main():
    process_object = BasicProcessOkru(yaml_file_path='/Users/timdunn/movie_project/all_processes/movie_configuration.yaml')
    process_object.process_and_combine()
if __name__ == '__main__':
    main()
