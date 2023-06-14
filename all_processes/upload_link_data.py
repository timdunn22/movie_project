from common_methods import reset_and_copy, convert_xy_columns
import pandas as pd
from all_processes.load_yaml_vars import LoadYamlVars
from all_processes.update_basic_link import (ConvertGoLinks, ConvertStreamlordLinks, ConvertTinyZoneLinks,
                                             ConvertChillLinks, ConvertLookLinks, ConvertPrimeLinks,
                                             ConvertOkruLinks, ConvertSflixLinks, ConvertSoapLinks,
                                             ConvertSolarLinks, ConvertTubiLinks, ConvertMembedLinks,
                                             ConvertSwatchLinks, ConvertVexLinks, ConvertYesMovieLinks,
                                             ConvertLinkDetails, ConvertLosMoviesLinks)


class UploadLinkData(LoadYamlVars):

    def __init__(self, configuration_file):
        super().__init__(configuration_file)
        self.link_details_path = self.links_path.get('Links Unconverted Path')
        self.link_details_df = None
        self.all_links_df = None
        self.merged_links_df = None
        self.dubbed_df = None
        self.links_imdb_df = None
        self.merged_imdb_path = self.everything_path
        self.links_dfs = list()
        self.convert_link_classes = [ConvertGoLinks, ConvertStreamlordLinks, ConvertChillLinks,
                                     ConvertLookLinks, ConvertPrimeLinks, ConvertOkruLinks, ConvertSflixLinks,
                                     ConvertSoapLinks, ConvertSolarLinks, ConvertTubiLinks, ConvertMembedLinks,
                                     ConvertSwatchLinks, ConvertVexLinks, ConvertYesMovieLinks, ConvertTinyZoneLinks,
                                     ConvertLosMoviesLinks]

    def convert_link_details(self):
        self.link_details_df = ConvertLinkDetails(self.link_details_path).convert_links()

    def merge_links_w_link_details(self):
        self.merged_links_df = pd.merge(self.link_details_df, self.all_links_df, how='outer', on='link_url')
        self.merged_links_df.drop_duplicates(subset=['link_url'], inplace=True)

    def convert_all_links(self):
        for convert_class in self.convert_link_classes:
            link_path = self.get_link_path(convert_class)
            self.links_dfs.append(convert_class(link_path).convert_links())

    def get_link_path(self, class_object):
        class_name = class_object.__name__
        scraper_class_name = class_name.split('Convert')[-1].split("Links")[0]
        return self.scraper_paths.get(scraper_class_name).get('Combined Data Path')

    def set_all_links(self):
        self.all_links_df = pd.concat(self.links_dfs).drop_duplicates(subset=['link_url'])

    def load_merged_df(self):
        self.links_imdb_df = pd.read_csv(self.everything_path, usecols=['tconst', 'link url'])

    def convert_merged_df(self):
        self.links_imdb_df['movie'] = self.links_imdb_df['tconst']
        self.links_imdb_df['link_url'] = self.links_imdb_df['link url'].apply(self.convert_merged_link_url)
        self.links_imdb_df.drop_duplicates(subset=['link_url'], inplace=True)
        self.links_imdb_df = reset_and_copy(self.links_imdb_df.loc[:, ['movie', 'link_url']])

    def convert_merged_link_url(self, url):
        if 'http' in str(url):
            return url
        elif 'free' in str(url):
            return f'https://losmovies.pics{url}'
        else:
            try:
                return f'https://ok.ru/video/{int(float(url))}'
            except:
                return url

    def save_link_data(self):
        self.merged_links_df.to_csv(self.merged_links_path, index=False)

    def save_merged_data(self):
        self.merged_links_df.to_csv(self.merged_links_path, index=False)

    def set_merge_links(self):
        self.convert_all_links()
        self.set_all_links()
        self.convert_link_details()
        self.merge_links_w_link_details()
        self.set_dubbed_df()
        self.merge_links_w_dubbed()

    def set_dubbed_df(self):
        self.dubbed_df = pd.read_csv(self.dubbed_path)
        self.convert_dubbed_df()
        self.dubbed_df.drop_duplicates(subset=['link_url'], inplace=True)

    def convert_dubbed_df(self):
        self.dubbed_df['link_url'] = self.dubbed_df['link']
        self.dubbed_df['audio_language'] = self.dubbed_df['language']
        self.dubbed_df['audio_language_probability'] = self.dubbed_df['probability']

    def merge_links_w_dubbed(self):
        self.merged_links_df = pd.merge(self.merged_links_df, self.dubbed_df, how='left', on='link_url')

    def merge_link_data_imdb(self):
        self.merged_links_df = pd.merge(self.merged_links_df, self.links_imdb_df, how='left', on='link_url')

    def drop_imdb_link_useless_columns(self):
        self.merged_links_df.drop(['link', 'language', 'probability'], inplace=True, axis=1)

    def convert_merged_link_data(self):
        self.drop_imdb_link_useless_columns()
        convert_xy_columns(self.merged_links_df)

    def set_all_link_data(self):
        self.set_merge_links()
        self.save_link_data()
        self.load_merged_df()
        self.convert_merged_df()
        self.merge_link_data_imdb()
        self.convert_merged_link_data()
        self.save_merged_data()
