import os
os.environ['DJANGO_SETTINGS_MODULE'] = 'movies.settings'
os.environ['DJANGO_ALLOW_ASYNC_UNSAFE'] = '1'
import django
django.setup()
from common_methods import *
from all_processes.convert_imdb_data import ConvertImdbData
from all_processes.update_movie_data import UpdateMovieData
from all_processes.update_extra_imdb_info import UpdateExtraImdbInfo
from all_processes.load_yaml_vars import LoadYamlVars
from common_methods import nonnull_columns
from movies.models import Movie
from all_processes.update_popular_imdb import UpdatePopularImdb


class ProcessImdbData(LoadYamlVars):

    def __init__(self, configuration_file, force_update=False):
        super().__init__(configuration_file)
        self.merged_df = None
        self.force_update = force_update
        self.update_top_data = UpdatePopularImdb()

    def convert_basic_imdb_data(self):
        ConvertImdbData(self.imdb_directory, self.output_data_directory).convert_basic_imdb_data()

    def download_imdb_data(self):
        UpdateMovieData(self.imdb_directory).get_latest_imdb()

    def get_converted_title_data(self):
        return pd.read_csv(self.converted_movie_title_path)

    def get_converted_extra_data(self):
        return pd.read_csv(self.extra_data_output_path)

    def convert_imdb_extra_data(self):
        UpdateExtraImdbInfo(configuration=self.configuration, force_update=self.force_update).convert_extra_data()

    def merge_basic_extra_data(self):
        self.merged_df = pd.merge(self.get_converted_title_data(), self.get_converted_extra_data(),
                                  how='left', on='tconst')

    def convert_merged_data_cols(self):
        integer_columns = [column.attname for column in Movie._meta.get_fields() if
                           'IntegerField' in str(column.__class__)]
        for column in integer_columns:
            self.merged_df.loc[~nonnull_columns(self.merged_df, column), column] = 0

    def download_top_data(self):
        self.update_top_data.run_and_generate_all()

    def merge_top_data(self):
        top_data_destinations = [self.update_top_data.top_indian_destination, 
                                 self.update_top_data.top_imdb_destination, 
                                 self.update_top_data.top_popular_destination]
        for destination in top_data_destinations:
            self.merged_df = pd.merge(self.merged_df, pd.read_csv(destination), on="tconst", how="left")

    def save_merged_data(self):
        self.merged_df.to_csv(self.merged_movie_title_path, index=False)

    def get_convert_all_imdb_data(self):
        self.download_imdb_data()
        self.convert_basic_imdb_data()
        self.convert_imdb_extra_data()
        self.merge_basic_extra_data()
        self.download_top_data()
        self.convert_merged_data_cols()
        self.save_merged_data()
