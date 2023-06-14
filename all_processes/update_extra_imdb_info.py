import pandas as pd

from all_processes.update_movie_file import (UpdateSoundmixFile, UpdateCountryFile, UpdateVideosFile,
                                             UpdateKeywordsFile, UpdateCertificateFile,
                                             UpdateBoxOfficeFile, UpdateGenreFile)

from all_processes.load_yaml_vars import LoadYamlVars
from common_methods import camel_to_snake, reset_and_copy


class UpdateExtraImdbInfo(LoadYamlVars):

    def __init__(self, configuration, force_update=False):
        super().__init__(configuration=configuration)
        self.extra_data_df = None
        self.output_columns = list()
        self.force_update = force_update
        self.class_objects = [UpdateSoundmixFile, UpdateCountryFile, UpdateVideosFile,
                              UpdateKeywordsFile, UpdateCertificateFile, UpdateBoxOfficeFile]

    def get_path_reference(self, class_object):
        class_name = class_object.__name__
        file_name = camel_to_snake("".join(class_name.split("Update")[-1].split("File")[0:-1]))
        return f'{self.output_data_directory}{file_name}.csv'

    def make_file(self, class_object):
        return class_object(self.extra_data_path, self.get_path_reference(class_object), 
                            force_update=self.force_update).make_file()

    def convert_other_file_extra_data(self):
        for class_object in self.class_objects:
            self.make_file(class_object)

    def get_basic_extra_data(self):
        self.extra_data_df = reset_and_copy(self.extra_data_df.loc[:, ['original air date', 'tconst', 'language',
                                                                       'color', 'score', 'poster', 'plot outline',
                                                                       'aspect ratio', 'production status',
                                                                       'production status updated', 'bottom 100 rank',
                                                                       'top 250 rank', 'box office_Budget',
                                                                       'box office_Cumulative Worldwide Gross']])

    def set_extra_data_df(self):
        self.extra_data_df = pd.read_csv(self.extra_data_path)

    def convert_extra_data(self):
        self.convert_other_file_extra_data()
        self.set_extra_data_df()
        self.get_basic_extra_data()
        self.convert_basic_extra_data()
        self.save_converted_extra_data()
        self.convert_genres()

    def convert_genres(self):
        input_path = f'{self.imdb_directory}title.basics.tsv'
        output_path = f'{self.output_data_directory}genres.csv'
        UpdateGenreFile(input_path, output_path, force_update=self.force_update).make_file()

    def save_converted_extra_data(self):
        reset_and_copy(self.extra_data_df.loc[:, self.output_columns]).to_csv(self.extra_data_output_path, index=False)

    def convert_basic_extra_data(self):
        for column in self.extra_data_df.columns:
            new_column = "_".join(column.lower().split())
            self.extra_data_df[new_column] = self.extra_data_df[column]
            self.output_columns.append(new_column)
