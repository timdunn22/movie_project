from all_processes.load_yaml_vars import LoadYamlVars
import pandas as pd
import numpy as np
from common_methods import reset_and_copy, flatten


class ConvertSingleColumnData:

    def __init__(self, file_name, columns_info, output_file_directory):
        self.columns = None
        self.columns_info = columns_info
        self.dfs = list()
        self.df = None
        self.final_output_path = f'{output_file_directory}single_{file_name}.csv'

    def set_dfs_to_extract(self):
        self.dfs = [self.get_converted_df(column_info) for column_info in self.columns_info]

    def get_converted_df(self, column_info):
        df = self.get_df_from_column_info(column_info)
        return self.convert_df(df, column_info)

    def convert_df(self, df, column_info):
        column_translation = column_info.get('column_translation')
        extraction_columns = column_info.get('columns_to_extract')
        if column_translation is not None:
            for key in column_translation.keys():
                df[column_translation[key]] = df[key]
        return reset_and_copy(df.loc[:, extraction_columns])

    def get_df_from_column_info(self, column_info):
        return pd.read_csv(column_info.get('file_path'), usecols=self.get_extraction_columns(column_info))

    def get_extraction_columns(self, column_info) -> list:
        translated_columns = column_info.get('column_translation')
        extraction_columns = column_info.get('columns_to_extract')
        if translated_columns is not None:
            filtered_columns = [column for column in extraction_columns
                                if column not in list(translated_columns.values())]
            return flatten([filtered_columns, list(translated_columns.keys())])
        return extraction_columns

    def make_single_df(self):
        if len(self.columns_info) > 1:
            self.df = pd.concat(self.dfs).drop_duplicates(subset=self.columns)
        else:
            self.df = reset_and_copy(self.dfs[0]).drop_duplicates(subset=self.columns)

    def save_to_file(self):
        self.df.to_csv(self.final_output_path, index=False)

    def set_columns(self):
        self.columns = list(np.unique(flatten([columns.get('columns_to_extract') for columns in self.columns_info])))

    def convert_and_save_to_file(self):
        self.set_dfs_to_extract()
        self.set_columns()
        self.make_single_df()
        self.save_to_file()


class ConvertSingleLanguage(ConvertSingleColumnData):

    def __init__(self, converted_imdb_title_file, converted_link_file, converted_aka_file, output_file_directory):
        aka_column_info = {'file_path': converted_aka_file, 'columns_to_extract': ['name'],
                           'column_translation': {'language': 'name'}}
        link_column_info = {'file_path': converted_link_file, 'columns_to_extract': ['name'],
                            'column_translation': {'audio_language': 'name'}}
        imdb_column_info = {'file_path': converted_imdb_title_file, 'columns_to_extract': ['name'],
                            'column_translation': {'language': 'name'}}
        super().__init__(file_name='language', columns_info=[aka_column_info, link_column_info, imdb_column_info],
                         output_file_directory=output_file_directory)


class ConvertSingleGenres(ConvertSingleColumnData):

    def __init__(self, converted_genre_file, output_file_directory):
        genre_column_info = {'file_path': converted_genre_file, 'columns_to_extract': ['name'],
                             'column_translation': {'genre': 'name'}}
        super().__init__(file_name='genre', columns_info=[genre_column_info],
                         output_file_directory=output_file_directory)


class ConvertSingleRegion(ConvertSingleColumnData):

    def __init__(self, converted_aka_file, output_file_directory):
        region_column_info = {'file_path': converted_aka_file, 'columns_to_extract': ['name'],
                              'column_translation': {'region': 'name'}}
        super().__init__(file_name='region', columns_info=[region_column_info],
                         output_file_directory=output_file_directory)


class ConvertSingleCountry(ConvertSingleColumnData):

    def __init__(self, converted_box_office_file, converted_movie_country_file, output_file_directory):
        box_office_column_info = {'file_path': converted_box_office_file, 'columns_to_extract': ['name'],
                                  'column_translation': {'country': 'name'}}
        movie_country_column_info = {'file_path': converted_movie_country_file, 'columns_to_extract': ['name', 'code']}

        super().__init__(file_name='country', columns_info=[box_office_column_info, movie_country_column_info],
                         output_file_directory=output_file_directory)


class ConvertSingleCertificate(ConvertSingleColumnData):

    def __init__(self, converted_certificate_file, output_file_directory):
        certificate_column_info = {'file_path': converted_certificate_file, 'columns_to_extract': ['name'],
                                   'column_translation': {'certificate': 'name'}}
        super().__init__(columns_info=[certificate_column_info], file_name='certificate',
                         output_file_directory=output_file_directory)


class ConvertSingleKeyword(ConvertSingleColumnData):

    def __init__(self, converted_keyword_file, output_file_directory):
        keyword_column_info = {'file_path': converted_keyword_file, 'columns_to_extract': ['name'],
                               'column_translation': {'keyword': 'name'}}
        super().__init__(columns_info=[keyword_column_info], file_name='keyword',
                         output_file_directory=output_file_directory)


class ConvertSingleColumns(LoadYamlVars):

    def __init__(self, configuration_file):
        super().__init__(yaml_file_path=configuration_file)
        self.single_column_classes = [ConvertSingleLanguage, ConvertSingleGenres,
                                      ConvertSingleRegion, ConvertSingleCountry,
                                      ConvertSingleCertificate, ConvertSingleKeyword]

    def convert_all_data(self):
        for single_column_class in self.single_column_classes:
            self.convert_data(single_column_class)

    def convert_data(self, single_column_class):
        single_column_object = self.single_object_w_args().get(single_column_class)
        single_column_object.convert_and_save_to_file()

    def single_object_w_args(self):
        return {
            ConvertSingleLanguage: ConvertSingleLanguage(converted_link_file=self.merged_links_path,
                                                         converted_aka_file=self.converted_aka_path,
                                                         converted_imdb_title_file=self.merged_movie_title_path,
                                                         output_file_directory=self.output_data_directory),
            ConvertSingleGenres: ConvertSingleGenres(converted_genre_file=self.genre_path,
                                                     output_file_directory=self.output_data_directory),
            ConvertSingleRegion: ConvertSingleRegion(converted_aka_file=self.converted_aka_path,
                                                     output_file_directory=self.output_data_directory),
            ConvertSingleCountry: ConvertSingleCountry(converted_box_office_file=self.box_office_path,
                                                       converted_movie_country_file=self.country_path,
                                                       output_file_directory=self.output_data_directory),
            ConvertSingleCertificate: ConvertSingleCertificate(converted_certificate_file=self.certificate_path,
                                                               output_file_directory=self.output_data_directory),
            ConvertSingleKeyword: ConvertSingleKeyword(converted_keyword_file=self.keywords_path,
                                                       output_file_directory=self.output_data_directory)
        }
