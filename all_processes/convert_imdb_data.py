from common_methods import *
import humps


class ConvertImdbData:

    def __init__(self, imdb_directory, imdb_import_data_directory):
        self.imdb_import_data_directory = imdb_import_data_directory
        self.imdb_directory = imdb_directory
        self.basic_data_classes = [ConvertRatingsData, ConvertAkaData, ConvertMovieCrewData,
                                   ConvertCrewData, ConvertTitleData]

    def convert_basic_imdb_data(self):
        for convert_data_class in self.basic_data_classes:
            convert_data_class(output_file_path=self.get_output_file_path(convert_data_class),
                               imdb_basic_directory=self.imdb_directory).convert_data()

    def get_output_file_path(self, convert_data_class):
        class_name = convert_data_class.__name__
        file_name = humps.decamelize(class_name.split('Convert')[-1].split('Data')[0])
        return f'{self.imdb_import_data_directory}{file_name}.csv'


class ConvertBasicImdb:

    def __init__(self, file_name, output_file_path, imdb_basic_directory, output_columns):
        self.df = None
        self.output_file_path = output_file_path
        self.file_name = file_name
        self.imdb_basic_directory = imdb_basic_directory
        self.output_columns = output_columns
        self.basic_columns = None

    def set_starting_df(self):
        self.df = pd.read_csv(f'{self.imdb_basic_directory}{self.file_name}', sep="\t")

    def set_basic_columns(self):
        self.basic_columns = [column for column in self.output_columns if column in self.converted_columns()]

    def converted_columns(self):
        return [humps.decamelize(column) for column in self.df.columns]

    def save_output_df(self):
        self.df.to_csv(self.output_file_path, index=False)

    def convert_columns(self):
        for column in self.output_columns:
            if column in self.basic_columns:
                self.df[column] = self.df[humps.camelize(column)]

    def reset_df(self):
        self.df = reset_and_copy(self.df.loc[:, self.output_columns])

    def set_null_columns(self):
        for col in self.df.columns:
            self.df.loc[self.df[col] == '\\N', col] = None

    def convert_data(self):
        self.set_starting_df()
        self.set_basic_columns()
        self.convert_columns()
        self.reset_df()
        self.set_null_columns()
        self.save_output_df()


class ConvertCrewData(ConvertBasicImdb):

    def __init__(self, output_file_path, imdb_basic_directory):
        super().__init__('name.basics.tsv', output_file_path=output_file_path,
                         imdb_basic_directory=imdb_basic_directory, output_columns=["nconst", "primary_name",
                                                                                    "birth_year", "death_year",
                                                                                    "primary_profession"])


class ConvertTitleData(ConvertBasicImdb):

    def __init__(self, output_file_path, imdb_basic_directory):
        super().__init__('title.basics.tsv', output_file_path=output_file_path,
                         imdb_basic_directory=imdb_basic_directory, output_columns=["tconst", "title_type",
                                                                                    "primary_title", "original_title",
                                                                                    "is_adult", "start_year",
                                                                                    "runtime_minutes", "end_year"])


class ConvertDataWMovie(ConvertBasicImdb):

    def __init__(self, input_file_name, output_file_path, imdb_basic_directory, output_columns):
        super().__init__(input_file_name, output_file_path=output_file_path,
                         imdb_basic_directory=imdb_basic_directory,
                         output_columns=output_columns)

    def convert_columns(self):
        super().convert_columns()
        self.df['movie'] = self.df['tconst']


class ConvertMovieCrewData(ConvertDataWMovie):

    def __init__(self, output_file_path, imdb_basic_directory):
        super().__init__('title.principals.tsv', output_file_path=output_file_path,
                         imdb_basic_directory=imdb_basic_directory, output_columns=["movie", "ordering", "crew",
                                                                                    "category", "job", "characters"])

    def convert_columns(self):
        super().convert_columns()
        self.df['crew'] = self.df['nconst']


class ConvertRatingsData(ConvertDataWMovie):

    def __init__(self, output_file_path, imdb_basic_directory):
        super().__init__('title.ratings.tsv', output_file_path=output_file_path,
                         imdb_basic_directory=imdb_basic_directory, output_columns=["movie", "average_rating",
                                                                                    "num_votes"])


class ConvertAkaData(ConvertBasicImdb):

    def __init__(self, output_file_path, imdb_basic_directory):
        super().__init__('title.akas.tsv', output_file_path=output_file_path,
                         imdb_basic_directory=imdb_basic_directory,
                         output_columns=["movie", "ordering", "title", "region",
                                         "language", "types", "attributes", "is_original_title"])

    def convert_columns(self):
        super().convert_columns()
        self.df['movie'] = self.df['titleId']
