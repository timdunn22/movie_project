from common_methods import *
from update_popular_imdb import UpdatePopularImdb


class UpdateMovieFile:

    def __init__(self, file_path, movie_import_path, force_update=False):
        self.extra_movie_path = file_path
        self.movie_import_path = movie_import_path
        self.extra_movie_df = None
        self.movie_import_df = None
        self.force_update = force_update

    def set_extra_movie_data(self):
        self.extra_movie_df = pd.read_csv(self.extra_movie_path)

    def set_movie_import_df(self):
        self.movie_import_df = pd.read_csv(self.movie_import_path)

    def make_file(self):
        self.set_extra_movie_data()
        if self.force_update:
            self.save_import_to_file()
        elif not self.import_file_updated():
            self.save_import_to_file()
        self.set_movie_import_df()

    def save_import_to_file(self):
        self.create_import_df().to_csv(self.movie_import_path, index=False)

    def import_file_updated(self):
        if os.path.exists(self.movie_import_path) and os.path.exists(self.extra_movie_path):
            return file_updated_time(self.movie_import_path) > file_updated_time(self.extra_movie_path)
        return False

    def get_data(self, row):
        pass

    def convert_data(self):
        return flatten(self.extra_movie_df.apply(lambda row: self.get_data(row), axis=1))

    def create_import_df(self):
        return pd.DataFrame(data=self.convert_data()).drop_duplicates()


class UpdateSingletonFile(UpdateMovieFile):

    def __init__(self, extra_data_file_path, file_path, extra_data_column, output_column, force_update=False):
        super().__init__(file_path=extra_data_file_path, movie_import_path=file_path, force_update=force_update)
        self.extra_data_column = extra_data_column
        self.output_column = output_column

    def get_data_row_item(self, row, item_index, item_value):
        return {self.output_column: item_value, 'ordering': item_index + 1, 'movie': row['tconst']}

    def column_populated(self, row):
        return not empty_column_value(row, self.extra_data_column)

    def get_data(self, row):
        if self.column_populated(row):
            return [self.get_data_row_item(row, index, item)
                    for index, item in enumerate(str(row[self.extra_data_column]).split(','))]
        return list()


class UpdateManyFile(UpdateMovieFile):

    def __init__(self, extra_data_file_path, file_path, col_split_string, force_update=False):
        super().__init__(file_path=extra_data_file_path, movie_import_path=file_path, force_update=force_update)
        self.col_split_string = col_split_string

    def get_import_columns(self):
        return [column for column in self.extra_movie_df.columns if self.col_split_string in column]

    def get_data_row_item(self, row, col):
        return {**self.get_movie_data_row(row), **self.get_other_data_row(row, col)}

    def get_other_data_row(self, row, col):
        pass

    def get_movie_data_row(self, row):
        return {'movie': row['tconst']}

    def get_data(self, row):
        return [self.get_data_row_item(row, col) for col in self.get_import_columns()
                if not empty_column_value(row, col)]


class UpdateKeywordsFile(UpdateSingletonFile):

    def __init__(self, extra_data_file_path, keyword_file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=keyword_file_path,
                         extra_data_column='keywords', output_column='keyword', force_update=force_update)

    def column_populated(self, row):
        return super().column_populated(row) and row[self.extra_data_column] != 'no keywords'


class UpdateVideosFile(UpdateSingletonFile):

    def __init__(self, extra_data_file_path, video_file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=video_file_path,
                         extra_data_column='videos', output_column='video_url', force_update=force_update)


class UpdateGenreFile(UpdateSingletonFile):

    def __init__(self, extra_data_file_path, genre_file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=genre_file_path,
                         extra_data_column='genres', output_column='genre', force_update=force_update)
        self.update_popular_imdb_object = UpdatePopularImdb()

    def set_extra_movie_data(self):
        self.extra_movie_df = pd.read_csv(self.extra_movie_path, sep='\t')
    
    def set_movie_import_df(self):
        super().set_movie_import_df()
        popular_genre_df = self.get_popular_genre_df
        self.movie_import_df = pd.merge(self.movie_import_df, popular_genre_df, on="movie", how="left")

    def get_popular_genre_df(self):
        try:
            return pd.read_csv(self.update_popular_imdb_object.genres_destination)
        except:
            self.update_popular_imdb_object.generate_genre_csv()
            self.update_popular_imdb_object.get_all_genres()
            return pd.read_csv(self.update_popular_imdb_object.genres_destination)
            


class UpdateBoxOfficeFile(UpdateManyFile):

    def __init__(self, extra_data_file_path, file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=file_path,
                         col_split_string='box office_Opening', force_update=force_update)

    def get_other_data_row(self, row, col):
        return {"country": self.get_country(col), 'amount': row[col]}

    def get_country(self, col):
        return col.split('box office_Opening Weekend ')[-1]


class UpdateCountryFile(UpdateManyFile):

    def __init__(self, extra_data_file_path, file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=file_path,
                         col_split_string='countries_', force_update=force_update)

    def get_other_data_row(self, row, col):
        return {"name": row[col], 'code': row[self.get_country_code_col(col)]}

    def get_movie_data_row(self, row):
        return dict()

    def get_country_code_col(self, col):
        country_number = col.split('countries_')[-1]
        return f'country codes_{country_number}'


class UpdateMovieCountryFile(UpdateManyFile):

    def __init__(self, extra_data_file_path, file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=file_path,
                         col_split_string='countries_', force_update=force_update)

    def get_other_data_row(self, row, col):
        return {"name": row[col]}


class UpdateCertificateFile(UpdateManyFile):

    def __init__(self, extra_data_file_path, file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=file_path,
                         col_split_string='certificates', force_update=force_update)

    def get_other_data_row(self, row, col):
        version = None
        if ":" in row[col]:
            col_splits = row[col].split(':')
            country = col_splits[0]
            certificate = col_splits[1]
            if len(col_splits) > 3:
                version = col_splits[3]
        else:
            country = None
            certificate = row[col]
        return {'certificate': certificate, 'country': country, 'version': version}


class UpdateSoundmixFile(UpdateManyFile):

    def __init__(self, extra_data_file_path, file_path, force_update=False):
        super().__init__(extra_data_file_path=extra_data_file_path, file_path=file_path,
                         col_split_string='sound mix', force_update=force_update)

    def get_other_data_row(self, row, col):
        return {"name": row[col]}
