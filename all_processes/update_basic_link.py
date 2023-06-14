from common_methods import minutes_from_string, reset_and_copy, nonnull_columns, get_year
import pandas as pd


class UpdateBasicLink:

    def __init__(self, file_path, output_columns, url_column='link url', title_column='movie_title', year_column='year',
                 apply_function=minutes_from_string, duration_column='duration'):
        self.file_path = file_path
        self.output_columns = output_columns
        self.df = None
        self.apply_function = apply_function
        self.title_column = title_column
        self.year_column = year_column
        self.url_column = url_column
        self.duration_column = duration_column

    def convert_links(self):
        self.set_df()
        self.set_urls()
        self.set_years()
        self.set_titles()
        self.set_durations()
        self.set_resolutions()
        self.reset_df()
        return self.df

    def set_years(self):
        if self.column_available(self.year_column):
            self.df['year'] = self.df[self.year_column]

    def read_df(self):
        self.df = pd.read_csv(self.file_path)

    def set_df(self):
        self.read_df()
        self.drop_duplicate_links()
        self.remove_missing_links()

    def drop_duplicate_links(self):
        self.df.drop_duplicates(subset=[self.url_column], inplace=True)

    def remove_missing_links(self):
        self.df = reset_and_copy(self.df.loc[nonnull_columns(self.df, self.url_column)])

    def set_urls(self):
        self.df['link_url'] = self.df[self.url_column]

    def set_titles(self):
        self.df['fulltitle'] = self.df[self.title_column]

    def column_available(self, column):
        return column in self.df.columns

    def set_durations(self):
        if self.column_available('duration'):
            self.df['duration'] = self.df['duration'].apply(self.apply_function)

    def set_resolutions(self):
        if self.column_available('quality'):
            self.df['resolution'] = self.df['quality'].apply(self.convert_resolutions)

    def convert_resolutions(self, quality):
        quality = str(quality).lower()
        if 'hd' in quality:
            return '1920 x 1080'
        elif '720' in quality:
            return '1080 x 720'
        elif '4k' in quality:
            return '3840 x 2160'
        else:
            return '720 x 480'

    def reset_df(self):
        self.df = reset_and_copy(self.df.loc[:, self.output_columns])


class ConvertGoLinks(UpdateBasicLink):

    def __init__(self, go_file_path):
        super().__init__(go_file_path, ['link_url', 'fulltitle', 'duration', 'year', 'quality', 'resolution'],
                         apply_function=self.convert_durations)

    def convert_durations(self, duration):
        try:
            return duration.split(" min")[0]
        except:
            return None


class ConvertLookLinks(UpdateBasicLink):

    def __init__(self, look_file_path):
        super().__init__(look_file_path, ['link_url', 'fulltitle', 'year'])


class ConvertMembedLinks(UpdateBasicLink):

    def __init__(self, membed_file_path):
        super().__init__(membed_file_path, ['link_url', 'fulltitle'])


class ConvertChillLinks(UpdateBasicLink):

    def __init__(self, chill_file_path):
        super().__init__(chill_file_path, ['link_url', 'fulltitle', 'image_url', 'quality', 'year', 'resolution'])


class ConvertPrimeLinks(UpdateBasicLink):

    def __init__(self, prime_file_path):
        super().__init__(prime_file_path, ['link_url', 'fulltitle', 'duration', 'quality', 'year', 'resolution'],
                         apply_function=self.convert_durations)

    def convert_durations(self, duration):
        try:
            return duration.split("m")[0]
        except:
            return None


class ConvertVexLinks(UpdateBasicLink):

    def __init__(self, vex_file_path):
        super().__init__(vex_file_path, ['link_url', 'fulltitle', 'duration', 'year'],
                         apply_function=self.convert_durations)

    def convert_durations(self, duration):
        try:
            return duration.split("m")[0]
        except:
            return None


class ConvertSoapLinks(UpdateBasicLink):

    def __init__(self, soap_file_path):
        super().__init__(soap_file_path, ['link_url', 'fulltitle', 'duration', 'quality', 'year', 'resolution'],
                         apply_function=self.convert_durations)

    def convert_durations(self, duration):
        if ' min' in str(duration):
            return duration.split(" min")[0]
        return duration


class ConvertSolarLinks(UpdateBasicLink):

    def __init__(self, solar_file_path):
        super().__init__(solar_file_path, ['link_url', 'fulltitle'])


class ConvertStreamlordLinks(UpdateBasicLink):

    def __init__(self, streamlord_path):
        super().__init__(streamlord_path, ['link_url', 'fulltitle', 'duration', 'year'],
                         apply_function=self.convert_durations)

    def convert_durations(self, duration):
        try:
            return duration.split(" min.")[0]
        except:
            return None


class ConvertYesMovieLinks(UpdateBasicLink):

    def __init__(self, ye_path):
        super().__init__(ye_path, ['link_url', 'fulltitle', 'year'])


class ConvertTubiLinks(UpdateBasicLink):

    def __init__(self, yes_path):
        super().__init__(yes_path, ['link_url', 'fulltitle', 'year'], year_column='movie_year', url_column='url')


class ConvertOkruLinks(UpdateBasicLink):

    def __init__(self, okru_file_path):
        super().__init__(okru_file_path, ['link_url', 'fulltitle', 'duration', 'year'],
                         title_column='link title', duration_column='movie duration')

    def set_years(self):
        self.df['year'] = self.df[self.title_column].apply(get_year)

    def read_df(self):
        self.df = pd.read_csv(self.file_path, usecols=['link title', 'link url', 'movie duration', 'duration', 'year'])

    def set_urls(self):
        self.df['link_url'] = self.df[self.url_column].apply(lambda url: f'https://ok.ru/video/{int(url)}')


class ConvertSwatchLinks(UpdateBasicLink):

    def __init__(self, swatch_file_path):
        super().__init__(swatch_file_path, ['link_url', 'fulltitle', 'duration',
                                            'quality', 'year', 'description', 'resolution'],
                         apply_function=self.convert_durations)

    def convert_durations(self, duration):
        try:
            return int(duration)
        except:
            return duration


class ConvertSflixLinks(UpdateBasicLink):

    def __init__(self, sflix_path):
        super().__init__(sflix_path, ['link_url', 'fulltitle', 'quality', 'year', 'resolution'])

    def set_years(self):
        self.df['year'] = self.df[self.year_column].apply(self.convert_to_int)

    def convert_to_int(self, column):
        try:
            return int(column)
        except:
            return column


class ConvertLinkDetails(UpdateBasicLink):

    def __init__(self, detail_path):
        super().__init__(detail_path, ['link_url', 'fulltitle', 'resolution', 'fps'],
                         url_column='webpage_url', title_column='fulltitle')

    def set_converted_resolution(self, row):
        try:
            return f'{int(row["height"])} x {int(row["width"])}'
        except:
            return None

    def set_resolutions(self):
        self.df['resolution'] = self.df.apply(self.set_converted_resolution)

    def read_df(self):
        self.df = pd.read_csv(self.file_path, usecols=["webpage_url", "fps", "width", "height", "fulltitle"])


class ConvertTinyZoneLinks(UpdateBasicLink):

    def __init__(self, tinyzone_path):
        super().__init__(tinyzone_path, ['link_url', 'fulltitle', 'quality', 'year', 'resolution', 'duration'],
                         apply_function=self.convert_to_int)

    def set_years(self):
        self.df['year'] = self.df[self.year_column].apply(self.convert_to_int)

    def convert_to_int(self, column):
        try:
            return int(column)
        except:
            return column


class ConvertLosMoviesLinks(UpdateBasicLink):

    def __init__(self, los_movies_path):
        super().__init__(los_movies_path, ['link_url', 'fulltitle', 'quality', 'year', 'resolution'])

    def set_years(self):
        self.df['year'] = self.df[self.year_column].apply(self.convert_to_int)

    def set_urls(self):
        self.df['link_url'] = self.df[self.url_column].apply(lambda url: f'https://losmovies.pics{url}')

    def convert_to_int(self, column):
        try:
            return int(column)
        except:
            return column

