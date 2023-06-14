from syncing_movie_data.create_movie_data import *
from common_methods import *


class UpdateMovieData:

    def __init__(self, imdb_directory='/Users/timdunn/Desktop/imdb_latest/'):
        self.imdb_directory = imdb_directory
        self.title_basics_path = "{}title.basics.tsv".format(self.imdb_directory)
        self.merged_imdb_path = "{}merged_imdb.csv".format(self.imdb_directory)
        self.title_akas_path = "{}title.akas.tsv".format(self.imdb_directory)
        self.imdb_data_url = 'https://datasets.imdbws.com/'
        self.genre_file_path = f'{self.imdb_directory}movie_genres.csv'
        self.converted_movie_basics_path = f'{self.imdb_directory}movie_converted.csv'
        self.extra_imdb_file = f'{self.imdb_directory}extra_imdb_data.csv'
        self.box_office_path = f'{self.imdb_directory}box_office.csv'
        self.country_file_path = f'{self.imdb_directory}countries.csv'
        self.certificate_file_path = f'{self.imdb_directory}certificates.csv'
        self.soundmix_file_path = f'{self.imdb_directory}soundmixes.csv'
        self.video_file_path = f'{self.imdb_directory}videos.csv'
        self.keywords_file_path = f'{self.imdb_directory}keywords.csv'

    def get_latest_imdb(self):
        if self.imdb_file_not_updated():
            soup = self.go_to_imdb_page()
            links = self.get_all_links(soup)
            self.download_all_links(links)
            self.extract_all_links()
            self.remove_archive_links()
        return self.set_imdb_data()

    def main_movie_file(self, limited_cols=True, movie_types='some'):
        if limited_cols:
            title_df = pd.read_csv(self.title_basics_path, sep="\t",
                                   usecols=['tconst', 'primaryTitle', 'startYear', 'runtimeMinutes', 'titleType'])
        else:
            title_df = pd.read_csv(self.title_basics_path, sep="\t")
        if movie_types == 'some':
            return reset_and_copy(title_df.loc[title_df["titleType"].isin(["movie", "tvMovie", "video"])])
        else:
            return title_df

    def make_movie_df(self):
        cols_to_remove = list()
        basic_file = self.main_movie_file(limited_cols=False, movie_types='all')
        for col in basic_file.columns:
            if camel_to_snake(col) != col:
                cols_to_remove.append(col)
                basic_file[camel_to_snake(col)] = basic_file[col]
        return basic_file.drop(cols_to_remove, axis=1)

    def eliminate_genres_from_movies(self, movie_df):
        return movie_df.drop(['genres'], axis=1)

    def remove_archive_links(self):
        for file in self.files_to_extract():
            os.remove(file)

    def imdb_merge_not_updated(self):
        return self.file_not_updated(file_path=self.merged_imdb_path)

    def file_not_updated(self, file_path):
        try:
            update_time = os.path.getmtime(file_path)
            days = ((time.time() - update_time) / 3600) / 24
            return days > 1
        except:
            return True

    def imdb_file_not_updated(self):
        return self.file_not_updated(self.title_basics_path) or self.file_not_updated(self.title_akas_path)

    def set_imdb_data(self):
        if self.imdb_merge_not_updated():
            df = CreateMovieData(movies_from_file_path=self.title_basics_path,
                                 alias_file_path=self.title_akas_path).create_imdb_df()
            df.to_csv(self.merged_imdb_path, index=False)
            return df
        else:
            return pd.read_csv(self.merged_imdb_path)

    def go_to_imdb_page(self):
        return get_soup_url(self.imdb_data_url)

    def get_uls(self, soup):
        return soup.find_all('a')

    def get_all_links(self, soup):
        return [ul.find('a').get('href') for ul in soup.find_all('ul')]

    def get_output_file(self, file_name):
        return "{}".format(file_name.split('.gz')[0])

    def extract_all_links(self):
        for file in self.files_to_extract():
            extract_file(file, self.get_output_file(file))

    def files_to_extract(self):
        return list(filter(lambda file_path: '.gz' in file_path, listdir_nohidden(self.imdb_directory)))

    def file_name_of_link(self, link):
        return "{}{}".format(self.imdb_directory, link.split(self.imdb_data_url)[-1])

    def download_all_links(self, links):
        for link in links:
            download_file(self.file_name_of_link(link), link)
