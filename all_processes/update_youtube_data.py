from common_methods import *
from syncing_movie_data.movie_data_row import *
from syncing_movie_data.merge_movie_data import *
from playlist_extractor.many_videos_extractor import *

class UpdateYoutubeData:
    
    def __init__(self, playlist_path='/Users/timdunn/Desktop/youtube_latest/playlists.rtf', youtube_directory='/Users/timdunn/Desktop/youtube_latest/', imdb_path='/Users/timdunn/Desktop/imdb_latest/merged_imdb.csv'):
        self.youtube_directory = youtube_directory
        self.youtube_movie_file = "{}youtube_movies.csv".format(self.youtube_directory)
        self.youtube_data = pd.read_csv(self.youtube_movie_file)
        self.merged_df = None
        self.playlist_path = playlist_path
        self.movie_row_object = MovieDataRow(self.youtube_data)
        self.imdb_df = pd.read_csv(imdb_path)
        
    def convert_youtube_df(self):
        self.add_url_column_to_df()
        self.add_year_column_to_df()
        self.add_title_column_to_df()
        self.add_searchable_column_to_df()
    
    def add_url_column_to_df(self):
        add_column_to_data(self.youtube_data, 'url', 'link url')
            
    def add_year_column_to_df(self):
        if column_not_available(self.youtube_data, 'year'):
            self.movie_row_object.add_year_to_df(input_year='title')
        
    def add_title_column_to_df(self):
        add_column_to_data(self.youtube_data, 'title', 'link title')
        
    def add_searchable_column_to_df(self):
        if column_not_available(self.youtube_data, 'searchable_movie_name'):
            movie_names = self.movie_row_object.apply_searchable_column('title',searchable_movie_name)
            self.youtube_data['searchable_movie_name'] = movie_names
      
    def merge_youtube_data(self):
        self.merged_df = MergeMovieData(imdb_df=self.imdb_df, links_df=self.youtube_data).set_merge_dfs()
    
    def run_all_processes(self):
        if self.not_latest_youtube_data():
            self.scrape_youtube_data()
        if self.youtube_not_merged():
            self.convert_youtube_df()
            self.merge_youtube_data()
        
    def not_latest_youtube_data(self):
        try:
            return (time.time() - os.path.getmtime(self.playlist_path))/3600 > 24
        except:
            return True   
        
    def scrape_youtube_data(self):
        self.youtube_data = pd.DataFrame(data=ManyVideosExtractor(playlist_file=self.playlist_path, playlists_path=self.youtube_movie_file).extract()).drop_duplicates()
        self.save_youtube_data()
        
    def save_youtube_data(self):
        self.youtube_data.to_csv(self.youtube_movie_file, index=False)
        
    def youtube_not_merged(self):
        try:
            return os.path.getmtime(self.youtube_movie_file) > os.path.getmtime(self.merged_df)
        except:
            return True