from playlist_extractor.youtube_extractor import *
from common_methods import *

class ManyVideosExtractor:

    def __init__(self, playlists=None, playlist_file='/Users/timdunn/Desktop/playlists.txt.rtf', use_files=True, playlists_path='/Users/timdunn/Desktop/youtube_movies.csv'):
        self.playlists = playlists
        self.playlist_file = playlist_file
        self.playlists_path = playlists_path
        if use_files:
            self.get_playlists_file()
            self.already_downloaded_playlists()
        self.completed_playlists = []
        self.data = []

    def extract(self):
        self.already_downloaded_playlists()
        for playlist in self.playlists:
            if playlist not in self.completed_playlists:
                self.extract_playlist(playlist)
                self.update_playlists_file()
                self.completed_playlists.append(playlist)
        return flatten(self.data)
        
    def extract_playlist(self, playlist):
        self.data.append(YoutubeExtractor(playlist).download_get_dicts())
    
    def playlists_data(self):
        return pd.read_csv(self.playlists_path)
    
    def update_playlists_file(self):
        current_data = list(filter(lambda x: 'duration' in x,flatten(self.data)))
        playlists_df = self.playlists_data()
        current_df = pd.DataFrame(data=current_data)
        pd.concat([playlists_df,current_df]).drop_duplicates(subset=["url"]).to_csv(self.playlists_path, index=False)
    
    def playlists_from_file(self):
        lines = list(filter(lambda line: "list" in line, open(self.playlist_file, 'r').readlines()))
        playlists = [self.convert_line(line) for line in lines]
        playlists = list(filter(lambda x: type(x) == str, playlists))
        return np.unique(list(filter(lambda x: 'list=' in x, playlists)))
                      
    def get_playlists_file(self):
        self.playlists = self.playlists_from_file()
    
    def playlists_downloaded(self):
        playlist_urls = list(self.playlists_data()['playlist_url'])
        return list(np.unique(np.array(playlist_urls, dtype=str)))
        
    def already_downloaded_playlists(self):
        downloaded = self.playlists_downloaded()
        self.playlists = list(filter(lambda playlist: not self.playlist_in_list(playlist, downloaded), self.playlists))
        
    def playlist_in_list(self, itema, listb):
        return self.get_playlist_id(itema) in [self.get_playlist_id(item) for item in listb]
        
    def get_playlist_id(self, item):
        return item.split('list=')[-1].split('&')[0]
    
    def convert_line(self, line):
        if "HYPERLINK" in line.strip() and 'list' in line.strip():
            line_splits = line.strip().split("HYPERLINK")
            return list(filter(lambda x: 'list' in x, line_splits))[0].split('"')[1]
        else:
            return None