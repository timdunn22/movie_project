from common_methods import *

class AddMovieInfo:
    
    def __init__(self, row, average=6.9):
        self.row = row
        self.average = average
        self.tconst = self.row['tconst']
        self.movie_object = Cinemagoer().get_movie(self.get_movie_id(), info=('keywords', 'plot', 'main'))
        self.movie_data = self.movie_object.data
        
    def get_movie_id(self):
        return self.tconst.split('tt')[-1]
    
    def get_video_poster(self):
        try:
            return self.movie_data.get('cover url')
        except:
            return 'No cover'
    
    def get_trailer(self):
        try:
            return ','.join(self.movie_data.get('videos'))
        except:
            return None
    
    def language(self):
        try:
            return self.movie_object.guessLanguage()
        except:
            return None
    
    def color(self):
        try:
            return ','.join(self.movie_data.get('color info'))
        except:
            return None
        
    def score(self):
        try:
            v = self.movie_data.get('votes')
            R = self.movie_data.get('rating')
            m = 100000
            return (v/(v+m) * R) + (m/(m+v) * self.average)
        except:
            return 0

    def get_keywords(self):
        try:
            return ','.join(self.movie_object.get('keywords')) 
        except:
            return "no keywords"
    
    def extra_info(self):
        movie_info = dict()
        excluded_keys = ['keyword', 'genre', 'cover url', 'language', 'color', 'video', 'runtime', 'title', 'year',
                 'aka', 'localized title', 'rating', 'votes', 'original title', 'imdbID', 'kind']
        for key in self.movie_data.keys():
            key_val = self.movie_data.get(key)
            set_key(movie_info, key, key_val, excluded_keys)
        movie_info.update(self.get_movie_info())
        return movie_info
    
    def get_movie_info(self):
        return {'tconst': self.tconst, 'language': self.language(), 'color': self.color(), 'score': self.score(), 'keywords': self.get_keywords(), 'videos': self.get_trailer(), 'poster': self.get_video_poster()}