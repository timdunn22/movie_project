from imdb import Cinemagoer
import json
import pandas as pd

class UpdatePopularImdb:

    def __init__(self, genres_list_file="/Users/timdunn/movie_watch/src/genres.json", 
                 destination_directory="./"):
        self.movie_object = Cinemagoer()
        self.genres = [genre for genre in json.load(open(genres_list_file)) 
                       if genre not in ['Short', 'Documentary']]
        self.movie_dict = dict()
        self.genres_destination = f'{destination_directory}top_genres.csv'
        # self.top_indian_destination = f'{destination_directory}top_indian.csv'
        self.top_imdb_destination = f'{destination_directory}top_imdb.csv'
        self.top_popular_destination = f'{destination_directory}popular.csv'

    def get_top_genre(self, genre):
        return self.movie_object.get_top50_movies_by_genres(genre)

    def get_top_imdb(self):
        self.movie_dict['top_imdb'] = self.movie_object.get_top250_movies()

    # def get_top_indian(self):
    #     self.movie_dict['top_indian_movies'] = self.movie_object.get_top250_indian_movies()

    def get_all_genres(self):
        for genre in self.genres:
            try:
                self.movie_dict[genre] = self.get_top_genre(genre)
            except:
                pass

    def get_popular_movies(self):
        self.movie_dict['popular_movies'] = self.movie_object.get_popular100_movies()

    def run_all(self):
        retrieve_data_functions = [self.get_all_genres, self.get_top_imdb, 
                                #    self.get_top_indian, 
                                   self.get_popular_movies]
        for retrieve_data_function in retrieve_data_functions:
            try:
                retrieve_data_function()
            except:
                print('there was an error processing', retrieve_data_function)
        return self.movie_dict

    def run_and_generate_all(self):
        self.run_all()
        self.generate_top_data()

    def generate_genre_csv(self):
        dfs = list()
        for genre in self.genres:
            data = [self.get_genre_data(movie, genre, index) for index, movie in 
                    enumerate(self.movie_dict.get(genre))]
            dfs.append(pd.DataFrame(data=data))
        df = pd.concat(dfs)
        df.to_csv(self.genres_destination, index=False)
        return df

    def generate_top_data(self):
        self.generate_genre_csv()
        self.generate_popular_movies()
        self.generate_top_imdb()
        # self.generate_top_indian()

    def generate_top_movies(self, key, destination, field):
        movie_data = [self.movie_data(movie, index, field) for index, movie in 
                               enumerate(self.movie_dict.get(key))]
        df = pd.DataFrame(data=movie_data)
        df.to_csv(destination, index=False)
        return df
    
    def movie_data(self, movie, index, field):
        movie_data = dict()
        movie_data['tconst'] = self.get_tconst(movie)
        movie_data[field] = index + 1
        return movie_data

    def get_tconst(self, movie):
        return f'tt{movie.getID()}'

    def generate_popular_movies(self):
        return self.generate_top_movies('popular_movies', self.top_popular_destination, 'top_popular_rank')

    def generate_top_imdb(self):
        return self.generate_top_movies('top_imdb', self.top_imdb_destination, 'top_250_rank')

    # def generate_top_indian(self):
    #     return self.generate_top_movies('top_indian_movies', self.top_indian_destination, 'top_indian_rank')

    def get_genre_data(self, movie, key, index):
        movie_data = dict()
        movie_data['genre'] = key
        movie_data['movie'] = self.get_tconst(movie)
        movie_data['top_50_rank'] = index + 1
        return movie_data