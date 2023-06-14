import pandas as pd
from common_methods import get_soup_url
class ExtractPosters:

    def __init__(self, imdb_path) -> None:
        self.tconsts = pd.read_csv(imdb_path, usecols='tconst')['tconst']

    def extract_poster(self, tconst):
        url = f'https://www.imdb.com/title/{tconst}/mediaviewer'
        soup = get_soup_url(url)
