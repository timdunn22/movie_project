from syncing_movie_data.movie_merge_instance import *

class YearExactMerge(MovieMergeInstance):
    
    def __init__(self, imdb_df, links_df, main_param, merged_links_urls=[]):
        super().__init__(imdb_df, links_df, main_param, merged_links_urls)
        
    def main_imdb_logic(self):
        return self.imdb_years() == str(self.main_param)
    
    def main_link_logic(self):
        return (self.links_years() == float(self.main_param))   