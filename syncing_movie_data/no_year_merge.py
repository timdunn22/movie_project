from syncing_movie_data.movie_merge_instance import *

class NoYearMerge(MovieMergeInstance):
    
    def __init__(self, imdb_df, links_df, main_param, merged_links_urls=[]):
        super().__init__(imdb_df, links_df, main_param, merged_links_urls)
        
    def main_imdb_logic(self):
        return self.imdb_df["runtimeMinutes"].astype(float).isin(self.get_runtime_minutes())
    
    def main_link_logic(self):
        return self.links_df["duration"].astype(float) == float(self.main_param)
        
    def get_runtime_minutes(self):
        return [float(num + self.main_param) for num in range(-5,5)]
    
    def merged_logic(self):
        pass