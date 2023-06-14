from syncing_movie_data.movie_merge_instance import *

class AlmostYearMerge(MovieMergeInstance):
    
    def __init__(self, imdb_df, links_df, main_param, merged_links_urls=[]):
        super().__init__(imdb_df, links_df, main_param, merged_links_urls)
        
    def main_imdb_logic(self):
        return self.imdb_df["startYear"].isin(self.years())
    
    def main_link_logic(self):
        return (self.links_years() == float(self.main_param))
    
    def years(self):
        return [str(num + self.main_param) for num in [-1,1]]    