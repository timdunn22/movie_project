from common_methods import *
from syncing_movie_data.movie_data_row import *

class MovieMergeInstance:
    
    def __init__(self, imdb_df, links_df, main_param, merged_link_urls):
        self.merged_link_urls = merged_link_urls
        self.imdb_df = imdb_df
        self.imdb_df = reset_and_copy(self.imdb_df.loc[self.present_runtimes()])
        self.links_df = links_df
        self.links_df = reset_and_copy(self.links_df.loc[self.present_durations() & self.not_merged()])
        self.main_param = main_param
               
    def not_merged(self):
        return ~self.links_df['link url'].isin(self.merged_link_urls)
    
    def concat_merge(self):
        return pd.concat([self.get_final_chunk(df) for df in self.get_imdb_chunks()])
    
    def get_imdb_chunks(self):
        current_imdb_df = self.current_imdb()
        if current_imdb_df.shape[0] == 0:
            return [current_imdb_df]
        return get_dfs_divided(current_imdb_df, chunks=get_chunks(current_imdb_df))
    
    def current_imdb(self):
        return reset_and_copy(self.imdb_logic())
    
    def imdb_logic(self):
        return self.imdb_df.loc[self.main_imdb_logic()]
    
    def main_imdb_logic(self):
        return True
    
    def imdb_years(self):
        return self.imdb_df["startYear"]
    
    def link_logic(self):
        return self.links_df.loc[self.main_link_logic()]
    
    def main_link_logic(self):
        return True
    
    def links_years(self):
        return self.links_df["year"].astype(float)
    
    def current_link(self):
        return reset_and_copy(self.link_logic())
    
    def merge_chunk(self, df):
        self.chunk = pd.merge(self.current_link(), df, how="cross")
    
    def present_durations(self):
        return nonnull_columns(self.links_df, 'duration')
    
    def present_runtimes(self):
        return nonnull_columns(self.imdb_df, 'runtimeMinutes')
    
    def merged_logic(self):
        self.chunk = self.chunk[self.duration_within_range()]
    
    def chunk_logic(self):
        self.chunk = self.chunk[self.movie_name_correct(self.chunk)]
    
    def movie_name_correct(self, df):
        return df.apply(lambda row: str(row['searchable_alias_title']) in str(row['searchable_movie_name']), axis=1)
    
    def get_final_chunk(self, df):
        self.merge_chunk(df)
        self.merged_logic()
        self.chunk_logic()
        return self.chunk
        
    def duration_within_range(self):
        return MovieDataRow(self.chunk).duration_within_range()