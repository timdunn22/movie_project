from syncing_movie_data.year_exact_merge import *
from syncing_movie_data.no_year_merge import *
from syncing_movie_data.almost_year_merge import *
from syncing_movie_data.merge_guessit_data import *
from syncing_movie_data.create_movie_data import *

class MergeMovieData(CreateMovieData):

    def __init__(self, imdb_df=None, links_df=None, groups_df=None):
        super().__init__(imdb_df, links_df, groups_df)
        self.merged_link_urls = []
    
    def year_exact_merge(self, year):
        return YearExactMerge(self.imdb_df, self.links_df, year, self.merged_link_urls).concat_merge()
        
    def year_almost_merge(self, year):
        return AlmostYearMerge(self.imdb_df, self.links_df, year, self.merged_link_urls).concat_merge()
    
    def no_year_merge(self, duration):
        return NoYearMerge(self.imdb_df, self.links_df, duration, self.merged_link_urls).concat_merge()
    
    def add_to_not_merged(self, df):
        df_links = list(df['link url'])
        if len(self.merged_link_urls) == 0:
            self.merged_link_urls.append(df_links)
            self.merged_link_urls = flatten(self.merged_link_urls)
        else:
            self.merged_link_urls = flatten([self.merged_link_urls, df_links])

    def year_almost_dfs(self):
        return [self.year_almost_merge(year) for year in range(1894,2024)]
    
    def imdb_durations(self):
        return list(self.imdb_df.loc[nonnull_columns(self.imdb_df, 'runtimeMinutes'),'runtimeMinutes'].astype(int))
    
    def link_durations(self):
        return list(self.links_df.loc[nonnull_columns(self.links_df, 'duration') & self.not_merged(),'duration'].astype(int))
    
    def similar_durations(self):
        f_list = flatten([list(np.unique(self.link_durations())), list(np.unique(self.imdb_durations()))])
        array, counts = np.unique(np.array(f_list), return_counts=True)
        return array[counts > 1]
    
    def no_year_dfs(self):
        return [self.no_year_merge(duration) for duration in self.similar_durations()]
        
    def no_year_df(self):
        df = pd.concat(self.no_year_dfs()).drop_duplicates(subset=['link url', 'tconst']).reset_index(drop=True)
        df.to_csv('/Users/timdunn/Desktop/no_year.csv', index=False)
        return df
    
    def one_year_dfs(self):
        self.one_years = []
        for year in range(1894,2024):
            print(year)
            df = self.year_exact_merge(year)
            self.one_years.append(df)
            df.to_csv("/Users/timdunn/Desktop/{}_okru_merged.csv".format(year), index=False)
        return self.one_years
    
    def one_year_df(self):
        df = pd.concat(self.one_year_dfs()).drop_duplicates(subset=['link url', 'tconst']).reset_index(drop=True)
        self.final_one_year = df
        df.to_csv('/Users/timdunn/Desktop/one_year.csv', index=False)
        return df
        
    def almost_year_df(self):
        df = pd.concat(self.year_almost_dfs()).drop_duplicates(subset=['link url', 'tconst']).reset_index(drop=True)
        self.almost_year = df
        df.to_csv('/Users/timdunn/Desktop/almost_year.csv', index=False)
        return df
    
    def set_merge_dfs(self):
        basic_links_merge = self.basic_merge()
        links_df = self.links_df[self.not_merged()]
        guess_it_df = self.set_guess_it_df(links_df)
        guess_it_df.to_csv('/Users/timdunn/Desktop/guessit_okru.csv', index=False)
        self.links_df = pd.merge(self.links_df, basic_links_merge, on='link url', how='left').drop_duplicates(subset=["tconst", "link url"])
        self.links_df = pd.merge(self.links_df, guess_it_df, on='link url', how='left')
        self.links_df.to_csv("/Users/timdunn/Desktop/merged_okru_imdb_guessit_others.csv", index=False)
        return concated_df
    
    def needed_columns(self):
        return ["link title","link url","duration","year","searchable_movie_name"]
    
    def not_merged(self):
        return ~self.links_df['link url'].isin(self.merged_link_urls)
    
    def merge_with_links(self, df, source_merge):
        merged_df = pd.merge(self.links_df[self.needed_columns()], df[['link url', 'tconst']], on='link url')
        merged_df['source_merge'] = source_merge
        merged_df = reset_and_copy(merged_df.loc[nonnull_columns(merged_df, 'tconst')])
        self.add_to_not_merged(year_one_df)
        return merged_df
        
    def basic_merge(self):
        year_one_df = self.merge_with_links(self.one_year_df(), "exact_year")
        almost_year_df = self.merge_with_links(self.almost_year_df(), "almost_year")
        no_year_df = self.merge_with_links(self.no_year_df(), "no_year")
        return pd.concat([year_one_df, almost_year_df, no_year_df]).drop_duplicates(subset=['tconst', 'link url'])

    def merge_imdb_links(self):
        return pd.merge(self.links_df, self.imdb_df, on=("tconst")
                                    ).drop_duplicates(subset=["tconst", "link url"]).reset_index(drop=True)
    
    def set_guess_it_df(self, links_df):
        return MergeGuessitData(links_df=self.links_df, imdb_df=self.imdb_df).set_guess_it_df(links_df)