from syncing_movie_data.create_movie_data import *

class MergeGuessitData(CreateMovieData):
    
    def __init__(self, imdb_df=None, links_df=None):
        super().__init__(imdb_df, links_df)
        self.link_titles = self.links_df["link title"]
        self.years = self.links_df['year']
        self.guessit_dicts = []
        self.movie_names = []
            
    def get_guessit_df(self):
        self.get_guessit_dicts()
        self.get_guessit_names()
        self.add_searchable_guessit_names()
        self.add_guessit_years()
        return self.links_df.loc[~(self.years == '') & ~(self.links_df['searchable_movie_name'] == '')]

    def get_guessit_dicts(self):
        self.guessit_dicts = self.link_titles.apply(self.get_guessit_dict)
    
    def get_guessit_dict(self, file_name):
        try:
            return api.guessit(file_name)
        except:
            return ''
    
    def get_guessit_names(self):
        self.movie_names = self.guessit_dicts.apply(self.guessit_movie_name)
        
    def add_searchable_guessit_names(self):
        self.links_df['searchable_movie_name'] = self.movie_names.apply(self.searchable_movie_name)
    
    def add_guessit_years(self):
        self.links_df['year'] = self.guessit_dicts.apply(self.guessit_year).astype(str)
    
    def guessit_year(self, guessit_dict):
        return self.guessit_attribute(guessit_dict, 'year')
    
    def guessit_movie_name(self, guessit_dict):
        return self.guessit_attribute(guessit_dict, 'title')
    
    def guessit_attribute(self, guessit_dict, attribute):
        try:
            if guessit_dict['type'] == 'movie':
                guessit_value = guessit_dict[attribute]
                if type(guessit_value) == list:
                    return guessit_value[0]
                return guessit_value
            return ''
        except:
            return ''
    
    def set_guess_it_df(self):
        guessit_df = self.get_guessit_df()
        print(guessit_df)
        self.years = self.links_df['year']
        guessit_df = self.links_df.loc[~(self.years == '')]
        guessit_df["year 1"] = self.set_year_vals(guessit_df, 1)
        guessit_df["year 2"] = self.set_year_vals(guessit_df, -1)
        merged_dfs = list()
        for year in ["year", "year 1", "year 2", None]:
            print(year)
            df_dict = self.merged_guessit_imdb(guessit_df, year_column=year)
            print("past merge part")
            guessit_df = df_dict["guessit_df"]
            merged_dfs.append(df_dict["merged_df"])
        return pd.concat(merged_dfs).drop_duplicates(subset=["link url"]).reset_index(drop=True)

    def start_guessit_imdb_merge(self, guessit_df, year_column="year"):
        left_columns = ["searchable_movie_name"]
        right_columns = ["searchable_alias_title"]
        if year_column is not None:
            left_columns.append(year_column)
            right_columns.append("startYear")
        return pd.merge(guessit_df, self.imdb_df, left_on=left_columns, right_on=right_columns)

    def merged_guessit_imdb(self, guessit_df, year_column="year"):
        merged_df = self.start_guessit_imdb_merge(guessit_df, year_column=year_column)
        merged_df = merged_df.loc[self.imdb_duration_nonnull(merged_df) & self.links_duration_nonnull(merged_df)]
        merged_df = reset_and_copy(merged_df.loc[self.duration_within_range(merged_df)])
        guessit_df = reset_and_copy(guessit_df.loc[~guessit_df["link url"].isin(merged_df["link url"])])
        return {"merged_df": merged_df, "guessit_df": guessit_df}

    def set_year_vals(self, guessit_df, num):
        return (guessit_df['year'].astype(int) + num).astype(str)

    def imdb_duration_nonnull(self, df):
        return nonnull_columns(df, self.imdb_duration_column)

    def links_duration_nonnull(self, df):
        return nonnull_columns(df, self.link_duration_column)

    def duration_within_range(self, df):
        return abs(df["runtimeMinutes"].astype(int) - df["duration"].astype(int)) < 10