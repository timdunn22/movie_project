import pandas as pd
from all_processes.merge_abstract import MergeAbstract
from guessit import api

class MergeMainData:

    def __init__(self, link_path, movie_path):
        self.link_df = pd.read_csv(link_path)
        self.movie_df = pd.read_csv(movie_path)
        self.link_url = 'link_url'
        self.movie_title_column = 'title'
        self.movie_year_column = 'startYear'
        self.movie_duration_column = 'runtimeMinutes'
        self.link_title_column = 'guessit_title'
        self.link_year_column = 'guessit_year'
        self.link_duration_column = 'duration'
        self.duration_range = 10
        self.year_range = 1

    def merge_top_movies(self):
        self.apply_guessit()
        self.set_merged_df()
    
    def set_merged_df(self):
        self.apply_strategy(right_columns=[self.link_title_column, self.link_year_column, self.link_duration_column], 
                            left_columns=[self.movie_title_column, self.movie_year_column, self.movie_duration_column])
        more_advanced_merge = pd.merge(self.movie_df, links_filtered,
                                       left_on=[self.movie_title_column, self.movie_year_column], 
                                       right_on=[self.link_title_column, self.link_year_column])
        more_advanced_merge = more_advanced_merge.loc[self.duration_within_range(more_advanced_merge)]
        more_advanced_merge = self.filter_out_link_duplicates(more_advanced_merge)
        links_filtered = self.exclude_links(self.link_df, [self.merged_df, more_advanced_merge])
        very_advanced_merge = pd.merge(self.movie_df, links_filtered, left_on=[self.movie_title_column], 
                                       right_on=[self.link_title_column])
        very_advanced_merge = very_advanced_merge.loc[self.year_within_range(very_advanced_merge) & 
                                                      self.duration_within_range(very_advanced_merge)]
        very_advanced_merge = self.filter_out_link_duplicates(very_advanced_merge)
        self.merged_df = pd.concat(self.merged_df, more_advanced_merge, very_advanced_merge)
        self.merged_df = self.filter_out_link_duplicates(self.merged_df)

    def apply_strategy(self, right_columns, left_columns):
        self.merged_df = MergeAbstract(self.movie_df, self.link_df, right_columns=right_columns, 
                                       excluded_links=[self.merged_df], merged_df=self.merged_df,
                                       left_columns=left_columns).apply_strategy()

    def filter_out_link_duplicates(self, df):
        return df.loc[df.duplicated(subset=[self.link_url])]

    def duration_within_range(self, df):
        return self.column_within_range(df, self.duration_range, self.movie_duration_column, 
                                        self.link_duration_column)

    def year_within_range(self, df):
        return self.column_within_range(df, self.year_range, self.movie_year_column, self.link_year_column)

    def column_within_range(self, df, amount, column_a, column_b):
        column_a_value = df[column_a]
        column_b_value = df[column_b]
        within_greater_amount = (  column_a_value >=  column_b_value - amount)
        within_lesser_amount = ( column_a_value <= column_b_value + amount)
        return within_greater_amount & within_lesser_amount


    def exclude_links(self, df, link_dfs):
        for link in link_dfs:
            df = df.loc[~df[self.link_url].isin(link[self.link_url])]
        return df

    def apply_guessit(self):
        self.guessit_values = self.link_df.apply(self.guessit_data)
        guessit_df = pd.DataFrame(data=self.guessit_values)
        self.link_df = pd.merge(self.link_df, guessit_df, on=self.link_url)
    
    def guessit_data(self, row):
        guessit_object = self.get_guessit_object(row['fulltitle'])
        return {'link_url': row['link_url'], 'year': self.guessit_year(guessit_object), 
                'guessit_title':self.guessit_year(guessit_object) }

    def guessit_title(self, guessit_object):
        return guessit_object['title']
    
    def guessit_year(self, guessit_object):
        return guessit_object['year']

    def get_guessit_object(self, title):
        return api.guessit(title)