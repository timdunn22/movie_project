import pandas as pd
from common_methods import column_within_range

class MergeAbstract:

    def __init__(self, first_df, second_df, right_columns, left_columns, duplicate_key, 
                 excluded_links, merged_df=None):
        self.first_df = first_df
        self.second_df = second_df
        self.right_columns = right_columns
        self.left_columns = left_columns
        self.duplicate_key = duplicate_key
        self.merged_df = merged_df
        self.excluded_links = excluded_links
    
    def apply_strategy(self):
        self.filter_excluded()
        self.set_merge_df()
        self.apply_intermediate_filters()
        self.filter_out_duplicates()
        return self.merged_df

    def apply_intermediate_filters(self):
        pass

    def filter_excluded(self):
        if self.excluded_links is not None:
            for link in self.excluded_links:
                self.merged_df = self.merged_df.loc[~self.merged_df[self.duplicate_key].isin(
                    link[self.duplicate_key])]

    def set_merge_df(self):
        self.merged_df = pd.merge(self.first_df, self.second_df, left_on=self.left_columns, 
                                  right_on=self.right_columns)

    def filter_out_duplicates(self):
        return self.merged_df.loc[self.merged_df.duplicated(subset=[self.duplicate_key])]
    
    def duration_within_range(self, df):
        return column_within_range(df, self.duration_range, self.movie_duration_column, 
                                        self.link_duration_column)

    def year_within_range(self, df):
        return column_within_range(df, self.year_range, self.movie_year_column, self.link_year_column)

class SimpleMerge(MergeAbstract):

    def __init__(self, movie_df, link_df, link_url_column, link_title_column, 
                 link_year_column, link_duration_column, movie_title_column, 
                 movie_year_column, movie_duration_column):
        right_columns = [link_title_column, link_year_column, link_duration_column]
        left_columns = [movie_title_column, movie_year_column, movie_duration_column]
        super().__init__(movie_df, link_df, right_columns, left_columns, link_url_column)

    def apply_intermediate_filters(self):
        self.merged_df = self.duration_within_range(self.merged_df)

class CloseYearMerge(MergeAbstract):

    def __init__(self, first_df, second_df, right_columns, left_columns, duplicate_key, 
                 excluded_links, merged_df=None, movie_title_column='title', 
                 movie_year_column='start_year', link_title_column='guessit_title', 
                 link_year_column='guessit_year'):
        left_columns = [movie_title_column, movie_year_column]
        right_columns = [link_title_column, link_year_column]
        super().__init__(first_df, second_df, right_columns, left_columns, 
                         duplicate_key, excluded_links, merged_df)

    def apply_intermediate_filters(self):
        self.merged_df = self.duration_within_range(self.merged_df)
        self.merged_df = self.year_within_range(self.merged_df)