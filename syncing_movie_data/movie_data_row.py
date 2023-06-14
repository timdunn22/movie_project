class MovieDataRow:
    
    def __init__(self, df, duration_column="duration", year_column="year"):
        self.df = df
        
    def duration_within_range(self):
        return (self.duration_greater_than() & self.duration_less_than())
    
    def duration_greater_than(self):
        return self.df['duration'].astype(int) > (self.df['runtimeMinutes'].astype(int) - 10)
    
    def duration_less_than(self):
        return self.df['duration'].astype(int) < (self.df['runtimeMinutes'].astype(int) + 10)
    
    def add_searchable_name_to_df(self, input_movie_name="link title", output_movie_name="searchable_movie_name"):
        self.df[output_movie_name] = self.df[input_movie_name].astype(str).apply(self.searchable_movie_name)

    def add_year_to_df(self, input_year="link title", output_year="year"):
        self.df[output_year] = self.get_year(input_year).values
        
    def get_year(self, column):
        return self.df[column].str.extract(r'(18[8-9][0-9]|19[0-9]{2}|20[0-1][0-9]|202[0-3])')
    
    def apply_searchable_column(self, input_column, searchable_function):
        return self.df[input_column].astype(str).apply(searchable_function)
    
    