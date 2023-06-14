from requirements import *
from itertools import zip_longest
from common_methods import *

class CreateMovieData:
    
    def __init__(self, imdb_df=None, links_df=None, movies_from_file_path="/Users/timdunn/Downloads/title.basics (1).tsv", alias_file_path="/Users/timdunn/Downloads/title.akas (1).tsv"):
        self.imdb_df = imdb_df
        self.links_df = links_df
        self.okru_links_path = "/Users/timdunn/Desktop/okru_progress.csv"
        self.merged_df = pd.read_csv('/Users/timdunn/Desktop/imdb_latest/merged_imdb.csv')
        self.imdb_duration_column = 'runtimeMinutes'
        self.link_duration_column = 'duration'
        self.link_year_column = 'year'
        self.imdb_year_column = 'startYear'
        self.imdb_data_path = "/Users/timdunn/Desktop/updated_imdb_alias_merge.csv"
        self.movies_from_file_path = movies_from_file_path
        self.alias_file_path = alias_file_path
        
    def create_links_df(self):
        okru_dfs = self.create_okru_dfs()
        final_df = self.concat_okru_dfs(okru_dfs)
        self.add_columns_okru_df(final_df)
        final_df = self.remove_merged_links(final_df)
        self.set_default_links_duration(final_df)
        return final_df
    
    def set_default_links_duration(self, df, duration_column="duration"):
        df.loc[df[duration_column].isnull() | df[duration_column] == "\\N"] = 0
    
    def update_okru_links_file(self):
        self.links_df.to_csv(self.okru_links_path, index=False)
    
    def add_columns_okru_df(self, okru_df):
        self.add_duration_to_df(okru_df)
        self.add_searchable_name_to_df(okru_df)
        self.add_year_to_df(okru_df)
        
    def remove_merged_links(self, df, link_input="link url"):
        df = df.loc[~df[link_input].isin(self.merged_df[link_input]), 
                                ['link title', 'link url','duration', 'year', 'searchable_movie_name']]
        return reset_and_copy(df)
        
    def add_duration_to_df(self, df, input_duration="movie duration", output_duration="duration"):
        df[output_duration] = df[input_duration].astype(str).apply(self.minutes_from_string).astype(int)
    
    def add_searchable_name_to_df(self, df, input_movie_name="link title", output_movie_name="searchable_movie_name"):
        df[output_movie_name] = df[input_movie_name].astype(str).apply(self.searchable_movie_name)
        
    def add_year_to_df(self, df, input_year="link title", output_year="year"):
        df[output_year] = self.get_year(df, input_year).values
    
    def create_imdb_df(self):
        title_df = self.movies_from_file()
        alias_df = self.alias_df_from_file()
        self.add_alias_searchable_title(alias_df)
        imdb_df = self.merge_alias_w_imdb_df(alias_df, title_df)
        self.add_searchable_columns_to_imdb_df(imdb_df)
        imdb_df = self.drop_imdb_duplicates(imdb_df)
        return imdb_df
    
    def drop_imdb_duplicates(self, imdb_df):
        return imdb_df.drop_duplicates(subset=['tconst', 'searchable_alias_title']).copy().reset_index(drop=True)
    
    def create_groups_df(self):
        return self.imdb_df.groupby(['startYear', 'runtimeMinutes'])
    
    def movies_from_file(self):
        file_data_frame = pd.read_csv(self.movies_from_file_path, sep="\t")
        return file_data_frame.loc[file_data_frame['titleType'].isin(["movie", "tvMovie", "video"])]
    
    def alias_df_from_file(self):
        return pd.read_csv(self.alias_file_path, sep="\t")
    
    def add_alias_searchable_title(self, df):
        df["searchable_alias_title"] = df["title"].astype(str).apply(self.searchable_movie_name)
        
    def merge_alias_w_imdb_df(self, alias_df, imdb_df):
        merged_df = pd.merge(imdb_df, alias_df, how="inner", validate ="one_to_many", left_on=("tconst"), 
                             right_on=("titleId"))
        return merged_df
    
    def add_searchable_columns_to_imdb_df(self, imdb_df, searchable_method="normal"):
        if searchable_method == "normal":
            imdb_df["searchable_primary_title"] = self.apply_searchable_column(imdb_df, "primaryTitle", self.searchable_movie_name)
            imdb_df["searchable_original_title"] = self.apply_searchable_column(imdb_df, "originalTitle", self.searchable_movie_name)
        else:
            imdb_df["searchable_primary_title"] = self.apply_searchable_column(imdb_df, "primaryTitle", self.simple_searchable_movie_name)
            imdb_df["searchable_original_title"] = self.apply_searchable_column(imdb_df, "originalTitle", self.simple_searchable_movie_name)
    
    def apply_searchable_column(self, df, input_column, searchable_function):
        return df[input_column].astype(str).apply(searchable_function)
    
    def create_okru_dfs(self):
        dfs = [pd.read_csv(file_path) for file_path in listdir_nohidden("/Users/timdunn/Desktop/okru_checkpoints/")]
        dfs.append(pd.read_csv(self.okru_links_path))
        return dfs
    
    def concat_okru_dfs(self, dfs):
        return pd.concat(dfs).drop_duplicates(subset=["link url"]).drop(["tconst"], axis=1).reset_index(drop=True)
    
    
    
    def searchable_movie_name(self,movie_name):
        movie_name = re.sub(":", "", movie_name)
        movie_name = re.sub("'", "", movie_name)
        movie_name = re.sub("\.\.\.", "", movie_name)
        movie_name = re.sub("\.\.", "", movie_name)
        movie_name = re.sub("_", " ", movie_name)
        movie_name = re.sub("-", " ", movie_name)
        movie_name = re.sub("  ", " ", movie_name)
        movie_name = re.sub("\.", " ", movie_name)
        movie_name = re.sub('"', "", movie_name)
        movie_name = re.sub("\*", "", movie_name)
        return movie_name.lower().strip()
    
    def simple_searchable_movie_name(self,movie_name):
        return re.sub(":", "", movie_name).lower()

    def get_year(self,df, column):
        return df[column].str.extract(r'(18[8-9][0-9]|19[0-9]{2}|20[0-1][0-9]|202[0-3])')

    def minutes_from_string(self, date_string):
        try:
            split_string = [int(split_str) for split_str in date_string.split(":")]
            if len(split_string) == 2:
                return round(split_string[0] + (split_string[1]/60))
            elif len(split_string) == 3:
                return round((split_string[0] * 60) + split_string[1] + (split_string[2]/60))
            else:
                return 0
        except:
            return 0
    
    def update_imdb_file(self):
        self.imdb_df.to_csv(self.imdb_data_path, index=False)
        
    def create_and_update_data(self):
        self.imdb_df = self.create_imdb_df()
        self.update_imdb_file()
        self.links_df = self.create_links_df()
        self.update_okru_links_file()
        self.groups_df = self.create_groups_df()
        
    def read_data_from_files(self):
        self.imdb_df = pd.read_csv(self.imdb_data_path)
        self.links_df = pd.read_csv(self.okru_links_path)
        self.groups_df = self.create_groups_df()