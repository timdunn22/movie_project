from common_methods import *
from all_processes.add_movie_info import AddMovieInfo
from all_processes.load_yaml_vars import LoadYamlVars


class AddAllImdbInfo(LoadYamlVars):
    
    def __init__(self, configuration_file, instances=5, only_tconsts=False):
        super().__init__(yaml_file_path=configuration_file)
        self.imdb_df = None
        self.movie_info_df = None
        self.only_tconsts = only_tconsts
        self.set_imdb_data()
        for instance_id in range(instances):
            setattr(self, f'{instance_id}', list())
        self.instances = instances
        
    def set_imdb_data(self):
        self.imdb_df = pd.read_csv(self.imdb_title_path, sep='\t', usecols=['tconst', 'titleType'])
        if not self.only_tconsts:
            self.imdb_df = reset_and_copy(self.imdb_df.loc[self.imdb_df['titleType'].isin(['video', 'movie', 'tvMovie'])])
        else:
            self.imdb_df = reset_and_copy(self.imdb_df.loc[self.imdb_df['tconst'].isin(self.only_tconsts)])

            
    def run_imdb_info_instance(self, df, instance_id):
        starting_time = time.time()
        for index, row in df.iterrows():
            try:
                self.append_row_instance_data(row, instance_id)
                if index % 100 == 0:
                    self.print_log_message(index, instance_id)
                if index % 1000 == 0:
                    self.save_temp_data(instance_id, starting_time)
            except:
                print("there was an exception")
                self.print_log_message(index, instance_id)
                self.save_temp_data(instance_id, starting_time)
        self.set_already_downloaded_data()
                
    def append_row_instance_data(self, row, instance_id):
        getattr(self, f'{instance_id}').append(AddMovieInfo(row).extra_info())

    def get_data_instance(self, instance_id):
        return getattr(self, f'{instance_id}')

    def get_movie_temp_path(self, instance_id, starting_time):
        return "{}{}_{}.csv".format(self.extra_data_directory, instance_id, starting_time)
    
    def print_log_message(self, index, instance_id):
        print("on index {} of instance {}".format(index, instance_id))
    
    def set_already_downloaded_data(self):
        dfs = [pd.read_csv(self.extra_data_path)]
        files = filtered_file_paths(self.extra_data_directory)
        dfs = flatten([dfs, [pd.read_csv(file) for file in files]])
        for file_path in files:
            os.remove(file_path)
        if len(files) == 0:
            self.movie_info_df = dfs[0]
        else:
            self.movie_info_df = pd.concat(dfs)
            self.movie_info_df.drop_duplicates(inplace=True)
            self.movie_info_df.to_csv(self.extra_data_path, index=False)
    
    def save_temp_data(self, instance_id, starting_time):
        return pd.DataFrame(data=self.get_data_instance(instance_id)).to_csv(
            self.get_movie_temp_path(instance_id, starting_time), index=False)
    
    def run_instances_imdb_scraper(self):
        imdb_dfs = self.get_imdb_dfs()
        thread_list = list()
        for instance_id in range(self.instances):
            thread = threading.Thread(name='ADD IMDB INFO INSTANCE # {}'.format(instance_id),
                                      target=self.run_imdb_info_instance,
                                      args=(imdb_dfs[instance_id], instance_id))
            thread_list.append(thread)
            thread.start()
            print(thread.name + ' started!')
        for thread in thread_list:
            thread.join()
            
    def run_with_exceptions(self, exceptions=10):
        for _ in range(exceptions):
            try:
                self.run_instances_imdb_scraper()
            except:
                print("there was a major exception")
            
    def get_imdb_dfs(self):
        self.set_already_downloaded_data()
        relevant_df = self.imdb_df.loc[~(self.imdb_df["tconst"].isin(self.movie_info_df["tconst"]))]
        return [relevant_df.loc[indexes] for indexes in divide_chunks(relevant_df.index, 
                                                                      round(relevant_df.index.size/self.instances))]    
        

def main():
    AddAllImdbInfo(os.environ.get('YAML_FILE', './movie_configuration.yaml'), 10).run_with_exceptions(30)

if __name__ == '__main__':
    main()