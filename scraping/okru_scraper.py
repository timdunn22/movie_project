from common_methods import *
from all_processes.update_movie_data import UpdateMovieData


class ScrapeAllImdb:

    def __init__(self, instances, imdb_directory, chromedriver_path, scraper_progress_path, scraper_class,
                 scraping_directory, tconst_path=None, use_proxy=False, only_tconsts=False):
        imdb_object = UpdateMovieData(imdb_directory=imdb_directory)
        imdb_object.get_latest_imdb()
        self.imdb_df = imdb_object.main_movie_file()
        self.chromedriver_path = chromedriver_path
        self.window_size = "1920,1080"
        self.scraper_progress_path = scraper_progress_path
        self.scraping_directory = scraping_directory
        self.instances = instances
        self.links_df = None
        self.use_proxy = use_proxy
        self.scraper_class = scraper_class
        self.only_tconsts = only_tconsts
        if not only_tconsts:
            self.tconst_path = tconst_path

    def run_instances_scraper(self):
        imdb_dfs = self.get_imdb_dfs()
        thread_list = list()
        for instance_id in range(self.instances):
            thread = threading.Thread(name='Test {}'.format(instance_id), target=self.scraper_function,
                                      args=(instance_id, imdb_dfs[instance_id]))
            thread_list.append(thread)
            thread.start()
            print(thread.name + ' started!')
        for thread in thread_list:
            thread.join()

    def scraper_function(self, instance_id, relevant_df):
        return self.scraper_class(instance_id, self.chromedriver_path, relevant_df, self.scraping_directory,
                                  use_proxy=self.use_proxy, window_size=self.window_size).run_scraper()

    def get_imdb_dfs(self):
        self.links_df = self.get_links_data()
        if not self.only_tconsts:
            relevant_df = self.imdb_df.loc[~(self.imdb_df["runtimeMinutes"] == "\\N") &
                                        ~(self.imdb_df["tconst"].isin(self.links_df["tconst"]))]
        else:
            relevant_df = self.imdb_df.loc[~(self.imdb_df["runtimeMinutes"] == "\\N") &
                                        (self.imdb_df["tconst"].isin(self.links_df["tconst"]))]

        return [relevant_df.loc[indexes] for indexes in divide_chunks(relevant_df.index,
                                                                      round(relevant_df.index.size / self.instances))]

    def get_links_data(self):
        if self.tconst_updated():
            return pd.read_csv(self.tconst_path)
        else:
            return self.concating_scraping_data()

    def tconst_updated(self):
        if self.tconst_path:
            try:
                tconst_update_time = file_updated_time(self.tconst_path)
                progress_update_time = file_updated_time(self.scraper_progress_path)
                return (tconst_update_time > progress_update_time) \
                    and (tconst_update_time > self.latest_directory_update())
            except:
                return False
        return False

    def latest_directory_update(self):
        directory_files = listdir_nohidden(self.scraping_directory)
        if len(directory_files) > 0:
            return max([file_updated_time(file) for file in directory_files])
        return 0

    def concating_scraping_data(self):
        progress_df = pd.read_csv(self.scraper_progress_path)
        if file_updated_time(self.scraper_progress_path) > self.latest_directory_update():
            dfs = [pd.read_csv(file_path) for file_path in filtered_file_paths(self.scraping_directory)]
            dfs.append(progress_df)
            final_df = pd.concat(dfs).drop_duplicates().reset_index(drop=True)
            final_df.to_csv(self.scraper_progress_path)
            return final_df
        else:
            return progress_df

class RunImdbScraper:

    def __init__(self, instance_id, chromedriver_path, relevant_df, scraping_data_directory,
                 use_proxy=False, window_size="1920,1080"):
        self.instance_id = instance_id
        self.driver = get_driver(use_proxy, window_size, chromedriver_path)
        self.relevant_df = relevant_df
        self.other_data = list()
        self.scraping_data_directory = scraping_data_directory
        self.starting_time = int(time.time())

    def run_scraper(self):
        try:
            for index, row in self.relevant_df.iterrows():
                next_data = self.get_movie_search_results(row["primaryTitle"], row["startYear"], row["tconst"], index)
                self.other_data.append(next_data)
                if index % 100 == 0:
                    print(f"The length of data for {self.instance_id} and index {index} is {len(self.convert_data())}")
                    self.save_progress_to_df()
            self.save_progress_to_df()
        except:
            self.save_progress_to_df()
            self.driver.quit()
            return self.convert_data()

    def convert_data(self):
        return [data for data in flatten(self.other_data) if type(data) == dict and has_some_data(data)]

    def get_movie_search_results(self, primary_title, start_year, tconst, index):
        return f'{primary_title} {start_year} {tconst} {self.instance_id} {index}'

    def save_progress_to_df(self):
        flattened_data = self.convert_data()
        if len(flattened_data) > 0:
            file_string = f"{self.scraping_data_directory}{self.instance_id}_{self.starting_time}.csv"
            pd.DataFrame(data=flattened_data).to_csv(file_string, index=False)


class OkruMovieCard:

    def __init__(self, tconst, element):
        self.tconst = tconst
        self.element = element

    def movie_dict(self):
        return {
            "link title": self.get_movie_link_title(),
            "link url": self.get_movie_url(),
            "movie duration": self.get_movie_duration(),
            "tconst": self.tconst
        }

    def get_movie_link_title(self):
        try:
            return self.element.find_element(By.CLASS_NAME, "video-card_n").get_attribute("text")
        except:
            return None

    def get_movie_url(self):
        try:
            return self.element.get_attribute("data-id")
        except:
            return None

    def get_movie_duration(self):
        try:
            return self.element.find_element(By.CLASS_NAME, "video-card_duration").text
        except:
            return None


class OkruPageScraper:

    def __init__(self, driver, movie_name, year, tconst, instance_id, index, all_data=[]):
        self.driver = driver
        self.movie_name = movie_name
        self.year = year
        self.tconst = tconst
        self.instance_id = instance_id
        self.index = index
        self.all_data = all_data
        self.okru_url = "https://ok.ru/video"

    def go_to_startpage(self):
        self.driver.get(self.okru_url)

    def get_search_button(self):
        element = self.driver.find_element(By.CLASS_NAME, "wrap-input__414z3")
        return element.find_element(By.TAG_NAME, "button")

    def on_default_page(self):
        try:
            return self.driver.find_element(By.CLASS_NAME, "portlet_h_name_t").text == "Weekly hits"
        except:
            return False

    def popup_available(self):
        try:
            self.popup()
            return True
        except:
            return False

    def popup(self):
        return self.driver.find_element(By.CLASS_NAME, "close-button__akasx")

    def close_popup(self):
        if self.popup_available():
            self.popup().click()

    def video_search(self):
        return self.driver.find_element(By.XPATH, "//input[@placeholder='Video search']")

    def search_movie(self):
        self.video_search().send_keys("{} ({})".format(self.movie_name, self.year), Keys.RETURN)

    def any_search_results(self):
        try:
            return not (self.driver.find_element(By.CLASS_NAME, "stub-empty_t").text == 'No search results found')
        except:
            return True

    def all_titles_movies(self):
        return [movie.text for movie in self.driver.find_elements(By.CLASS_NAME, "video-card_duration")]

    def movie_dict(self, element):
        return OkruMovieCard(self.tconst, element).movie_dict()

    def get_all_movie_info(self):
        try:
            return [self.movie_dict(element) for element in
                    self.driver.find_elements(By.CLASS_NAME, "video-card")]
        except:
            print("all movie info except block")
            return list()

    def scroll_for_all_results(self):
        movie_results = 0
        current_results = 1
        while movie_results != current_results:
            movie_results = current_results
            self.close_popup()
            self.scroll_bottom()
            time.sleep(3)
            current_results = len(self.all_titles_movies())
        return current_results

    def scroll_bottom(self):
        self.driver.execute_script("window.scrollTo(0,document.body.scrollHeight)")

    def get_movie_search_results(self):
        self.go_to_startpage()
        self.close_popup()
        self.video_search().clear()
        time.sleep(3)
        self.close_popup()
        self.search_movie()
        self.close_popup()
        if self.any_search_results():
            self.close_popup()
            time.sleep(5)
            if self.on_default_page():
                print("on default page if")
                return "pending"
            else:
                if self.index % 10 == 0:
                    print(f"Went into else of instance {self.instance_id} and index {self.index}")
                self.scroll_for_all_results()
                self.close_popup()
                results = self.get_all_movie_info()
                if len(results) == 0:
                    return [{'link title': None, 'link url': None, 'movie duration': None, 'tconst': self.tconst}]
                else:
                    return results
        else:
            no_match_dict = {'link title': None, 'link url': None, 'movie duration': None, 'tconst': self.tconst}
            return [no_match_dict]

    def restart_movie_search(self):
        try:
            self.driver.quit()
            self.driver = get_driver()
            return self.get_movie_search_results()
        except:
            no_match_dict = {'link title': None, 'link url': None, 'movie duration': None, 'tconst': self.tconst}
            return [no_match_dict]


class OkruScraper(RunImdbScraper):

    def __init__(self, instance_id, chromedriver_path, relevant_df, okru_directory,
                 use_proxy=False, window_size="1920,1080"):
        super().__init__(instance_id, chromedriver_path, relevant_df, okru_directory,
                         use_proxy=use_proxy, window_size=window_size)

    def get_movie_search_results(self, movie_name, year, tconst, index, all_data=[]):
        return OkruPageScraper(self.driver, movie_name, year, tconst, self.instance_id,
                               index, all_data).get_movie_search_results()

def main():
    from all_processes.load_yaml_vars import LoadYamlVars
    vars = LoadYamlVars(os.environ.get('YAML_FILE', './movie_configuration.yaml'))
    ScrapeAllImdb(instances=5, imdb_directory=vars.imdb_directory, 
                  chromedriver_path=vars.chromedriver_path, 
                  scraper_progress_path=vars.get_scraper_progress_path(OkruScraper), 
                  scraper_class=OkruScraper, 
                  scraping_directory=vars.scraper_paths.get('Okru').get('Data Directory') ).run_instances_scraper()

if __name__ == '__main__':
    main()
