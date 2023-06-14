from common_methods import *
from scraping.hdonline_scraper import *
from scraping.bmovies_scraper import *
from scraping.chill_scraper import *
from scraping.go_scraper import *
from scraping.look_scraper import *
from scraping.los_movies_scraper import *
from scraping.membed_scraper import *
from scraping.movie_123_scraper import *
from scraping.one_movies_scraper import *
from scraping.prime_scraper import *
from scraping.sflix_scraper import *
from scraping.soap_scraper import *
from scraping.solar_scraper import *
from scraping.streamlord_scraper import *
from scraping.swatch_scraper import *
from scraping.tiny_zone_scraper import *
from scraping.vex_scraper import *
from scraping.yes_scraper import *
from scraping.youtube_playlist_scraper import *
from scraping.okru_scraper import *
from all_processes.load_yaml_vars import LoadYamlVars


class RunAllScrapers(LoadYamlVars):

    def __init__(self, configuration_file="./movie_configuration.yaml"):
        super().__init__(configuration_file)
        self.scraper_dfs = list()

    def run_a_scraper(self, scraper_object):
        try:
            return scraper_object().run_scraper()
        except:
            print('There was an exception running the {} scraper'.format(scraper_object().main_link))

    def run_all_scrapers(self):
        for scaper_object in self.scrapers():
            try:
                self.scraper_dfs.append(self.run_a_scraper(scaper_object))
            except:
                pass

    def run_unusual_scrapers(self, instances=6, use_proxy=False):
        unusual_scrapers = [OkruScraper]
        for scraper in unusual_scrapers:
            scraper_progress_path = self.get_scraper_progress_path(scraper)
            scraping_directory = self.get_scraping_directory(scraper)
            scraping_tconst_path = self.get_scraper_tconst_path(scraper)
            self.scraper_dfs.append(ScrapeAllImdb(instances, self.imdb_directory, self.chromedriver_path,
                                                  scraper_progress_path, scraper, scraping_directory,
                                                  tconst_path=scraping_tconst_path,
                                                  use_proxy=use_proxy).run_instances_scraper())

    def scrapers(self):
        return [BmoviesScraper, GoScraper, ChillScraper, HdonlineScraper, LookScraper,
                LosMoviesScraper, MembedScraper, Movie123Scraper, OneMoviesScraper, PrimeScraper,
                SflixScraper, SoapScraper, SolarScraper, StreamlordScraper,
                SwatchScraper, TinyZoneScraper, VexScraper, YesScraper]

    def concat_all_dfs(self):
        return reset_and_copy(pd.concat(self.scraper_dfs).drop_duplicates())
