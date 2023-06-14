from scraping.scraper import *

class GoScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://gomovies-online.link", "https://gomovies-online.link/all-films/{}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all(self.div_correct)
    
    def div_correct(self, div):
        return div.has_attr('data-filmname') and div.has_attr('data-quality') and div.has_attr('data-duration')
    
    def prime_stats_div(self, tag):
        return tag.find("div", {"class": "film-stats"})
    
    def movie_link_url(self, tag):
        return "https://gomovies-online.link{}".format(tag.find("a")["href"])
    
    def movie_link_title(self, tag):
        return tag["data-filmname"]
    
    def movie_year(self, tag):
        return tag["data-year"]
    
    def movie_duration(self, tag):
        return tag["data-duration"]
    
    def movie_quality(self, tag):
        return tag["data-quality"].split("itemAbsolute_")[-1]
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "year": self.movie_year(tag), "duration": self.movie_duration(tag), "quality": self.movie_quality(tag)}
    
    def is_last_page(self, page):
        soup = self.get_page_soup(page)
        try:
            if page == 0:
                return False
            int(soup.find("li", {"class": "last"}).find("a")["data-page"])
            return False
        except:
            return True