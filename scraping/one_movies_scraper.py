from scraping.scraper import *

class OneMoviesScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://1movies.life/allmovies_3", "https://1movies.life/allmovies_3/{}/23", page_logging)
    
    def get_movie_tags(self, soup):
        return [div.find('div', {'class': 'poster'}) for div in soup.find_all('div', {'class': 'offer_box'})]
    
    def movie_link_url(self, tag):
        try:
            return "https://1movies.life{}".format(tag.get('data-href'))
        except:
            return None
    
    def movie_link_title(self, tag):
        try:
            return tag["data-name"]
        except:
            return None
    
    def movie_year(self, tag):
        try:
            return tag["data-year"]
        except:
            return None
    
    def movie_duration(self, tag):
        try:
            return tag["data-duration"].split(' min')[0]
        except:
            return None
    
    def movie_quality(self, tag):
        try:
            return tag["data-quality"]
        except:
            return None
        
    def movie_description(self, tag):
        try:
            return tag["data-desc"]
        except:
            return None
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "description": self.movie_description(tag), "year": self.movie_year(tag), 
                "duration": self.movie_duration(tag), "quality": self.movie_quality(tag)}
    
    def is_last_page(self, page):
        try:
            if page == 0:
                return False
            soup = self.get_page_soup(page)
            soup.find("ul", {"class": "pagination"}).find("li", {'class': 'last'}).find('span')
            return False
        except:
            return True