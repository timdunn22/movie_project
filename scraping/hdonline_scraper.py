from scraping.scraper import *

class HdonlineScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://w10.hdonline.eu/movies", "https://w10.hdonline.eu/movies/page/{}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all('div', {'class': 'item more-info'})
    
    def movie_link_url(self, tag):
        try:
            return tag.find('a').get('href')
        except:
            return None
    
    def movie_link_title(self, tag):
        try:
            return tag.find('h3').find('a').text.strip()
        except:
            return None
        
    def meta_tag(self, tag):
        return tag.find('div', {'class': 'meta'})
    
    def movie_year(self, tag):
        try:
            return self.meta_tag(tag).find('a').text.strip()
        except:
            return None
    
    def movie_duration(self, tag):
        try:
            return self.meta_tag(tag).text.strip().split(' min')[0].split(' ')[-1]
        except:
            return None
    
    def movie_quality(self, tag):
        try:
            return tag.find('div', {'class': 'quality'}).text.strip()
        except:
            return None
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "year": self.movie_year(tag), "duration": self.movie_duration(tag), 
                "quality": self.movie_quality(tag)}
    
    def is_last_page(self, page):
        try:
            if page == 0:
                return False
            soup = self.get_page_soup(page)
            return  '»' not in soup.find("ul", {"class": "pagination"}).find_all('a')[-1].text.strip()
        except:
            return True