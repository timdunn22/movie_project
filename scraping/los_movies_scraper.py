from scraping.scraper import *

class LosMoviesScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://losmovies.pics/#challenge", 'https://losmovies.pics/?page={}', page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all('div', {'class': 'showEntityMovie'})
    
    def div_info_tag(self, tag):
        return tag.find('div', {'class': 'showRowImage'})
    
    def movie_link_url(self, tag):
        try:
            return self.div_info_tag(tag).find('a').get('href')
        except:
            return None
    
    def movie_link_title(self, tag):
        try:
            return tag.find('h4', {'class': 'showRowName'}).text.strip()
        except:
            return None
    
    def movie_year(self, tag):
        try:
            return re.findall(year(), self.div_info_tag(tag).find('img').get('src'))[0]
        except:
            return None
        
    def movie_quality(self, tag):
        try:
            return tag.find('div', {'class': 'movieQuality'}).text.strip()
        except:
            return None
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                 "year": self.movie_year(tag), "quality": self.movie_quality(tag)}
    
    def is_last_page(self, page):
        try:
            if page == 0:
                return False
            soup = self.get_page_soup(page)
            soup.find('a', {'class': 'nextLink'}).text
            return False
        except:
            return True