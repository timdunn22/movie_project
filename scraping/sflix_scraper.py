from scraping.scraper import *

class SflixScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__('https://sflix.to/movie', "https://sflix.to/movie?page={}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all('div', {'class': 'film-detail'})
    
    def link_tag(self, tag):
        return tag.find('h2', {'class': 'film-name'}).find('a')
    
    def movie_link_url(self, tag):
        try:
            return "https://sflix.to{}".format(self.link_tag(tag)['href'])
        except:
            return None
    
    def movie_link_title(self, tag):
        try:
            return self.link_tag(tag).text.strip()
        except:
            return None
    
    def movie_year(self, tag):
        try:
            return tag.find_all('span', {'class': 'fdi-item'})[-1].text.strip()
        except:
            return None
        
    def movie_quality(self, tag):
        try:
            return tag.find_all('span', {'class': 'fdi-item'})[1].find('strong').text.strip()
        except:
            return None
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "year": self.movie_year(tag), "quality": self.movie_quality(tag)}
    
    def is_last_page(self, page):
        try:
            soup = self.get_page_soup(page)
            return soup.find('a', {'title': 'Last'}) == None
        except:
            True