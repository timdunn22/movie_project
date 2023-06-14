from scraping.scraper import *

class SwatchScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://swatchfree.in/movies/", 'https://swatchfree.in/movies/page/{}', page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find('div', {'id': 'archive-content'}).find_all('article', {'class': 'item movies'})
    
    def get_link_tag(self, tag):
        return self.get_data_tag(tag).find('a')
    
    def get_data_tag(self, tag):
        try:
            return tag.find('div', {'class': 'data'})
        except:
            return None
    
    def movie_link_url(self, tag):
        try:
            return self.get_link_tag(tag).get('href')
        except:
            return None
    
    def movie_link_title(self, tag):
        try:
            return self.get_link_tag(tag).text.strip()
        except:
            return None
        
    def meta_data_span_text(self, tag):
        return [span.text.strip() for span in tag.find('div', {'class': 'metadata'}).find_all('span')]
    
    def movie_year(self, tag):
        try:
            return flatten([re.findall(year(), span_text) for span_text in self.meta_data_span_text(tag)])[0]
        except:
            return None
    
    def movie_duration(self, tag):
        try:
            spans = list(filter(lambda span_text: 'min' in span_text, self.meta_data_span_text(tag)))
            return spans[0].strip().split(' min')[0]
        except:
            return None
        
    def movie_quality(self, tag):
        try:
            return tag.find('span', {'class': 'quality'}).text.strip()
        except:
            return None
    
    def get_movie_description(self, tag):
        try:
            return tag.find('div', {'class': 'texto'}).text.strip()
        except:
            return None
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "description": self.get_movie_description(tag), "year": self.movie_year(tag), 
                "duration": self.movie_duration(tag), "quality": self.movie_quality(tag)}
    
    def is_last_page(self, page):
        soup = self.get_page_soup(page)
        try:
            if page == 0:
                return False
            soup.find('i', {'id': 'nextpagination'}).text
            return False
        except:
            return True