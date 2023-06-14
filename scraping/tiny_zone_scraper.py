from scraping.scraper import *

class TinyZoneScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://tinyzonetv.to", "https://tinyzonetv.to/movie?page={}", page_logging)
    
    def get_movie_tags(self, soup):
        return [div.find('div', {'class': 'film-detail'}) for div in soup.find_all('div', {'class': 'flw-item'})]
    
    def movie_link_url(self, tag):
        try:
            return "https://tinyzonetv.to{}".format(self.link_tag(tag).get('href'))
        except:
            return None
    
    def movie_link_title(self, tag):
        try:
            return self.link_tag(tag).text.strip()
        except:
            return None
        
    def link_tag(self, tag):
        return tag.find('a')
    
    def div_info_tag(self, tag):
        return tag.find('div', {'class': 'film-infor'})
    
    def spans(self, tag):
        return [span.text.strip() for span in self.div_info_tag(tag).find_all('span')]
    
    def movie_year(self, tag):
        try:
            return flatten([re.findall(year(), span_text) for span_text in self.spans(tag)])[0]
        except:
            return None
    
    def movie_duration(self, tag):
        try:
            return list(filter(lambda span_text: 'm' in span_text, self.spans(tag)))[0].split('m')[0]
        except:
            return None
    
    def movie_quality(self, tag):
        try:
            return self.spans(tag)[0]
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
            soup.find("a", {"title": "Last"}).text.strip()
            return False
        except:
            return True