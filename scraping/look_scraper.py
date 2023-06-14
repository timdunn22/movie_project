from scraper import Scraper

class LookScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://lookmovie2.to", "https://lookmovie2.to/page/{}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("div",{"class": "mv-item-infor"})
    
    def get_link_tag(self, tag):
        return tag.find("a")
    
    def movie_link_url(self, tag):
        return self.get_link_tag(tag)["href"]
    
    def movie_link_title(self, tag):
        return self.get_link_tag(tag).text
    
    def more_results(self, page):
        soup = self.get_page_soup(page)
        return len(soup.find_all("a", "pagination_next")) == 1