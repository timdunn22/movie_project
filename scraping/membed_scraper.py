from scraper import Scraper

class MembedScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://membed1.com", "https://membed1.com/movies?page={}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("li", {"class": "video-block"})
    
    def get_link_tag(tag):
        return tag.find("a")
    
    def movie_link_url(self, tag):
        return "https://membed1.com{}".format(self.get_link_tag(tag)["href"])
    
    def movie_link_title(self, tag):
        return self.get_link_tag(tag).find("div", {"class": "name"}).text.strip()
    
    def more_results(self, page):
        soup = self.get_page_soup(page)
        try:
            soup.find("li", {"class": "next"}).text
            return False
        except:
            return True