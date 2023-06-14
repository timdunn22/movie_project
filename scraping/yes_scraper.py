from scraper import Scraper

class YesScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://w5.yesmovies123.me", "https://w5.yesmovies123.me/movies/page/{}/", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("article", {"class": "item movies"})
    
    def get_link_tag(self, tag):
        return tag.find("h3").find("a")
    
    def movie_link_url(self, tag):
        return get_link_tag(tag)["href"]
    
    def movie_link_title(self, tag):
        return get_link_tag(tag).text
    
    def more_results(self, page):
        soup = get_page_soup(page)
        return int(soup.find("div", {"class": "pagination"}).find("span").text.split("of")[-1].strip()) == page