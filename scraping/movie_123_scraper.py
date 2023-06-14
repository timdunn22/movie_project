from scraper import Scraper

class Movie123Scraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://123moviesgoto.com", "https://123moviesgoto.com/movies?page{}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("div", {"class": "ml-item"})
    
    def prime_stats_div(tag):
        return tag.find("div", {"class": "film-stats"})
    
    def movie_link_url(self, tag):
        return "https://123moviesgoto.com{}".format(div.find("a")["href"])
    
    def movie_link_title(self, tag):
        return tag.find("div", {"class": "qtip-title"}).text.strip()
    
    def movie_year(self, tag):
        return tag.find("div", {"class": "jtip-top"}).find_all("div", {"jt-info"})[1].text
    
    def movie_duration(self, tag):
        return tag.find("div", {"class": "jtip-top"}).find_all("div", {"jt-info"})[2].text
    
    def movie_quality(self, tag):
        return tag.find("div", {"class": "jtip-quality"}).text
        
    def movie_info(self, tag):
        return {"link url" : movie_link_url(tag), "movie_title": movie_link_title(tag), 
                "year": movie_year(tag), "duration": movie_duration(tag), "quality": movie_quality(tag)}
    
    def more_results(self, page):
        soup = get_page_soup(page)
        return not soup.find_all("a", {"class": "swchItem"})[-1].find("span").text == "»"