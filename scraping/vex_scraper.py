from scraper import Scraper

class VexScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://vexmovies.pw", "https://vexmovies.pw/movies?page={}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("div", {"class": "film-detail"})
    
    def get_link_tag(self, tag):
        return tag.find("a")
    
    def movie_link_url(self, tag):
        return "https://vexmovies.pw{}".format(self.get_link_tag(tag)["href"])
    
    def movie_link_title(self, tag):
        return self.get_link_tag(tag)["title"]
    
    def movie_year(self, tag):
        return tag.find("span", {"class": "fdi-item"}).text
    
    def movie_duration(self, tag):
        return tag.find("span", {"class": "fdi-duration"}).text
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "year": self.movie_year(tag), "duration": self.movie_duration(tag)}
    
    def more_results(self, page):
        soup = self.get_page_soup(page)
        return int(soup.find("a", {"title": "Last"})["href"].split("page=")[-1]) == page