from scraping.scraper import Scraper

class PrimeScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://primewire.mx", "https://primewire.mx/movie?page={}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("div", {"class": "fbr-line fbr-content"})
    
    def get_link_tag(tag):
        return tag.find("h2", {"class": "film-name"}).find("a")
    
    def prime_stats_div(tag):
        return tag.find("div", {"class": "film-stats"})
    
    def movie_link_url(self, tag):
        return "https://primewire.mx{}".format(self.get_link_tag(tag)["href"])
    
    def movie_link_title(self, tag):
        return self.get_link_tag(tag)["title"]
    
    def movie_year(self, tag):
        return self.prime_stats_div(tag).find("span").text
    
    def movie_duration(self, tag):
        return self.prime_stats_div(tag).find_all("span")[-1].text
    
    def movie_quality(self, tag):
        return tag.find("div", {"class": "fbrl-quality"}).find("span").text
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "year": self.movie_year(tag), "duration": self.movie_duration(tag), "quality": self.movie_quality(tag)}
    
    def more_results(self, page):
        soup = self.get_page_soup(page)
        return int(soup.find("a", {"title": "Last"})["href"].split("page=")[-1]) == page