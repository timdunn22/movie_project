from scraper import Scraper

class ChillScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://123chill.to", "https://123chill.to/movies/page/{}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("a", {"class": "ml-mask"})
    
    def movie_link_url(self, tag):
        return tag["href"]
    
    def movie_link_title(self, tag):
        return tag.find("span", {"class": "mli-info"}).find("h2").text
    
    def movie_year(self, tag):
        return tag["data-year"]
    
    def movie_duration(self, tag):
        return tag["data-duration"]
    
    def movie_quality(self, tag):
        try:
            return tag.find("span", {"class": "mli-quality"}).text
        except:
            return None
    
    def movie_image(self, tag):
        return tag.find("img")["data-original"]
        
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag), 
                "image": self.movie_image(tag), "quality": self.movie_quality(tag)}
    
    def more_results(self, page):
        soup = self.get_page_soup(page)
        return soup.find("ul", {"class": "pagination"}).find_all("li")[-1].text.strip() == "Next"