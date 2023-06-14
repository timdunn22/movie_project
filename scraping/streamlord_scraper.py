from scraper import Scraper

class StreamlordScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("http://www.streamlord.com", "http://www.streamlord.com/movies.php?page={}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("div", {"class": "movie-grid"})
    
    def div_title_div(tag):
        return tag.find("div", {"class": "movie-grid-title"})
    
    def movie_link_url(self, tag):
        return "http://www.streamlord.com/{}".format(div.find("li", {"class": "movie"}).find_all("a")[-1]["href"])
    
    def movie_link_title(self, tag):
        return div_title_div(tag).text.strip()
    
    def movie_year(self, tag):
        return div_title_div(tag).find("span", {"class": "movie-grid-year"}).text
    
    def movie_duration(self, tag):
        return div_title_div(tag).find("span", {"class": "movie-grid-runtime"}).text
        
    def movie_info(self, tag):
        return {"link url" : movie_link_url(tag), "movie_title": movie_link_title(tag), 
                "year": movie_year(tag), "duration": movie_duration(tag)}
    
    def more_results(self, page):
        soup = get_page_soup(page)
        return not soup.find("div", {"id": "pagination"}).find_all("a")[-1].text.strip().split(" ")[0] == "NEXT"