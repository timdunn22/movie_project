from scraper import Scraper

class SoapScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://soap2day.casa", "https://soap2day.casa/movies/page/{}", page_logging)
    
    def get_movie_tags(self, soup):
        return soup.find_all("div", {"class": "ml-item"})
    
    def soap_tip_div(div):
        return div.find("div", {"id": "hidden_tip"})
    
    def movie_link_url(self, tag):
        return tag.find("a")["href"]
    
    def movie_link_title(self, tag):
        return soap_tip_div(tag).find("div", {"class": "qtip-title"}).text
    
    def movie_quality(self, tag):
        try:
            return soap_tip_div(div).find("div", {"class": "jtip-quality"}).text
        except:
            return None
    
    def movie_duration(self, tag):
        return soap_tip_div(tag).find_all("div", {"class": "jt-info"})[2].text
        
    def movie_info(self, tag):
        return {"link url" : movie_link_url(tag), "movie_title": movie_link_title(tag), 
                "quality": movie_quality(tag), "duration": movie_duration(tag)}
    
    def more_results(self, page):
        soup = get_page_soup(page)
        return int(soup.find("div", {"class": "pagination"}).find("span").text.split("of")[-1].strip()) == page