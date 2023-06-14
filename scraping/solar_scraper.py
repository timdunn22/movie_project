from scraper import Scraper

class SolarScraper(Scraper):
    def __init__(self, page_logging=True):
        super().__init__("https://ww3.solarmovie.to/movies.html", "https://ww3.solarmovie.to/movies/{}.html", page_logging)
    
    def get_movie_tags(self, soup):
        divs = [div for div in soup.find_all("div", {"class": "card"})]
        return list(filter(lambda div: None not in [div.find("a", {"class": "poster"}), 
                                                     div.find("h2", {"class": "card-title"})], divs))
    
    def movie_link_url(self, tag):
        return soup.find("a", {"class": "poster"})["href"]
    
    def movie_link_title(self, tag):
        return soup.find("h2", {"class": "card-title"}).text
    
    def more_results(self, page):
        soup = get_page_soup(page)
        return int(soup.find("a", {"aria-label": "Last"})["href"].split("/")[-1].split(".html")[0]) == page