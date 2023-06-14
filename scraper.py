from requirements import *

class Scraper:
    def __init__(self, main_link, page_url_format, page_logging=True):
        self.main_link = main_link
        self.page_url_format = page_url_format
        self.page_logging = page_logging
    
    def run_scraper():
        data = list()
        soup = get_soup_url(get_page_url(1))
        time.sleep(3)
        try:
            page = 0
            while (not is_last_page(page)):
                if self.page_logging:
                    print(page)
                time.sleep(3)
                data.append(movies_info(page))
                page += 1
                return pd.DataFrame(data=flatten(data))
        except:
            return data
    
    def get_page_url(self, page):
        return self.page_url_format.format(page)
    
    def get_page_soup(self, page):
        return get_soup_url(get_page_url(page))
    
    def get_soup_url(self, url):
        return BeautifulSoup(url.content, "html.parser")
    
    def more_results(self, soup):
        return False
    
    def is_last_page(self, page):
        try:
            if page == 0:
                return False
            return more_results(page)
        except:
            return True
        
    def more_results(self, page):
        return False
        
    def movies_info(self, page):
        soup = get_page_soup(self, page)
        tags = get_movie_tags(soup)
        return [movie_info(tag) for tag in tags]
    
    def movie_info(self, tag):
        return {"link url" : movie_link_url(tag), "movie_title": movie_link_title(tag)}
    