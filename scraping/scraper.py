from common_methods import *

class Scraper:
    def __init__(self, main_link, page_url_format, page_logging=True):
        self.main_link = main_link
        self.page_url_format = page_url_format
        self.page_logging = page_logging
    
    def run_scraper(self):
        data = list()
        soup = get_soup_url(self.get_page_url(1))
        print('got past initial soup')
        time.sleep(3)
        try:
            page = 0
            while (not self.is_last_page(page)):
                page += 1
                if self.page_logging:
                    print(page)
                time.sleep(3)
                data.append(self.movies_info(page))
                print(np.unique([record['link url'] for record in flatten(data)]).size)
            return pd.DataFrame(data=flatten(data))
        except:
            print('entered exception')
            return data
    
    def get_page_url(self, page):
        return self.page_url_format.format(page)
    
    def get_page_soup(self, page):
        return get_soup_url(self.get_page_url(page))
    
    def is_last_page(self, page):
        return False
    
    def movies_info(self, page):
        soup = self.get_page_soup(page)
        tags = self.get_movie_tags(soup)
        return [self.movie_info(tag) for tag in tags]
    
    def movie_info(self, tag):
        return {"link url" : self.movie_link_url(tag), "movie_title": self.movie_link_title(tag)}