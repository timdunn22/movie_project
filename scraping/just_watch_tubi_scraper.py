from common_methods import *

class JustWatchTubiScraper:
    
    def __init__(self, url, proxies=None):
        self.url = url
        self.soup = get_soup_url(url, proxies=proxies)
                
    def get_imdb_tconst(self):
        try:
            return self.soup.find('div', {'v-uib-tooltip': 'IMDB'}).find('a').get('href').split('title/')[1].split('/')[0]
        except:
            return None
    
    def movie_runtime(self):
        try:
            return self.soup.find_all('h3', string=re.compile("Runtime"))[0].parent.next_sibling.text
        except:
            return None
    
    def movie_year(self):
        try:
            text = self.soup.find('div', {'class': 'title-block'}).find('span').text.strip()
            text = re.sub('\(', '', text)
            text = re.sub('\)', '', text)
            return text
        except:
            return None
        
    
    def movie_title(self):
        try:
            return self.soup.find('div', {'class': 'title-block'}).find('h1').text.strip()
        except:
            return None
    
    def just_watch_rating(self):
        try:
            return self.soup.find('div', {'v-uib-tooltip': 'JustWatch Rating'}).text.strip()
        except:
            return None
    
    def get_divs(self):
        try:
            return self.soup.find_all('div', {'class': 'presentation-type price-comparison__grid__row__element__icon'})
        except:
            return None
    
    def get_links(self):
        return [div.find('a').get('href') for div in self.get_divs()]
    
    def get_tubi_link(self):
        return list(filter(lambda link: 'tubi' in link, self.get_links()))[0]
    
    def get_tubi_url(self):
        try:
            return self.get_tubi_link().split('tubitv.com%2Fmovies%2F')[1].split('%')[0]
        except:
            return None
    
    def get_movie_info(self):
        return {"link url": self.get_tubi_url(), 'tconst': self.get_imdb_tconst(), 'movie_duration': self.movie_runtime(),
                "movie_title": self.movie_title(), 'just_watch_rating': self.just_watch_rating(), 'just_watch_url': self.url}
        