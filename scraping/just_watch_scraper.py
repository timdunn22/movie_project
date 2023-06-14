from common_methods import *

class JustWatchScraper:
    
    def __init__(self, url, proxies=None, link_type='tubi', split_string='tubitv.com%2Fmovies%2F'):
        self.url = url
        self.soup = get_soup_url(url, proxies=proxies)
        self.link_type = link_type
        self.split_string = split_string
                
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
    
    def get_link(self):
        return list(filter(lambda link: self.link_type in link, self.get_links()))[0]
    
    def get_url(self):
        try:
            return self.get_tubi_link().split(self.split_string)[1].split('%')[0]
        except:
            return None
        
    def get_links_dict(self):
        new_dict = {}
        count = 1
        for link in self.get_links():
            new_dict['link {}'.format(count)] = link
            count += 1
        return new_dict
    
    def get_movie_info(self):
        starting_dict = {'tconst': self.get_imdb_tconst(), 'movie_duration': self.movie_runtime(),"movie_title": self.movie_title(), 'just_watch_rating': self.just_watch_rating(), 'just_watch_url': self.url}
        starting_dict.update(self.get_links_dict())
        return starting_dict        