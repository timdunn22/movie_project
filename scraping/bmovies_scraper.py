from scraping.selenium_scraper import SeleniumScraper
from common_methods import *


class BmoviesScraper(SeleniumScraper):
    def __init__(self, output_file, proxy_file, output_directory, instances=5):
        super().__init__("https://bmovies.co/movies/", 'https://bmovies.co/movies/page/{}', 
                         output_file=output_file, proxy_file=proxy_file, instances=instances,
                         output_directory=output_directory)
        self.current_driver = None
        self.vars = None

    def get_movie_tags(self, soup):
        return soup.find_all('div', {'class': 'featuredItems'})

    def get_poster(self, tag):
        return tag.find('img').get('src')

    def get_trailer(self, alternative_soup):
        try:
            trailer_div = alternative_soup.find('div', {'id': 'trailer_tab'})
            return trailer_div.find('iframe').get('src')
        except:
            return None

    def get_link_soup(self, tag):
        return self.get_soup_url(self.movie_link_url(tag))
        
    def div_info_tag(self, tag):
        return tag.find('div', {'class': 'popcontents'})

    def movie_link_url(self, tag):
        try:
            return tag.find('a').get('href')
        except:
            return None

    def jt_info_tags(self, tag):
        return [jt_info_tag.text.strip() for jt_info_tag in self.div_info_tag(tag).find_all('div', {'class': 'jt-info'})]

    def movie_link_title(self, tag):
        try:
            return self.div_info_tag(tag).find('h4').text.strip()
        except:
            return None

    def movie_year(self, tag):
        try:
            return flatten([re.findall(year(), jt_info) for jt_info in self.jt_info_tags(tag)])[0]
        except:
            return None

    def movie_duration(self, tag):
        try:
            return list(filter(lambda jt_tag: 'min' in jt_tag, self.jt_info_tags(tag)))[0].split('min')[0]
        except:
            return None

    def get_quality(self, alternative_soup):
        try:
            li = [li for li in alternative_soup.find('div', {'class': 'chartdescriptionRight'}).find_all('li') 
                  if 'quality' in li.find('strong').text.lower()][0]
            return li.find('a').text
        except:
            return None

    def get_alternative_soup(self, tag, selenium_instance):
        return selenium_instance.get_soup_url(self.movie_link_url(tag))

    def get_alternative_link_dict(self, tag, selenium_instance):
        alternative_soup = self.get_alternative_soup(tag, selenium_instance)
        return {"trailer": self.get_trailer(alternative_soup), "quality": self.get_quality(alternative_soup)}

    def movie_info(self, tag, selenium_instance):
        primary_dict =  {"link url": self.movie_link_url(tag), "movie_title": self.movie_link_title(tag),
                          "year": self.movie_year(tag), "duration": self.movie_duration(tag), 
                          "poster": self.get_poster(tag)}
        print('past primary dict')
        print('primary dict is', primary_dict)
        alternative_dict = self.get_alternative_link_dict(tag, selenium_instance)
        return {**primary_dict, **alternative_dict}


    def set_vars(self, single_selenium_instance):
        url = self.get_page_url(1)
        soup = single_selenium_instance.get_soup_url(url, count=25)
        pagination = self.get_pagination(soup)
        if pagination:
            self.vars = self.get_pages(pagination)
        else:
            time_count = 1
            while not pagination:
                time.sleep(1)
                # print("time count is", time_count)
                single_selenium_instance.driver.save_screenshot('/Users/timdunn/Downloads/screenshot_sample.png')
                time_count += 1
                soup = get_selenium_soup(single_selenium_instance.driver)
                pagination = self.get_pagination(soup)
                if time_count > 20:
                    single_selenium_instance.reset_driver()
                    self.set_vars(single_selenium_instance)
            self.vars = self.get_pages(pagination)

    def get_pagination(self, soup):
        return soup.find('div', {'class': 'pagination'})

    def get_pages(self, pagination):
        last_page = int(pagination.find_all('li')[-2].find('a').text)
        return list(range(1, last_page + 1))

    def is_last_page(self, page):
        try:
            if page == 0:
                return False
            soup = self.get_page_soup(page)
            return not soup.find('div', {'class': 'pagination'}).find_all('li')[-1].find('a').text == 'Next >'
        except:
            return True