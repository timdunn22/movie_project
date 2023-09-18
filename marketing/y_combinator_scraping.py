import pysnooper
from movie_project.common_methods import (re, merge_many_dicts, os, 
                                          common_null_values, get_selenium_soup, sample )
from movie_project.marketing.kftv_scraper import (UndetectedSeleniumScraper, 
                                                  MultiThreadedSeleniumScraper, 
                                                  ThreadManagement)
from movie_project.all_processes.load_yaml_vars import LoadYamlVars
import time
from selenium.webdriver.common.by import By
from selenium import webdriver
from selenium.webdriver.firefox.options import Options



def typical_driver(host, port, headless=True):
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-extensions")
    options.add_argument(f"--proxy-server={host}:{port}")
    if headless:
        options.add_argument("--headless")
    return webdriver.Firefox(options=options, )

def get_driver_from_options(options):
    num = sample(list(range(1,6)), 1)[0]
    if num == 1:
        return webdriver.Firefox(options=options)
    elif num == 2:
        return webdriver.Edge(options=options)
    elif num == 3:
        return webdriver.ChromiumEdge(options=options)
    elif num == 4:
        return webdriver.Ie(options=options)
    elif num == 5:
        return webdriver.Safari(options=options)

class YCombinatorPageScraper:

    def __init__(self, soup):
        self.soup = soup

    def company_size(self):
        try:
            return self.card().find_all('div')[1].find_all('div')[1].find_all('span')[1].text
        except:
            return None

    def company_crunchbase_profile(self):
        try:
            return self.soup.find('a', {'title': "Crunchbase profile"})["href"]
        except:
            return None

    def company_linkedin_profile(self):
        try:
            return self.soup.find('a', {'title': "LinkedIn profile"})['href']
        except:
            return None

    def company_twitter_account(self):
        try:
            return self.soup.find('a', {'title': "Twitter account"})['href']
        except:
            return None

    def card(self):
        try:
            return self.soup.find('div', {'class': 'ycdc-card space-y-1.5 sm:w-[300px]'})
        except:
            return None

    def company_name(self):
        try:
            return self.card().find('div').text
        except:
            return None

    def company_founded(self):
        try:
            return self.card().find_all('div')[1].find('div').find_all('span')[1].text
        except:
            return None

    def company_location(self):
        try:
            return self.card().find_all('div')[1].find_all('div')[2].find_all('span')[1].text
        except:
            return None

    def founder_section(self):
        try:
            return self.soup.find_all('div', {'class': "ycdc-card"})[-1].parent.parent
        except:
            return None

    def company_description(self):
        try:
            return self.soup.find('p', {'class': "whitespace-pre-line"}).text
        except:
            return None

    def company_url(self):
        try:
            return self.soup.find('div', {'class': "my-8 mb-4"}).find('a', href=re.compile('http'))['href']
        except:
            return None

    def company_founders(self):
        try:
            cards = self.founder_section().find_all('div', {'class': 'ycdc-card'})
            dicts = [{f'Founder {index+1} LinkedIn': card.find('a', 
                                                               title="LinkedIn profile"
                                                               )['href'], 
            f'Founder {index+1} Name': card.find('div', {'class': 
                                                         'leading-snug'}).find('div').text,
            f'Founder {index+1} Description': card.previous_sibling.find('p').text}
            for index, card in enumerate(cards)]
            return merge_many_dicts(dicts)
        except:
            return None

    def company_info(self):
        try:
            return merge_many_dicts([ self.initial_dict(), self.company_founders() ])
        except:
            return None

    def verify_method(self):
        return self.company_name()

    def initial_dict(self):
        return {
                'Company Size': self.company_size(),
                'Company Location': self.company_location(),
                'Company Url': self.company_url(),
                'Company Description': self.company_description(),
                'Company Crunchbase Profile': self.company_crunchbase_profile(),
                'Company LinkedIn Profile': self.company_linkedin_profile(),
                'Company Twitter Account': self.company_twitter_account(),
                'Company Name': self.company_name(),
                'Company Founded': self.company_founded()
        }

class YCombinatorScraper(UndetectedSeleniumScraper):
    unique_identifier = 'YCombinator URL'
    item_file_col = 'href'
    confirmed_extracted_col = 'Company Name'

    def __init__(self, progress_directory, proxy_file=None, items=None, 
                 instance_id=None, not_found_data_file=None):
        super().__init__(unique_identifier=self.unique_identifier, 
                         item_file_col=self.item_file_col, 
                         not_found_data_file=not_found_data_file,
                         progress_directory=progress_directory, 
                         proxy_file=proxy_file, 
                         items=items, 
                         page_scraper_class=YCombinatorPageScraper,
                         instance_id=instance_id)

    def get_driver(self):
        return typical_driver(self.proxy_object.proxy_host, 
                              self.proxy_object.proxy_port, True)

    def is_not_found(self, soup):
        try:
            return soup.find('h1').text.strip() == '404'
        except:
            return False

    # @pysnooper.snoop(depth=1)
    def verify_data(self, soup, url):
        try:
            return YCombinatorPageScraper(soup=soup, url=url).verify_data()
        except:
            return None


# @pysnooper.snoop(depth=4)
def main():
    vars = LoadYamlVars(os.environ.get('YAML_FILE', 
                                       './movie_project/all_processes/movie_configuration.yaml'))
    progress_directory = vars.marketing.get('YComb Progress Directory')
    MultiThreadedSeleniumScraper(instances=5, 
                                 progress_directory=progress_directory, 
                                 item_file=vars.marketing.get('YComb Links'), 
                                 not_found_item_file=vars.marketing.get('YComb Not Found File'),
                                 scraper_class=YCombinatorScraper, 
                                 proxy_file=vars.proxies_path, 
                                 thread_class=ThreadManagement,
                                 output_file=vars.marketing.get('YComb Profiles')
                                 ).run_instances_and_concat()

if __name__ == '__main__':
    main()