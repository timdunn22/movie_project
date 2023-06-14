
from common_methods import *

class SingleSeleniumScraper:

    def __init__(self, proxies, instance_id=1, checking_function=None):
        self.proxies = proxies
        self.driver = get_selenium_driver(proxies=self.proxies, headless=True)
        self.instance_id = instance_id
        self.data = list()
        self.checking_function = checking_function
    
    def navigate_url(self, url):
        self.driver.get(url)

    def get_soup_url(self, url, count=1, checking_function=None):
        try:
            self.navigate_url(url)
            time.sleep(count +2)
            # print('after sleep ')
            # soup = get_selenium_soup(self.driver)
            # function_value = self.checking_function(soup)
            soup = get_selenium_soup(self.driver) 
            if checking_function is not None:
                if self.try_checking_function(soup, checking_function) is not None:
                    return soup
                timeout_count = 0
                while self.try_checking_function(soup, checking_function) is None and ( timeout_count < 30 ):
                    time.sleep(1)
                    timeout_count += 1
                    soup = get_selenium_soup(self.driver) 
                if timeout_count == 30:
                    # print('enter soup reset')
                    return self.repeat_checking(soup, checking_function, url)
                return soup
            return soup
            # while not function_value or count > 20:
            #     time.sleep(1)
            #     count += 1
            #     print('after while sleeping')
            #     soup = get_selenium_soup(self.driver)
            #     function_value = self.checking_function(soup)
        except Exception as e:
            # print('into main exception soup reset')
            soup = get_selenium_soup(self.driver) 
            return self.repeat_checking(soup, checking_function, url)
            # if 'proxy' in str(e).lower():
            #     self.reset_driver()
            # else:

    def repeat_checking(self, soup, checking_function, url):
        while self.try_checking_function(soup, checking_function) is None:
            self.reset_driver()
            self.navigate_url(url)
            time.sleep(5)
            soup = get_selenium_soup(self.driver) 
        return soup
            
    def try_checking_function(self, soup, checking_function):
        try:
            return checking_function(soup)
        except:
            return None

    def go_to_page(self, page):
        self.get_soup_url(self.get_page_url(page))

    def reset_driver(self):
        # self.reset_proxies()
        self.driver.quit()
        self.driver = get_selenium_driver(proxies=self.proxies)