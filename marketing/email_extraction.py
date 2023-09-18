import pandas as pd
import pysnooper

import requests
from tld import get_fld
from movie_project.marketing.kftv_scraper import (ThreadedScraperYaml, ThreadManagement,
                                                  UndetectedSeleniumScraper)
from movie_project.requirements import time
from movie_project.common_methods import (re, merge_many_dicts, os, pd, email_regex,
                                          common_null_values, get_selenium_soup, 
                                          emails_from_text, sample)

class EmailThreadedScraper(ThreadedScraperYaml):
    def __init__(self, progress_directory=None, output_file=None, 
                 item_file=None, proxy_file=None,  not_found_item_file=None, 
                 thread_class=ThreadManagement, yaml_file=None, instances=5):
        
        super().__init__(instances=instances, 
                         maketing_key='Email',
                         yaml_file=yaml_file,
                         progress_directory=progress_directory,  
                         output_file=output_file, 
                         item_file=item_file, 
                         proxy_file=proxy_file, 
                         not_found_item_file=not_found_item_file, 
                         thread_class=thread_class, 
                         scraper_class=EmailExtraction)
    
    def get_all_data(self, instance_id, items):
        try:
            super().get_all_data(instance_id, items)
            merged_df = pd.merge(left=pd.read_csv(self.item_file), 
                                right=pd.DataFrame(data=self.data), on='Company Url')
            merged_df.drop_duplicates(inplace=True)
            self.data = [dict(row) for _, row in merged_df.iterrows()]
            return self.data
        except:
            return self.data

class EmailExtraction(UndetectedSeleniumScraper):
    unique_identifier = 'Company Url'
    item_file_col = 'Company Url'
    confirmed_extracted_col = 'Email 1'

    def __init__(self, progress_directory, proxy_file=None, items=None, 
                 instance_id=None, not_found_data_file=None):
        super().__init__(unique_identifier=self.unique_identifier, 
                         item_file_col=self.item_file_col, 
                         not_found_data_file=not_found_data_file,
                         progress_directory=progress_directory, 
                         proxy_file=proxy_file, 
                         items=items, 
                         page_scraper_class=EmailPageScraper,
                         instance_id=instance_id)

    def extract_get_emails(self):
        self.extract_links()
        return self.emails

    def set_link_vars(self, link_file):
        try:
            self.link_df = pd.read_csv(link_file)
            self.links = list(self.link_df[self.unique_identifier])
        except:
            self.link_df = None
            self.links = list()

    @pysnooper.snoop(depth=3, watch=(  'len(links)'))
    def get_nested_emails(self, soup):
        return EmailPageScraper(soup=soup, driver=self.driver).get_nested_emails()

    def is_not_found(self, soup):
        try:
            emails = self.get_nested_emails(soup)
            return len(list(emails)) == 0
        except:
            return True
    
    @pysnooper.snoop(depth=2, watch=('self.data', 'len(self.items)', 
                                     'len(self.not_found_data)', 'len(self.links)'))
    def get_item_data(self, item):
        try:
            current_time = time.time()
            self.go_to_page(item)
            soup, _ = self.get_soup_vars(current_time)
            if self.is_not_found(soup):
                self.save_not_found_items()
                return self.set_not_found_vars(item)
            else:
                return self.company_info(soup, item)
        except:
            pass

    def verify_data(self, soup):
        try:
            verify_value = self.page_scraper_class(soup=soup, 
                                                   driver=self.driver).verify_method()
            return verify_value not in common_null_values()
        except:
            return False

    def company_info(self, soup, item):
        try:
            basic_dict = EmailPageScraper(soup=soup, driver=self.driver).company_info()
            basic_dict['Company Url'] = item
            return basic_dict
        except:
            return None


class EmailPageScraper:

    def __init__(self, soup, driver):
        self.soup = soup
        self.emails = set()
        self.driver = driver

    def get_emails_from_page(self):
        try:
            return emails_from_text(self.driver.page_source)
        except:
            return list()

    def go_to_page(self, link):
        self.driver.get(link)
        time.sleep(3)

    def get_url_link(self, link_tag):
        if 'href' in link_tag.attrs:
            href = link_tag.attrs["href"]
            if href.starts_with('/'):
                return self.url + href
            return href

    def get_links(self):
        try:
            links = set()
            links.update( [self.get_url_link(link_tag) 
                           for link_tag in self.soup.find_all('a') 
                           if self.get_url_link(link_tag)] not in common_null_values())
            return links
        except:
            return list()

    def update_emails(self):
        self.emails.update(self.get_emails_from_page())

    @pysnooper.snoop(depth=1, watch=(  'len(links)'))
    def update_emails_of_links(self):
        links = self.get_links()
        for link in links:
            self.go_to_page(link)
            self.update_emails()

    def get_nested_emails(self):
        try:
            self.update_emails()
            self.update_emails_of_links()
            return list(self.emails)
        except:
            return None

    @pysnooper.snoop(depth=1)
    def get_emails(self):
        try:
            self.get_nested_emails()
            dicts = [{f'Email {index+1}': email } 
                     for index, email in enumerate(self.emails)]
            if len(dicts) > 0:
                return merge_many_dicts(dicts)
            return None
        except:
            return None

    def verify_method(self):
        return self.get_nested_emails()

    @pysnooper.snoop(depth=2)
    def company_info(self):
        try:
            return self.get_emails()
        except:
            return None

# @pysnooper.snoop(depth=6)
def main():
    yaml_file = os.environ.get('YAML_FILE', 
                               './movie_project/all_processes/movie_configuration.yaml')
    EmailThreadedScraper(instances=1, yaml_file=yaml_file).run_instances_and_concat()

if __name__ == '__main__':
    main()