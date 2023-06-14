from common_methods import *


class UpdateLinkData:

    def __init__(self, everything_links_path, merged_links_path):
        self.everything_links_path = everything_links_path
        self.merged_links_path = merged_links_path

    def convert_link_data(self):
        self.convert_merged_links()

    def merged_links_updated(self):
        return file_updated_time(self.merged_links_path) > file_updated_time(self.everything_links_path)

    def convert_merged_links(self):
        if not self.merged_links_updated():
            self.convert_merged_links()

