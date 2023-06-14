from all_processes.upload_link_data import UploadLinkData
from all_processes.process_imdb_data import ProcessImdbData
from all_processes.convert_single_column_data import ConvertSingleColumns
import os


class ConvertAllData:

    def __init__(self, configuration_file, force_update=False):
        self.configuration_file = configuration_file
        self.force_update = force_update

    def convert_data(self):
        self.convert_imdb_data()
        self.convert_link_data()
        self.convert_single_column_data()

    def convert_imdb_data(self):
        ProcessImdbData(configuration_file=self.configuration_file, force_update=self.force_update).get_convert_all_imdb_data()

    def convert_link_data(self):
        UploadLinkData(configuration_file=self.configuration_file).set_all_link_data()

    def convert_single_column_data(self):
        ConvertSingleColumns(configuration_file=self.configuration_file).convert_all_data()

def main():
    ConvertAllData(os.environ.get('YAML_FILE', './movie_configuration.yaml'), force_update=True).convert_data()

if __name__ == '__main__':
    main()
