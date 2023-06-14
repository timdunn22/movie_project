from all_processes.load_yaml_vars import LoadYamlVars
from all_processes.update_movie_data import UpdateMovieData
from all_processes.upload_link_data import UploadLinkData


class ConvertDataForImport(LoadYamlVars):

    def __init__(self, configuration_file):
        super().__init__(configuration_file)
        self.configuration_file = configuration_file

    def convert_imdb_data(self):
        UpdateMovieData(imdb_directory=self.imdb_directory).get_latest_imdb()

    def convert_link_data(self):
        UploadLinkData(configuration_file=self.configuration_file).set_all_link_data()

    def convert_all_data(self):
        self.convert_imdb_data()
        self.convert_link_data()
