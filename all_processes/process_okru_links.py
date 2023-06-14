from common_methods import *
from all_processes.add_dubbing_info import *

class ProcessOkruLinks:
    
    def __init__(self, okru_links_file="/Users/timdunn/Desktop/everything_merged.csv", 
                 data_path="/Users/timdunn/okru_extra.csv", instances=5, 
                 dubbed_data_path="/Users/timdunn/okru_dubbed_data.csv", only_tconsts=False):
        self.data_path = data_path
        self.instances = instances
        self.dubbed_data_path = dubbed_data_path
        everything = pd.read_csv(okru_links_file, usecols=['new link url', 'link url'])
        try:
            if len(only_tconsts) > 1:
                everything = pd.read_csv(okru_links_file, usecols=['new link url', 'link url', 'tconst'])
                everything = everything.loc[everything['tconst'].isin(only_tconsts)]
        except:
            everything = pd.read_csv(okru_links_file, usecols=['new link url', 'link url'])
        relevant = everything.loc[everything['new link url'].apply(lambda x: type(x) == str) & everything['link url'].apply(
    lambda x: type(x) == float), 'new link url']
        self.links = np.unique(relevant)
        self.model = whisper.load_model("base")
        self.video_directory = "./"
        self.data = []
        self.dubbed_data = []
        
    def download_okru_video_info(self, video_link):
        ydl_opts = {'skip_download': True, 'match_filter': self.add_to_df}
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            try:
                ydl.download([video_link])
            except:
                print("there was an exception")
                self.data.append({'link': video_link, 'error': "There was an error"})
                
    def video_download_opts(self, link):
        return {'format': 'worst', 'outtmpl': self.get_file_name(link)}
    
    def get_file_name(self, link):
        return "{}.mp4".format(link.split("/")[-1])
    
    def video_path(self, link):
        return "{}{}".format(self.video_directory, self.get_file_name(link))
    
    def download_a_video(self, video_link):
        with youtube_dl.YoutubeDL(self.video_download_opts(video_link)) as ydl:
            ydl.download([video_link])
            
    def update_okru_extra_info(self):
        links = self.data_links()
        for index, link in enumerate(links):
            try:
                self.download_okru_video_info(link)
                if index % 1000 == 0:
                    print('length of data is', len(self.data))
                    self.add_data_to_file()
            except:
                self.add_data_to_file()
        self.add_data_to_file()
    
    def add_data_to_file(self):
        self.data_to_file(self.data, self.data_path)
        
    def dubbed_data_to_file(self):
        self.data_to_file(self.dubbed_data, self.dubbed_data_path)
        
    def data_to_file(self, data, file_path):
        try:
            dfs = [pd.DataFrame(data=data)]
            try:
                dfs.append(pd.read_csv(file_path))
            except:
                pass
            pd.concat(dfs).drop_duplicates().to_csv(file_path, index=False)
        except:
            pass
        
    def add_to_df(self, info_dict):
        self.data.append(self.add_okru_info(info_dict))
        return None
    
    def add_okru_info(self, info_dict):
        movie_info = dict()
        info_dict['description'] = self.get_description(info_dict)
        filtered_keys = self.filter_keys(info_dict)
        for key in filtered_keys:
            key_val = info_dict.get(key)
            set_key(movie_info, key, key_val)
        return movie_info
    
    def filter_keys(self, info_dict):
        return [key for key in info_dict.keys() if not ( key.startswith('format') or key.startswith('http') )]

    def get_description(self, info_dict):
        try:
            url = info_dict.get('webpage_url')
            soup = get_soup_url(url)
            div = soup.find('div', {"class": 'vp-layer-description'})
            return div.text
        except:
            return None

    def add_dubbed_data(self, link):
        temp_data = {'link': link}
        temp_data.update(AddDubbingInfo(self.video_path(link), model=self.model).load_and_detect_audio())
        self.dubbed_data.append(temp_data)
    
    def update_dubbed_video(self, link):
        try:
            self.download_a_video(link)
            self.add_dubbed_data(link)
        except:
            pass
        
    def dubbed_links(self):
        return self.filter_links(self.dubbed_data_path)
        
    def data_links(self):
        return self.filter_links(self.data_path)
        
    def filter_links(self, file_path):
        try:
            data_links = pd.read_csv(file_path)['link']
            return list(filter(lambda link: link not in data_links, self.links))
        except:
            return self.links
        
    def remove_video_file(self, link):
        try:
            os.remove(self.video_path(link))
        except:
            pass
    
    def update_dubbed_data(self):
        links = self.dubbed_links()
        for index, link in enumerate(links):
            self.update_dubbed_video(link)
            if index % 100 == 0:
                print('length of data is', len(self.dubbed_data))
                self.dubbed_data_to_file()
                self.remove_video_file(link)

def main():
    for _ in range(30):
        try:
            ProcessOkruLinks().update_okru_extra_info()
        except:
            print('crazy exception')
    
    for _ in range(30):
        try:
            ProcessOkruLinks().update_dubbed_data()
        except:
            print('crazy exception dubbed')

    

if __name__ == '__main__':
    main()
