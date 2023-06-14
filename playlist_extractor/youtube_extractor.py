from __future__ import unicode_literals
import youtube_dl
import pandas as pd
import numpy as np
import glob
import os
from bs4 import BeautifulSoup
import requests
import re

class YoutubeExtractor:

    def __init__(self, playlist_url):
        self.playlist_url = playlist_url
        self.data = []
        self.failure_count = 0 
    
    def download_get_dicts(self):
        self.downoad_playlist_info()
        return [YoutubeParser(video_info).info() for video_info in self.data]
    
    def playlist_start(self):
        try:
            return self.data[-1]['playlist_index'] + 1
        except:
            print("Play start error")
            self.failure_count += 1
            return 1   
        
    def playlist_over_last_index(self):
        try:
            return list(filter(lambda x: 'n_entries' in x.keys(), self.data))[0]['n_entries']
        except:
            print("over list index error")
            return 5
        
    def is_a_playlist(self):
        try:
            return self.failure_count < 5
        except:
            return True
        
    def downoad_playlist_info(self):
        while (self.playlist_start() < self.playlist_over_last_index()) and self.is_a_playlist():
            self.download_video_info(ydl_opts=self.ydl_opts())
    
    def ydl_opts(self):
        return {'skip_download': True, 'match_filter': self.add_to_df, "playliststart": self.playlist_start()}
    
    def add_to_df(self, info_dict):
        self.data.append(info_dict)
        return None
    
    def download_video_info(self, ydl_opts={'skip_download': True}):
        with youtube_dl.YoutubeDL(ydl_opts) as ydl:
            try:
                ydl.download([self.playlist_url])
            except:
                self.data.append({'playlist_index': self.playlist_start()})
                print("there was an exception")

class YoutubeParser:

    def __init__(self, video_info):
        self.video_info = video_info
        
    def info(self):
        return {"duration": self.duration(), "title": self.title(), "description": self.description(),
                "playlist_url": self.playlist_url(),"subitiles": self.subtitles(), "fps": self.fps(), "resolution": self.resolution(), "url": self.url()}
    
    def duration(self):
        try:
            return self.video_info['duration'] / 60
        except:
            return None
    
    def resolution(self):
        try:
            return "{} x {}".format(self.video_info["width"], self.video_info["height"] )
        except:
            return None
    
    def title(self):
        return self.video_info.get('fulltitle')
    
    def description(self):
        return self.video_info.get('description')
    
    def fps(self):
        return self.video_info.get('fps')
        
    def url(self):
        return self.video_info.get('webpage_url')
        
    def playlist_url(self):
        try:
            return "https://www.youtube.com/watch?list={}".format(self.video_info['playlist_id'])
        except:
            return None
        
    def subtitles(self):
        try:
            return ",".join(list(sampe_data['subtitles'].keys()))
        except:
            return list()