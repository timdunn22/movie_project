from common_methods import *

class AddDubbingInfo:
    
    def __init__(self, video_path, model=None, trim_multiplier=1, audio_path="/Users/timdunn/converted.wav"):
        self.video_path = video_path
        self.model = model
        self.trim_multiplier = trim_multiplier
        self.audio = None
        if model is None:
            self.model = whisper.load_model("base")
        self.converted_audio_path = audio_path
        
    def extract_video_clip(self):
        return mp.VideoFileClip(self.video_path)
    
    def convert_audio(self):
        self.convert_audio_to_file()
        self.write_middle_audio()
        self.load_speech()
        self.trim_audio()
        
    def convert_audio_to_file(self):
        self.extract_video_clip().audio.write_audiofile(self.converted_audio_path)
        
    def trim_audio(self):
        self.audio = whisper.pad_or_trim(self.audio, length=int(self.trim_multiplier * 480000))
        
    def load_speech(self):
        self.audio = whisper.load_audio(self.converted_audio_path)
        
    def write_middle_audio(self):
        r = sr.Recognizer()
        audio = sr.AudioFile(self.converted_audio_path)
        with audio as source:
            audio_file = r.record(source, offset=300, duration=300)
        wav_bytes = audio_file.get_wav_data(convert_rate=16000)
        bytesio_object = io.BytesIO(wav_bytes)
        self.converted_audio_path = "{}/shortened_converted.wav".format("/".join(self.converted_audio_path.split("/")[0:-1]))
        with open(self.converted_audio_path, "wb") as f:
            f.write(bytesio_object.getbuffer())
    
    def load_and_detect_audio(self):
        self.convert_audio()
        mel = whisper.log_mel_spectrogram(self.audio).to(self.model.device)
        _, probs = self.model.detect_language(mel)
        language = max(probs, key=probs.get)
        return {'language': language, 'probability': probs.get(language)}