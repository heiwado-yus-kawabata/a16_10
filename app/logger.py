from google.cloud import logging
import traceback

# debug メモリ調査
import psutil

class Logger:
    def __init__(self, log_name):
        self.log_name = log_name
        self.log_client = logging.Client()
        self.log = self.log_client.logger(name=log_name)

    def logger(self, text, severity):
        text = f"[{self.log_name}]" + text
        self.log.log_text(text, severity=severity)

    def info(self, text):
        self.logger(text=text, severity="INFO")

    def error(self, text, exception=None):
        if exception:
            text += "\nException details: " + str(exception) + traceback.format_exc()
        self.logger(text=text, severity="ERROR")

    def warning(self, text):
        self.logger(text=text, severity="WARNING")

    def mem_usage(self, index):
        proc = psutil.Process()
        info = proc.memory_info()
        self.logger(text=f"###{index} Memory usage: {info.rss} bytes", severity="DEBUG")
