import logging
import sys

# 定义自定义的过滤器类
class FileSetFilter(logging.Filter):
    def __init__(self, allowed_files):
        super().__init__()
        self.allowed_files = allowed_files

    def filter(self, record):
        # 只允许指定的文件名记录日志
        return record.filename in self.allowed_files

class Logger:
    _instance = None

    def __new__(cls, log_file='download.log'):
        if not cls._instance:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._log_file = log_file
            cls._instance._logger = cls._instance._set_logger()
        return cls._instance
    
    def _get_allow_files(self):
        pass

    def _set_logger(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

        # 创建流处理程序（输出到控制台）
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)

        # 创建文件处理程序（输出到文件）
        file_handler = logging.FileHandler(self._log_file)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)

        # 添加处理程序
        logger.addHandler(stream_handler)
        logger.addHandler(file_handler)

        # 添加过滤器到文件处理程序
        # allowed_files = ["download.py", "system_check.py", "utils.py", "config.py", "exception.py", "logger.py"]
        # file_filter = FileSetFilter(allowed_files)
        # file_handler.addFilter(file_filter)
        # stream_handler.addFilter(file_filter)

        return logger

    def get_logger(self) -> logging.Logger:
        return self._logger

# 使用示例
if __name__ == "__main__":
    # logger = MyLogger("multiple_log.log", ['log1_t.py']).getLogger()
    # logger.info("This is a test message")
    logger = Logger().get_logger()
    logger.info("This is a wrap singleton logger")
