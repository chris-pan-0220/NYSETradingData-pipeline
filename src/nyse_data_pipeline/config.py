import yaml
import logging
import os

class Config:
    _instance = None

    def __new__(cls,
                config_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                                           'config.yaml')): 
        if not cls._instance:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._config_file = config_file
            cls._instance._config = cls._instance.load_config()
        return cls._instance

    def load_config(self):
        try:
            with open(self._config_file, 'r') as config_file:
                config = yaml.safe_load(config_file)
            return config
        except Exception as e:
            error_message = f"Error loading configuration file `{self._config_file}`: {str(e)}"
            logging.error(error_message)
            print(error_message)
            raise Exception(error_message)

    def get_config(self):
        return self._config

if __name__ == '__main__':
    CONFIG = Config()
    config = CONFIG.get_config()
    print(config)
