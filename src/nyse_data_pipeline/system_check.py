import yaml
import os
import logging
import redis

from nyse_data_pipeline.exception import (
    SFTPConnectError
)
from nyse_data_pipeline.download import connect_to_sftp

logging.basicConfig(filename=f'system.log',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    encoding='utf8')

def check_sftp_config(sftp_config: dict):
    required_keys = ['user', 'host', 'port', 'key_file', 'max_retry']
    logging.debug("check_sftp_config; {!r}".format(required_keys))

    for key in required_keys:
        if key not in sftp_config:
            raise ValueError(f"Missing key '{key}' in SFTP configuration.")
        value = sftp_config[key]
        if value is None:
            raise ValueError(f"Missing key '{key}' in SFTP configuration.")
    

    logging.debug("check_sftp_config; all configuration is set")

def check_redis_config(redis_config: dict):
    required_keys = ['host', 'port', 'db']
    logging.debug("check_redis_config; {!r}".format(required_keys))

    for key in required_keys:
        if key not in redis_config:
            raise ValueError(f"Missing key '{key}' in Redis configuration.")
        value = redis_config[key]
        if value is None:
            raise ValueError(f"Missing key '{key}' in Redis configuration.")
        
    logging.debug("check_redis_config; all configuration is set")

def check_download_config(download_config: dict):

    required_keys = ['safe_to', 'only', 'ignore']
    logging.debug("check_download_config; {!r}".format(required_keys))

    for key in required_keys:
        if key not in download_config:
            raise ValueError(f"Missing key '{key}' in Download configuration.")
        value = download_config[key]
        if value is None:
            raise ValueError(f"Missing key '{key}' in SFTP configuration.")
        
    # check if download path exists
    local_dir_base = download_config['safe_to']
    dir_list = download_config['only'] # TODO: implement igonre
    for dir_name in dir_list:
        if not os.path.exists(local_dir_base[dir_name]):
            raise FileNotFoundError(f"Safe to path '{local_dir_base[dir_name]}' does not exist.")
        
    logging.debug("check_download_config; all configuration is set")

def check_config(config: dict):

    required_keys = ['sftp','redis', 'download'] 

    logging.debug("check_config; {!r}".format(required_keys))

    for key in required_keys:
        if key not in config.keys():
            raise ValueError(f"Missing key '{key}' in configuration.")

    # check sftp config
    sftp_config = config.get('sftp')
    if not sftp_config:
        raise ValueError("SFTP configuration is missing or incomplete.")
    check_sftp_config(sftp_config)

    # check redis config
    redis_config = config.get('redis')
    if not redis_config:
        raise ValueError("Redis configuration is missing or incomplete.")
    check_redis_config(redis_config)

    # check download config
    download_config = config.get('download')
    if not download_config:
        raise ValueError("Download configuration is missing or incomplete.")
    check_download_config(download_config)

    logging.debug("check_config; all configuration is set")

def health_check_sftp(sftp_config):

    logging.debug("health_check_sftp; try to connect to sftp: ")

    account = f"{sftp_config['user']}@{sftp_config['host']}"
    client, sftp = None, None
    try:
        client, sftp = connect_to_sftp()
        logging.debug(f"Remote sftp server: {account} is reachable")
    except SFTPConnectError:
        logging.exception(f"Remote sftp server: {account} is unreachable;")
        raise
    except Exception:
        logging.exception(f"Remote sftp server: {account} is unreachable; unknown error")
        raise
    finally:
        if sftp:
            sftp.close()
        if client:
            client.close()

    logging.debug("health_check_sftp; successfully connect to sftp: ")
        
def health_check_redis(redis_config):

    logging.debug("health_check_redis; try to connect to redis: ")

    try:
        r = redis.Redis(host=redis_config['host'], port=redis_config['port'], db=redis_config['db'])
        r.ping()
        logging.debug(f"Redis server host={redis_config['host']}, port={redis_config['port']}, db={redis_config['db']} is reachable.")
    except Exception:
        logging.exception(f"Redis server host={redis_config['host']}, port={redis_config['port']}, db={redis_config['db']} is unreachable.")
        raise
    finally:
        if r:
            r.close()

    logging.debug("health_check_redis; successfully connect to redis")

def health_check_disk(download_config):
    local_dir_base = download_config['safe_to']
    dir_list = download_config['only'] # TODO: implement igonre
    create_dir = download_config['create_dir']

    logging.debug("health_check_disk; check all local directory: ")

    for dir_name in dir_list:
        dir_path = local_dir_base[dir_name]
        if os.path.exists(dir_path):
            logging.debug(f"{dir_name}: {dir_path} exist")
        else:
            if not create_dir:
                raise Exception(f"{dir_name}: {dir_path} do not exist")
            try:
                os.makedirs(dir_path)
            except Exception:
                logging.exception(f"{dir_name}: {dir_path} do not exist; make directories fail")
                raise

    logging.debug("health_check_disk; all local directory is checked")

def health_check(config: dict):

    logging.debug("health_check; sftp, redis, local disk")

    sftp_config = config['sftp']
    redis_config = config['redis']
    download_config = config['download']

    health_check_sftp(sftp_config)
    health_check_redis(redis_config)
    health_check_disk(download_config)

    logging.debug("health_check; all component is ok")
           
# https://blog.csdn.net/SunJW_2017/article/details/120241354

if __name__ == '__main__':
    # load config
    with open('./config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)

    # check config
    check_config(config)
    print("Configuration is valid.")

    health_check(config)
    print("Health check pass.")
