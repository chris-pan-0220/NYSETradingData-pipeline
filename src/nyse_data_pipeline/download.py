import paramiko
import time
import os
import sys
import logging

# exception handling
from nyse_data_pipeline.exception import (
    CollectDownloadTaskError,
    DownloadTaskError
)

from nyse_data_pipeline.config import Config
from nyse_data_pipeline.utils import Utils, DownloadInfo
from nyse_data_pipeline.logger import Logger

logger = Logger().get_logger()

def download_by_paramiko(sftp: paramiko.SFTPClient, download_info: DownloadInfo):
    """
    Use paramiko download a remote file
    """
    remote_dir = download_info.remote_dir
    local_dir = download_info.local_dir
    file_name = download_info.file_name

    remote_file_path = f'{remote_dir}/{file_name}'
    local_file_path = f'{local_dir}\\{file_name}'
    try:
        sftp.get(remote_file_path, local_file_path)

        if os.stat(local_file_path).st_size != download_info.size:
            raise Exception("The size of the downloaded file (local_file_path) does not match the expected size (download_info.size).")

        return True
    except Exception as e:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        raise DownloadTaskError(f"Error downloading file {download_info.file_name}") from e

# TODO: add param `start_day`, `end_day`
"""
test senario:
1. 日期輸入不合法觸發assert Utils.validate_date 
2. only (list)是空的, 預期回傳download_task_list = []
3. dir_name不存在, 所以產生的remote_dir路徑不存在, list_dir_remote_with_sizes失敗, 收到ListRemoteDirError
4. local_dir_base[dir_name]錯誤, 因為local_dir_base沒有dir_name這個key (dir_name不存在)
5. local_dir_base[dir_name]路徑不存在, 所以執行list_dir_local_with_sizes失敗, 收到ListLocalDirError
6. list_local的資料跟list_remote的資料一樣 => 沒有蒐集到任何task
7. list_remote的資料, 有部分沒出現在list_local => 有蒐集到task
8. list_remote的資料有部分跟list_local的資料一樣, 但是size不同 => 有蒐集到task
"""
def collect_task(start_year: int,
                 start_month: int,
                 end_year: int,
                 end_month: int,
                 sftp: paramiko.SFTPClient,
                 create_dir: bool,
                 safe_to: dict[str, str],
                 only: list[str]) -> list[DownloadInfo]:
    
    local_dir_base = safe_to
    dir_list = only
    download_task_list = []

    assert Utils.validate_date(str(start_year), str(start_month)), "Invalid start year or month"
    assert Utils.validate_date(str(end_year), str(end_month)), "Invalid end year or month"
    assert (start_year < end_year) or (start_year == end_year and start_month <= end_month), "Invalid date range"

    for dir_name in dir_list:
        for year in range(start_year, end_year + 1):
            year_str = str(year)
            for month in range(1, 13):
                month_str = str(month).zfill(2)  # Zero-padding for single-digit months
                if (year == start_year and month < start_month) or \
                    (year == end_year and month > end_month):
                    continue

                remote_dir = os.path.join(dir_name, f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')
                remote_dir = remote_dir.replace('\\', '/')

                # TODO: 
                # Now local dir path is "L:\2024\EQY_US_ALL_TRADE_ADMIN_2024",
                # can we change to "L:\EQY_US_ALL_TRADE_ADMIN\EQY_US_ALL_TRADE_ADMIN_2024" ?
                local_dir = os.path.join(local_dir_base[dir_name], f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')

                if not os.path.exists(local_dir_base[dir_name]): 
                    raise Exception(f"local_dir_base {local_dir_base[dir_name]} does not exist")
                if not os.path.exists(local_dir) and not create_dir:
                    raise Exception(f"local_dir {local_dir} does not exist and create_dir is {create_dir}")
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)

                try: 
                    local_files = Utils.list_dir_local_with_sizes(local_dir)
                    remote_files = Utils.list_dir_remote_with_sizes(sftp, remote_dir)
                    
                    download_tasks = []
                    for remote_file in remote_files:
                        if remote_file.name not in (local_file.name for local_file in local_files):
                            task = DownloadInfo(remote_dir=remote_dir,
                                                    local_dir=local_dir,
                                                    file_name=remote_file.name,
                                                    size=remote_file.size)
                            download_tasks.append(task)
                            continue
                        for local_file in local_files:
                            if remote_file.name == local_file.name and remote_file.size != local_file.size:
                                task = DownloadInfo(remote_dir=remote_dir,
                                                    local_dir=local_dir,
                                                    file_name=remote_file.name,
                                                    size=remote_file.size)
                                download_tasks.append(task)
                                os.remove(f"{task.local_dir}\\{task.file_name}")
                                break

                    download_task_list.extend(download_tasks)

                except Exception as e:
                    raise CollectDownloadTaskError(f"Error collecting tasks for {remote_dir}") from e

    return download_task_list

# TODO: check if it's useful
def validate_download_config(download_config: dict):
    create_dir = download_config.get('create_dir', True)
    safe_to = download_config.get('safe_to', {})
    only = download_config.get('only', [])

    if not isinstance(create_dir, bool):
        raise ValueError("create_dir must be a boolean value")

    if not isinstance(safe_to, dict):
        raise ValueError("safe_to must be a dictionary")

    if not isinstance(only, list):
        raise ValueError("only must be a list")

    for dir_name, local_dir in safe_to.items():
        if not os.path.exists(local_dir):
            if not create_dir:
                raise Exception(f"local_dir {local_dir} does not exist and create_dir is set to False")
            else:
                os.makedirs(local_dir)
        elif not os.path.isdir(local_dir):
            raise Exception(f"{local_dir} is not a directory")
        
    for item in only:
        if item not in safe_to.keys():
            raise Exception(f"Directory '{item}' in 'only' does not exist in 'safe_to' keys")

def proc_download():
    CONFIG = Config()
    config = CONFIG.get_config() 
    download_config = config['download']
    sftp_config = config['sftp']

    start_year = download_config['start_year']
    start_month = download_config['start_month']
    end_year = download_config['end_year']
    end_month = download_config['end_month']

    client, sftp = None, None

    # get logger
    logger = logging.getLogger('proc_download')
    logger.setLevel(logging.INFO)
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    # to stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(logging.INFO)
    # to file
    file_handler = logging.FileHandler('download.log')
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    # add handler
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    validate_download_config(download_config=download_config)
    logger.info("Download configuration is valid")

    # TODO: save download tasks and resume next time. Use redis.
    
    try:
        client, sftp = Utils.connect_to_sftp(user=sftp_config['user'],
                                       host=sftp_config['host'],
                                       port=sftp_config['port'],
                                       key_file=sftp_config['key_file'],
                                       max_retry=sftp_config['max_retry'])

        logger.info(f'Collect download tasks. Start from {start_year}/{start_month} to {end_year}/{end_month}')
        task_list = collect_task(start_year=start_year,
                    start_month=start_month,
                    end_year=end_year,
                    end_month=end_month,
                    sftp=sftp,
                    create_dir=download_config['create_dir'],
                    safe_to=download_config['safe_to'],
                    only=download_config['only'])

        if len(task_list) == 0:
            logger.info("Collect 0 tasks. Exit")
            return
                
        if len(task_list) != 0:
            total_size = sum(task.size for task in task_list)
            size, size_unit = Utils.convert(total_size)
            logger.info(f"Collect {len(task_list)} tasks. Total size: {size:.2f} {size_unit}")
            for task in task_list:
                size, size_unit = Utils.convert(task.size)
                logger.info(f"Download file info: {task.file_name}, {size:.2f} {size_unit}")

        for download_info in task_list:
            logger.info(f'Download file {download_info.file_name}')
            st = time.time()
            download_by_paramiko(sftp=sftp,
                                 download_info=download_info)
            # it will raise error if download fail
            end = time.time()

            # reconnect to sftp server, prevent stuck in pipe
            if sftp:
                sftp.close()
            if client:
                client.close()
            client, sftp = Utils.connect_to_sftp(user=sftp_config['user'],
                                host=sftp_config['host'],
                                port=sftp_config['port'],
                                key_file=sftp_config['key_file'],
                                max_retry=sftp_config['max_retry'])

            duration = 1 if int(end - st) == 0 else int(end - st)
            byte = download_info.size
            size, size_unit, rate, rate_unit, format_time = Utils.convert(byte, duration)
            download_msg = f'size = {size:.2f} {size_unit}, time = {format_time}, rate = {rate:.2f}{rate_unit}/s'
            logger.info(f'Download info: {download_msg}')
            
            # download success
            file_name = download_info.file_name
            if '.done' in file_name:
                continue

        logger.info("Download Complete !")
    except Exception:
        logger.error("Error in download procedure", exc_info=True)
        raise
    finally:
        if sftp:
            sftp.close()
        if client: 
            client.close()

if __name__ == "__main__":
    proc_download()