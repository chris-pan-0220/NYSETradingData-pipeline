import paramiko
import time
import os
import redis
import json
from datetime import datetime
from sqlalchemy.orm import sessionmaker

# exception handling
from paramiko.ssh_exception import (
    NoValidConnectionsError
)
from nyse_data_pipeline.exception import (
    SFTPConnectError,
    SFTPConnectMaxRetryError,
    SFTPconnectNoValidError,
    ListDirLocalError,
    ListDirRemoteError,
    CollectDownloadTaskError,
    DownloadTaskError
)
import socket

from nyse_data_pipeline.config import Config
from nyse_data_pipeline.utils import Utils
from nyse_data_pipeline.logger import Logger
from nyse_data_pipeline.analysis import connect_to_mysql_by_orm
from nyse_data_pipeline.schema import Task, TaskCategory

# TODO: 移動到main procedure，使用統一的log
# TODO: 移動到config.yaml
# t_time = datetime.now().strftime('%Y-%m-%d %H%M%S')
# logging.basicConfig(filename=f'log\\download {t_time}.log',
#                     level=logging.INFO,
#                     format='%(asctime)s - %(levelname)s - %(message)s',
#                     encoding='utf8')

logger = Logger().get_logger()

# TODO: logging誰要記錄 ???
# info越裡面的function優先紀錄
# exception raise越外層優先紀錄

# TODO: test可能不需要?
def connect_to_sftp(user, host, port: int, key_file: str, max_retry: int):
    retry = 0
    while True:
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            # TODO: exception
            client.connect(hostname=host,
                           port=port,
                           username=user,
                           key_filename=key_file)
            transport = client.get_transport()
            sftp = paramiko.SFTPClient.from_transport(transport)

            # server must support sftp 
            assert(sftp is not None)

            return client, sftp
        except NoValidConnectionsError as e:
            raise SFTPconnectNoValidError from e
        except socket.error as e:
            if retry == max_retry:
                raise SFTPConnectMaxRetryError(max_retry) from e
            retry += 1
        except Exception as e:
            raise SFTPConnectError("unknown error") from e

# TODO: test可能不需要?
def connect_to_redis(host, port, db):
    redis_client = redis.Redis(host=host, port=port, db=db)
    return redis_client
       
class FileInfo:
    def __init__(self,
                 name: str,
                 size: int) -> None:
        self.name = name 
        self.size = size
    
def list_dir_remote_with_sizes(sftp: paramiko.SFTPClient, remote_dir: str) -> list[FileInfo]:
    """
        List files in the remote sftp server directory,
        
        and return file names and file sizes

        EX: SPLITS_US_ALL_BBO/SPLITS_US_ALL_BBO_2024/SPLITS_US_ALL_BBO_202401
    """
    try:
        file_names = sftp.listdir(remote_dir)
        files = []
        for file_name in file_names:
            file_size = sftp.stat(f'{remote_dir}/{file_name}').st_size
            files.append(FileInfo(name=file_name,
                                  size=file_size))
        return files
    except IOError as e:
        raise ListDirRemoteError("Error listing sftp server directory, maybe no such file, permission denied, or other errors") from e
    except Exception as e:
        raise ListDirRemoteError("Error listing sftp server directory") from e

def list_dir_local_with_sizes(local_dir: str) -> list[FileInfo]:
    """
        List files in the local directory along with their sizes.
        
        Args:
        - local_dir (str): The local directory path.
        - EX: e:\SPLITS_US_ALL_BBO\SPLITS_US_ALL_BBO_2024\SPLITS_US_ALL_BBO_202401

        Returns:
        - list: A list of dictionaries containing file names and sizes.
    """
    try:
        # List file names and sizes
        file_names = os.listdir(local_dir)
        files = []

        for file_name in file_names:
            file_path = os.path.join(local_dir, file_name)
            file_size = os.path.getsize(file_path)
            files.append(FileInfo(name=file_name,
                                  size=file_size))
        return files

    except OSError as e:
        # Handle the case where the local directory cannot be accessed
        raise ListDirLocalError("Error list local directory") from e

    except Exception as e:
        # Handle other unexpected errors
        raise ListDirLocalError("Error list local directory, unexpected error occurred") from e
    
class DownloadInfo:
    def __init__(self,
                 remote_dir: str,
                 local_dir: str,
                 file_name: str,
                 size: int) -> None:
        self.remote_dir = remote_dir
        self.local_dir = local_dir 
        self.file_name = file_name 
        self.size = size 

def download_info_to_dict(obj):
    return obj.__dict__

def dict_to_download_info(d):
    return DownloadInfo(d['remote_dir'], d['local_dir'], d['file_name'], d['size'])

def download_by_paramiko(sftp: paramiko.SFTPClient, download_info: DownloadInfo):
    """
    使用paramiko下载一个文件
    """
    remote_dir = download_info.remote_dir
    local_dir = download_info.local_dir
    file_name = download_info.file_name

    remote_file_path = f'{remote_dir}/{file_name}'
    local_file_path = f'{local_dir}\\{file_name}'
    try:
        # st = time.time()
        sftp.get(remote_file_path, local_file_path)
        # end = time.time()

        if os.stat(local_file_path).st_size != download_info.size:
            raise Exception("The size of the downloaded file (local_file_path) does not match the expected size (download_info.size).")

        # duration = 1 if int(end - st) == 0 else int(end - st)
        # byte = download_info.size
        # size, size_unit, rate, rate_unit, format_time = Utils.convert(byte, duration)
        # sftp_download_msg = f'get {file_name}. total {size:.2f} {size_unit}, take {format_time},  {rate:.2f}{rate_unit}/s'
        # logger.info(f'sftp download info: {sftp_download_msg}')

        return True
    except Exception as e:
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        raise DownloadTaskError(f"Error downloading file {download_info.file_name}") from e

# TODO: 以天為單位?
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
def collect_task(start_year: str,
                 start_month: str,
                 end_year: str,
                 end_month: str,
                 sftp: paramiko.SFTPClient,
                 create_dir: bool,
                 safe_to: dict[str, str],
                 only: list[str]) -> list[DownloadInfo]:
    # TODO: 傳入ignore
    
    local_dir_base = safe_to
    dir_list = only
    download_task_list = []

    # TODO: 修改validate_date, 使他能夠檢查天
    assert Utils.validate_date(start_year, start_month), "Invalid start year or month"
    assert Utils.validate_date(end_year, end_month), "Invalid end year or month"
    assert (start_year < end_year) or (start_year == end_year and start_month <= end_month), "Invalid date range"

    for dir_name in dir_list:
        for year in range(int(start_year), int(end_year) + 1):
            year_str = str(year)
            for month in range(1, 13):
                month_str = str(month).zfill(2)  # Zero-padding for single-digit months
                if (year_str == start_year and month < int(start_month)) or \
                    (year_str == end_year and month > int(end_month)):
                    continue

                remote_dir = os.path.join(dir_name, f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')
                remote_dir = remote_dir.replace('\\', '/')

                # TODO: L:\2024\EQY_US_ALL_TRADE_ADMIN_2024
                # 能否改成 L:\EQY_US_ALL_TRADE_ADMIN\EQY_US_ALL_TRADE_ADMIN_2024
                local_dir = os.path.join(local_dir_base[dir_name], f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')
                
                # if local_dir_base[dir_name] == 'SPLITS_US_ALL_BBO':
                #     local_dir = os.path.join(local_dir_base[dir_name], f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')
                # else: 
                # TODO: L:\2024\EQY_US_ALL_TRADE_ADMIN_2024
                # 能否改成 L:\EQY_US_ALL_TRADE_ADMIN\EQY_US_ALL_TRADE_ADMIN_2024
                # local_dir = os.path.join(local_dir_base[dir_name], f'{dir_name}_{year_str}', f'{dir_name}_{year_str}{month_str}')

                # TODO: if create dir ...
                if not os.path.exists(local_dir_base[dir_name]): 
                    raise Exception(f"local_dir_base {local_dir_base[dir_name]} does not exist")
                if not os.path.exists(local_dir) and not create_dir:
                    raise Exception(f"local_dir {local_dir} does not exist and create_dir is {create_dir}")
                if not os.path.exists(local_dir):
                    os.makedirs(local_dir)

                try: 
                    local_files = list_dir_local_with_sizes(local_dir)
                    remote_files = list_dir_remote_with_sizes(sftp, remote_dir)
                    
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
                                t_size, t_size_unit = Utils.convert(task.size)
                                size, size_unit = Utils.convert(local_file.size)
                                logger.warning(f"File {remote_file.name} is incomplete. Correct file size: {t_size:.2f} {t_size_unit} ; now is {size:.2f} {size_unit}, remove to download again")
                                os.remove(f"{task.local_dir}\\{task.file_name}")
                                break

                    download_task_list.extend(download_tasks)

                except Exception as e:
                    raise CollectDownloadTaskError(f"Error collecting tasks for {remote_dir}") from e

    return download_task_list

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
    mysql_config = config['mysql']
    redis_config = config['redis']

    validate_download_config(download_config=download_config)
    logger.info("Download configuration is valid")

    now = datetime.now()
    year = now.year 
    month = now.month

    client, sftp = None, None
    redis_client = None
    engine = None 
    session = None

    # TODO: 從上次還沒下載完成的部分繼續, query redis
    
    try:
        redis_client = connect_to_redis(host=redis_config['host'],
                                        port=redis_config['port'],
                                        db=redis_config['db'])
        client, sftp = connect_to_sftp(user=sftp_config['user'],
                                       host=sftp_config['host'],
                                       port=sftp_config['port'],
                                       key_file=sftp_config['key_file'],
                                       max_retry=sftp_config['max_retry'])
        n = redis_client.llen('download_task')
        logger.info(f'Resume {n} download tasks from redis server.')

        if n == 0:
            logger.info(f'Collect download tasks. Start from {year}/{month} to {year}/{month}')
            task_list = collect_task(start_year=str(year),
                        start_month=str(month),
                        end_year=str(year),
                        end_month=str(month),
                        sftp=sftp,
                        create_dir=download_config['create_dir'],
                        safe_to=download_config['safe_to'],
                        only=download_config['only'])
            
            if len(task_list) == 0:
                logger.info("Successfully collect 0 tasks. Exit")
                return
            
            # 測試 proc_except_bbo, 重新排列task_list, 把20240402的資料排列到最前面
            # test_date = '20240402'
            # a_list = [x for x in task_list if (test_date in x.file_name)] 
            # b_list = [x for x in task_list if (test_date not in x.file_name)] 
            # print(len(a_list))
            # print(len(b_list))
            # a_list.extend(b_list)
            # 改成只放20240402的
            # task_list = a_list
            # print(len(task_list))
            # exit(0)
            # print(f'len a = {len(a_list)}, len task_list = {len(task_list)}')
            # end test

            total_size = sum(task.size for task in task_list)
            size, size_unit = Utils.convert(total_size)
            logger.info(f"Successfully collect {len(task_list)} tasks. Total size: {size:.2f} {size_unit}")
            
            for task in task_list:
                size, size_unit = Utils.convert(task.size)
                logger.info(f"Download file info: {task.file_name}, {size:.2f} {size_unit}")

            task_list = [json.dumps(download_info_to_dict(t)) for t in task_list]
            redis_client.rpush('download_task', *task_list)

        n = redis_client.llen('download_task')

        engine = connect_to_mysql_by_orm(host=mysql_config['host'],
                                user=mysql_config['user'],
                                password=mysql_config['password'],
                                database=mysql_config['database']['prod'])
        Session = sessionmaker(bind=engine)
        session = Session()

        for _ in range(n):
            val = redis_client.lindex('download_task', 0)
            d = json.loads(val)
            download_info = dict_to_download_info(d)
            logger.info(f'Download file {download_info.file_name}')

            st = time.time()
            download_by_paramiko(sftp=sftp,
                                 download_info=download_info)
            end = time.time()

            if sftp:
                sftp.close()
            if client:
                client.close()
            client, sftp = connect_to_sftp(user=sftp_config['user'],
                                host=sftp_config['host'],
                                port=sftp_config['port'],
                                key_file=sftp_config['key_file'],
                                max_retry=sftp_config['max_retry'])

            duration = 1 if int(end - st) == 0 else int(end - st)
            byte = download_info.size
            size, size_unit, rate, rate_unit, format_time = Utils.convert(byte, duration)
            download_msg = f'get {download_info.file_name}. total {size:.2f} {size_unit}, take {format_time},  {rate:.2f}{rate_unit}/s'
            logger.info(f'sftp download info: {download_msg}')
            # download失敗會直接raise error
            
            # download success
            file_name = download_info.file_name
            if '.done' in file_name:
                redis_client.lpop('download_task')
                continue
            
            # TODO: 底下這兩個條件還沒有測試
            if 'SPLITS_US_ALL_BBO' in file_name:
                # 如果不是bbo，alphabet不適用
                date = file_name[-11:-3]
                alphabet = file_name[-13]

                taskCategoryId = session.query(TaskCategory.id) \
                    .filter(TaskCategory.name == 'SPLITS_US_ALL_BBO') \
                    .scalar()
                
                new_task = Task(taskCategoryId=taskCategoryId,
                                filename=file_name,
                                alphabet=alphabet,
                                date=date,
                                groupuuid=None)
                session.add(new_task)
                session.commit()
            else:
                date = file_name[-11:-3]
                category = file_name[:-12]

                if category == 'EQY_US_ALL_ADMIN_CTS' or category == 'EQY_US_ALL_ADMIN_UTP':
                    category = 'EQY_US_ALL_ADMIN'

                taskCategoryId = session.query(TaskCategory.id) \
                    .filter(TaskCategory.name == category) \
                    .scalar()
                
                new_task = Task(taskCategoryId=taskCategoryId,
                                filename=file_name,
                                alphabet=None,
                                date=date,
                                groupuuid=None)
                session.add(new_task)
                session.commit()

            redis_client.lpop('download_task')
    except Exception:
        logger.error("Error in download procedure", exc_info=True)
        raise
    finally:
        if redis_client:
            redis_client.close()  
        if session:
            session.close() 
        if engine: 
            engine.dispose()
        if sftp:
            sftp.close()
        if client: 
            client.close()

# def download_by_paramiko_test1():
#     download_info = {
#         'remote_dir': 'SPLITS_US_ALL_BBO/SPLITS_US_ALL_BBO_2024/SPLITS_US_ALL_BBO_202401',
#         'local_dir': r'C:\Users\chrispan\Desktop\workspace\NYSE',
#         'file_name': 'SPLITS_US_ALL_BBO_Z_20240102.gz',
#         'size': 165252937
#     }
#     result = download_by_paramiko(download_info)
#     print('Result: ', result)

# def download_by_paramiko_test2():
#     download_info = {
#         'remote_dir': 'invalid_dir',
#         'local_dir': r'C:\Users\chrispan\Desktop\workspace\NYSE',
#         'file_name': 'SPLITS_US_ALL_BBO_Z_20240102.gz',
#         'size': 165252937
#     }
#     result = download_by_paramiko(download_info)
#     print('Result: ', result)

# def download_by_paramiko_test3():
#     download_info = {
#         'remote_dir': 'SPLITS_US_ALL_BBO/SPLITS_US_ALL_BBO_2024/SPLITS_US_ALL_BBO_202401',
#         'local_dir': 'invalid_dir',
#         'file_name': 'SPLITS_US_ALL_BBO_Z_20240102.gz',
#         'size': 165252937
#     }
#     result = download_by_paramiko(download_info)
#     print('Result: ', result)

# def list_dir_local_with_sizes_test():
#     try:
#         dir = r'e:\SPLITS_US_ALL_BBO\SPLITS_US_ALL_BBO_2024\SPLITS_US_ALL_BBO_202401xxxx'
#         files = list_dir_local_with_sizes(dir)
#         for f in files:
#             print(f)
#     except Exception:
#         error_message = f"Error list local directory"
#         logging.error(error_message)
#         print(error_message)
#     print('這裡可以繼續執行!')

# def collect_task_test():
#     task_list = collect_task('2024', '02', '2024', '02')
#     for t in task_list:
#         print(t)

if __name__ == "__main__":
    # download_by_paramiko_test1()
    # download_by_paramiko_test2()
    # download_by_paramiko_test3()
    # list_dir_local_with_sizes_test()
    # collect_task_test()
    proc_download()