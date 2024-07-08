# utils.py
from datetime import datetime
import os

import paramiko
from nyse_data_pipeline.exception import *

class FileInfo:
    def __init__(self,
                 name: str,
                 size: int) -> None:
        self.name = name 
        self.size = size
    
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

class Utils:
    @staticmethod
    def validate_date(year: str, month: str) -> bool:
        """
        Validate the year and month values.
        """
        try:
            year = int(year)
            month = int(month)
        except ValueError:
            return False  # Year or month cannot be converted to integers
        
        now = datetime.now()
        now_year = now.year
        now_month = now.month
        
        if not (2013 <= year <= now_year):
            return False  # Year is out of range
        
        if not (1 <= month <= 12):
            return False  # Month is out of range
        
        if year == now_year and month > now_month:
            return False  # Future month in the current year
        
        return True

    @staticmethod
    def convert(byte: int, duration: int = None):
        """
        Compute file size `byte` and download rate `byte`/`duration` to proper unit.
        
        Including `B, KB, MB, GB`
        """
        size_units = ['B', 'KB', 'MB', 'GB']
        rate_units = ['B', 'KB', 'MB', 'GB']

        size = byte
        size_unit = size_units[0]

        if byte >= 1024:
            for unit in size_units[1:]:
                size /= 1024
                if size < 1024:
                    size_unit = unit
                    break

        if duration:
            rate = byte / duration
            rate_unit = rate_units[0]

            if rate >= 1024:
                for unit in rate_units[1:]:
                    rate /= 1024
                    if rate < 1024:
                        rate_unit = unit
                        break

            hours, remainder = divmod(duration, 3600)
            minutes, seconds = divmod(remainder, 60)
            format_time = f"{int(hours):02d}:{int(minutes):02d}:{int(seconds):02d}"

            return size, size_unit, rate, rate_unit, format_time
        else:
            return size, size_unit
    
    @staticmethod
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

    @staticmethod
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

    @staticmethod
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