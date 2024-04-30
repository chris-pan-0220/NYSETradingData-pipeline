import pytest
import json
from unittest.mock import MagicMock, patch, call
from nyse_data_pipeline.download import proc_download, DownloadInfo, download_info_to_dict
from nyse_data_pipeline.exception import (
    DownloadTaskError
)

def test_proc_download_empty_task_list():
    # 模擬測試數據
    sftp_mock = MagicMock()
    client_mock = MagicMock()
    redis_client_mock = MagicMock()
    session_mock = MagicMock()
    engine_mock = MagicMock()
    with patch('nyse_data_pipeline.download.connect_to_redis', return_value=redis_client_mock), \
         patch('nyse_data_pipeline.download.connect_to_mysql_by_orm', return_value=engine_mock), \
         patch('nyse_data_pipeline.download.connect_to_sftp', return_value=(client_mock, sftp_mock)), \
         patch('nyse_data_pipeline.download.collect_task', return_value=[]), \
         patch('nyse_data_pipeline.download.datetime') as mock_datetime:
        mock_datetime.now().year = 2024
        mock_datetime.now().month = 2

        # 執行被測試的函式
        proc_download()

        # 驗證
        
        assert client_mock.close.called
        assert sftp_mock.close.called

        assert redis_client_mock.close.not_called

        assert session_mock.close.not_called
        assert engine_mock.dispose.not_called

        assert redis_client_mock.rpush.not_called

def test_proc_download_download_error():
    # 模擬測試數據
    sftp_mock = MagicMock()
    client_mock = MagicMock()
    redis_client_mock = MagicMock()
    session_mock = MagicMock()
    engine_mock = MagicMock()
    task_list = [
        DownloadInfo(remote_dir='remote_dir1', local_dir='local_dir1', file_name='file1.txt', size=100),
        DownloadInfo(remote_dir='remote_dir2', local_dir='local_dir2', file_name='file2.txt', size=200)
    ]
    with patch('nyse_data_pipeline.download.connect_to_redis', return_value=redis_client_mock), \
         patch('nyse_data_pipeline.download.connect_to_mysql_by_orm', return_value=engine_mock), \
         patch('nyse_data_pipeline.download.connect_to_sftp', return_value=(client_mock, sftp_mock)), \
         patch('nyse_data_pipeline.download.collect_task', return_value=task_list), \
         patch('nyse_data_pipeline.download.download_by_paramiko', side_effect=DownloadTaskError):
    
        # 模擬 collect_task 返回一些 task
        task_list = [
            DownloadInfo(remote_dir='remote_dir1', local_dir='local_dir1', file_name='file1.txt', size=100),
            DownloadInfo(remote_dir='remote_dir2', local_dir='local_dir2', file_name='file2.txt', size=200)
        ]

        json_task_list = [json.dumps(download_info_to_dict(t)) for t in task_list]

        # 模擬 lindex 方法返回 MagicMock 對象
        redis_client_mock.lindex.return_value = json_task_list[0]

        # 測試函式
        with pytest.raises(DownloadTaskError):
            proc_download()

        # 驗證相應的函式是否被正確調用
        assert redis_client_mock.rpush.call_args_list == [
            call('download_task', json_task_list[0], json_task_list[1])  # 驗證 task_list 是否被正確傳遞
        ]
        # assert redis_client_mock.rpush.assert_called_once()
        assert redis_client_mock.llen.call_args_list == [call('download_task')]
        assert redis_client_mock.lindex.call_args_list == [call('download_task', 0)]
        assert redis_client_mock.lpop.call_count == 0

        assert client_mock.close.called
        assert sftp_mock.close.called

        assert redis_client_mock.close.called

        assert session_mock.close.not_called
        assert engine_mock.dispose.not_called

def test_proc_download_file_done():
    # 模擬測試數據
    sftp_mock = MagicMock()
    redis_client_mock = MagicMock()
    client_mock = MagicMock()
    session_mock = MagicMock()
    engine_mock = MagicMock()
    task_list = [
        DownloadInfo(remote_dir='remote_dir1', local_dir='local_dir1', file_name='file.done', size=10)
    ]
    json_task_list = [json.dumps(download_info_to_dict(t)) for t in task_list]    
    with patch('nyse_data_pipeline.download.connect_to_redis', return_value=redis_client_mock), \
         patch('nyse_data_pipeline.download.connect_to_mysql_by_orm', return_value=engine_mock), \
         patch('nyse_data_pipeline.download.connect_to_sftp', return_value=(client_mock, sftp_mock)), \
         patch('nyse_data_pipeline.download.collect_task', return_value=task_list), \
         patch('nyse_data_pipeline.download.download_by_paramiko'):
        
        # 模擬 lindex 方法返回 MagicMock 對象
        redis_client_mock.lindex.return_value = json_task_list[0]
        # 執行被測試的函式
        proc_download()
        

        # 驗證相應的函式是否被正確調用
        assert redis_client_mock.rpush.call_args_list == [
            call('download_task', json_task_list[0])  # 驗證 task_list 是否被正確傳遞
        ]
        # assert redis_client_mock.rpush.assert_called_once()
        assert redis_client_mock.llen.call_args_list == [call('download_task')]
        assert redis_client_mock.lindex.call_args_list == [call('download_task', 0)]
        assert redis_client_mock.lpop.call_count == 1

        assert client_mock.close.called
        assert sftp_mock.close.called

        assert redis_client_mock.close.called

        assert session_mock.close.not_called
        assert engine_mock.dispose.not_called



if __name__ == "__main__":
    pytest.main([__file__, '-v'])