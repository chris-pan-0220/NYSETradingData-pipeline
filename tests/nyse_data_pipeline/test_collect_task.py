import pytest
from unittest.mock import patch, MagicMock 
from unittest import mock
from nyse_data_pipeline.download import collect_task, FileInfo
from nyse_data_pipeline.exception import (
    SFTPConnectMaxRetryError,
    SFTPconnectNoValidError,
    SFTPConnectError)
# from paramiko.ssh_exception import *
import paramiko

@pytest.fixture
def mock_sftp():
    sftp = mock.Mock(spec=paramiko.SFTPClient)
    return sftp

@pytest.fixture
def safe_to():
    return {
        'SPLITS_US_ALL_BBO': r'E:\SPLITS_US_ALL_BBO'
    }

@pytest.fixture
def only():
    return ['SPLITS_US_ALL_BBO']

def test_collect_task(mock_sftp, safe_to, only):
    start_year = '2024'
    start_month = '02'
    end_year = '2024'
    end_month = '02'
    create_dir = False

    # 模擬 list_dir_local_with_sizes 和 list_dir_remote_with_sizes 的行為
    with patch('nyse_data_pipeline.download.list_dir_local_with_sizes') as mock_list_dir_local, \
         patch('nyse_data_pipeline.download.list_dir_remote_with_sizes') as mock_list_dir_remote:
        
        # 模擬 list_dir_local_with_sizes 返回空列表
        mock_list_dir_local.return_value = []
        # 模擬 list_dir_remote_with_sizes 返回一些文件信息
        mock_list_dir_remote.return_value = [
            FileInfo(name='file1.txt', size=100),
            FileInfo(name='file2.txt', size=200)
        ]

        # 呼叫函式進行測試
        download_tasks = collect_task(start_year, start_month, end_year, end_month,
                                      mock_sftp, create_dir, safe_to, only)

        # 確認 collect_task 正確執行
        assert len(download_tasks) == 2
        assert download_tasks[0].file_name == 'file1.txt'
        assert download_tasks[1].file_name == 'file2.txt'
        assert download_tasks[0].size == 100
        assert download_tasks[1].size == 200

        # 確認函式呼叫了相應的方法
        mock_list_dir_local.assert_called()
        mock_list_dir_remote.assert_called()

def test_only_empty():
    sftp_mock = MagicMock()
    create_dir = False
    safe_to = {}
    only = []
    start_year, start_month = '2024', '01'
    end_year, end_month = '2024', '01'
    result = collect_task(start_year, start_month, end_year, end_month,
                            sftp_mock, create_dir, safe_to, only)
    assert result == []

def test_local_remote_identical():
    # 模擬測試數據
    sftp_mock = MagicMock()
    create_dir = False
    safe_to = {'SPLITS_US_ALL_BBO': r'E:\SPLITS_US_ALL_BBO'}
    only = ['SPLITS_US_ALL_BBO']
    start_year, start_month = '2024', '01'
    end_year, end_month = '2024', '01'

    with patch('nyse_data_pipeline.download.list_dir_local_with_sizes') as mock_list_dir_local, \
         patch('nyse_data_pipeline.download.list_dir_remote_with_sizes') as mock_list_dir_remote:
    
        # 模擬 list_dir_local_with_sizes 返回空列表
        mock_list_dir_local.return_value = [
            FileInfo(name='file1.txt', size=100),
            FileInfo(name='file2.txt', size=200)
        ]
        # 模擬 list_dir_remote_with_sizes 返回一些文件信息
        mock_list_dir_remote.return_value = [
            FileInfo(name='file1.txt', size=100),
            FileInfo(name='file2.txt', size=200)
        ]

        # 執行被測試的函式
        result = collect_task(start_year, start_month, end_year, end_month,
                            sftp_mock, create_dir, safe_to, only)
        
        # 驗證結果是否符合預期
        assert len(result) == 0  # 因為本地和遠程文件列表相同，所以不應該蒐集到任何任務

def test_remote_partial_not_in_local():
    # 模擬測試數據
    sftp_mock = MagicMock()
    create_dir = False
    safe_to = {'SPLITS_US_ALL_BBO': r'E:\SPLITS_US_ALL_BBO'}
    only = ['SPLITS_US_ALL_BBO']
    start_year, start_month = '2024', '01'
    end_year, end_month = '2024', '01'

    with patch('nyse_data_pipeline.download.list_dir_local_with_sizes') as mock_list_dir_local, \
         patch('nyse_data_pipeline.download.list_dir_remote_with_sizes') as mock_list_dir_remote:
    
        # 模擬 list_dir_local_with_sizes 返回空列表
        mock_list_dir_local.return_value = [
            FileInfo(name='file2.txt', size=200)
        ]
        # 模擬 list_dir_remote_with_sizes 返回一些文件信息
        mock_list_dir_remote.return_value = [
            FileInfo(name='file1.txt', size=100),
            FileInfo(name='file2.txt', size=200),
            FileInfo(name='file3.txt', size=300)
        ]

        # 執行被測試的函式
        result = collect_task(start_year, start_month, end_year, end_month,
                            sftp_mock, create_dir, safe_to, only)
        
        # 驗證結果是否符合預期
        assert len(result) == 2  # 預期蒐集到兩個任務
        # 驗證 file1.txt 是否在結果中
        assert any(task.file_name == 'file1.txt' for task in result)
        # 驗證 file3.txt 是否在結果中
        assert any(task.file_name == 'file3.txt' for task in result)

def test_remote_partial_size_mismatch():
        # 模擬測試數據
    sftp_mock = MagicMock()
    create_dir = False
    safe_to = {'SPLITS_US_ALL_BBO': r'E:\SPLITS_US_ALL_BBO'}
    only = ['SPLITS_US_ALL_BBO']
    start_year, start_month = '2024', '01'
    end_year, end_month = '2024', '01'

    with patch('nyse_data_pipeline.download.list_dir_local_with_sizes') as mock_list_dir_local, \
         patch('nyse_data_pipeline.download.list_dir_remote_with_sizes') as mock_list_dir_remote:
    
        # 模擬 list_dir_local_with_sizes 返回空列表
        mock_list_dir_local.return_value = [
            FileInfo(name='file1.txt', size=50)
        ]
        # 模擬 list_dir_remote_with_sizes 返回一些文件信息
        mock_list_dir_remote.return_value = [
            FileInfo(name='file1.txt', size=100)
        ]

        # 執行被測試的函式
        result = collect_task(start_year, start_month, end_year, end_month,
                            sftp_mock, create_dir, safe_to, only)
        
        # 驗證結果是否符合預期
        assert len(result) == 1  # 預期蒐集到兩個任務
        # 驗證 file1.txt 是否在結果中
        assert any(task.file_name == 'file1.txt' for task in result)
        
# 你可以添加其他測試情境，例如：目錄不存在、local 和 remote 文件不匹配等等。
        
if __name__ == "__main__":
    pytest.main([__file__, '-v'])
