import os
import logging
import pytest
from unittest.mock import MagicMock, patch

# 导入要测试的函数
from nyse_data_pipeline.download import download_by_paramiko, dict_to_download_info

# 定义测试数据
json = {
    "remote_dir": "/remote/directory",
    "local_dir": os.getcwd(),
    "file_name": "test_file.txt",
    "size": 1024,  # 指定文件大小，用于计算下载速率
}
download_info = dict_to_download_info(json)

# 定义测试函数
@patch('nyse_data_pipeline.download.connect_to_sftp')
def test_download_by_paramiko_successful_download(mock_connect_to_sftp):
    # 创建一个模拟的 SFTPClient 对象
    sftp_mock = MagicMock()
    client_mock = MagicMock()
    mock_connect_to_sftp.return_value = (client_mock, sftp_mock)

    # 设置成功下载的情况
    def successful_download(remote_path, local_path):
        # 创建一个测试文件
        test_data = "A" * 1024  # 创建一个大小为1024字节的测试数据
        print('local path: ', local_path)
        with open(local_path, 'w') as f:
            f.write(test_data)

    sftp_mock.get.side_effect = successful_download

    # 运行被测试的函数
    result = download_by_paramiko(sftp_mock, download_info)

    # 断言下载完成后文件存在且返回值为 True
    assert os.path.exists(download_info.local_dir + "\\" + download_info.file_name) == True
    assert result == True


@patch('nyse_data_pipeline.download.connect_to_sftp')
def test_download_by_paramiko_download_failed(mock_connect_to_sftp):
    # 创建一个模拟的 SFTPClient 对象
    sftp_mock = MagicMock()
    client_mock = MagicMock()
    mock_connect_to_sftp.return_value = (client_mock, sftp_mock)

    # 设置下载失败的情况，模拟抛出异常
    sftp_mock.get.side_effect = Exception('Download failed')

    # 运行被测试的函数
    with pytest.raises(Exception) as e:
        result = download_by_paramiko(sftp_mock, download_info)

    # 断言 download_info 中的 download 字段没有被设置为 True
    assert os.path.exists(download_info.local_dir + "\\" + download_info.file_name) == False

@patch('nyse_data_pipeline.download.connect_to_sftp')
def test_download_by_paramiko_size_missmatch(mock_connect_to_sftp):
    # 创建一个模拟的 SFTPClient 对象
    sftp_mock = MagicMock()
    client_mock = MagicMock()
    mock_connect_to_sftp.return_value = (client_mock, sftp_mock)

    # 设置下载失敗的情况
    def fail_download(remote_path, local_path):
        # 创建一个测试文件
        with open(local_path, 'w') as f:
            f.write("This is a test file.")
    
    sftp_mock.get.side_effect = fail_download

    # 运行被测试的函数
    with pytest.raises(Exception) as e:
        result = download_by_paramiko(sftp_mock, download_info)

    # 断言下载完成后文件大小不一致，文件已被删除，download_info['download'] == False
    assert os.path.exists(download_info.local_dir + "/" + download_info.file_name) == False

# 在测试前，可以设置日志级别
# logging.basicConfig(level=logging.DEBUG)

# 运行测试
if __name__ == "__main__":
   pytest.main([__file__, '-v'])