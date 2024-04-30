import paramiko
import pytest
from unittest.mock import Mock
from nyse_data_pipeline.download import list_dir_remote_with_sizes
from nyse_data_pipeline.exception import (
    ListDirRemoteError
)

@pytest.fixture
def mock_sftp():
    sftp = Mock(spec=paramiko.SFTPClient)
    # 配置 sftp.listdir 和 sftp.stat 的行為
    sftp.listdir.return_value = ['file1.txt', 'file2.txt']
    sftp.stat.return_value.st_size = 100 # 假設所有文件大小都是 100
    return sftp

def test_list_dir_remote_with_sizes(mock_sftp):
    remote_dir = '/path/to/remote/directory'
    files = list_dir_remote_with_sizes(mock_sftp, remote_dir)
    assert len(files) == 2
    assert files[0].name == 'file1.txt'
    assert files[0].size == 100
    assert files[1].name == 'file2.txt'
    assert files[1].size == 100
    # 如果有特定的例外應該被處理，也可以寫測試來確保它們被捕獲和處理

def test_list_dir_remote_with_sizes_connection_error(mock_sftp):
    remote_dir = '/path/to/remote/directory'

    mock_sftp.listdir.side_effect = paramiko.SSHException("Connection reset by peer")
    mock_sftp.stat.side_effect = paramiko.SSHException("Connection reset by peer")

    with pytest.raises(ListDirRemoteError) as e:
        files = list_dir_remote_with_sizes(mock_sftp, remote_dir)

    # Execute the tests
if __name__ == "__main__":
    pytest.main([__file__, '-v'])