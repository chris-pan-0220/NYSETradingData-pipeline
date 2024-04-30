import pytest
from unittest.mock import patch, call

from nyse_data_pipeline.download import validate_download_config

@pytest.fixture
def download_config():
    return {
        'create_dir': True,
        'safe_to': {
            'directory1': '/path/to/directory1',
            'directory2': '/path/to/directory2'
        },
        'only': ['directory1', 'directory2']
    }

def test_only_strings_exist_in_safe_to_keys(download_config):
    with patch('os.path.exists') as mock_exists, \
        patch('os.path.isdir') as mock_dir:
        mock_exists.return_value = True
        mock_dir.return_value = True
        # No exception should be raised
        validate_download_config(download_config)

def test_invalid_only_strings(download_config):
    invalid_config = download_config.copy()
    invalid_config['only'] = ['NON_EXISTENT_DIR', 'INVALID_DIR']  # Neither exist in safe_to

    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = False
        with pytest.raises(Exception):
            validate_download_config(invalid_config)

# 可以增加其他測試用例來涵蓋不同情況下的驗證        

def test_path_exists_but_not_dir(download_config):
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = True
        with pytest.raises(Exception, match="is not a directory"):
            validate_download_config(download_config)

def test_path_not_exists_and_create_dir_is_false(download_config):
    download_config['create_dir'] = False
    with patch('os.path.exists') as mock_exists:
        mock_exists.return_value = False
        with pytest.raises(Exception) as exc_info:
            validate_download_config(download_config)
            assert "does not exist and create_dir is False" in str(exc_info.value)

def test_path_not_exists_and_create_dir_is_true(download_config):
    download_config['create_dir'] = True
    with patch('os.path.exists') as mock_exists, \
         patch('os.makedirs') as mock_makedirs:
        mock_exists.return_value = False
        validate_download_config(download_config)
        # 检查 os.makedirs 是否被正确调用了两次
        assert mock_makedirs.call_count == 2

        # 检查每次调用的参数
        expected_calls = [
            call('/path/to/directory1'),  # 预期第一次调用的参数
            call('/path/to/directory2')   # 预期第二次调用的参数
        ]
        assert mock_makedirs.call_args_list == expected_calls

if __name__ == "__main__":
    pytest.main([__file__, '-v'])