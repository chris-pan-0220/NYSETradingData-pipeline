import pytest
from nyse_data_pipeline.download import list_dir_local_with_sizes
from nyse_data_pipeline.exception import (
    ListDirLocalError
)

@pytest.mark.parametrize("dir_path, expected_length", [
    (r'e:\SPLITS_US_ALL_BBO\SPLITS_US_ALL_BBO_2024\SPLITS_US_ALL_BBO_202401', 567),  # 示例参数组1
])
def test_list_dir_local_with_sizes(dir_path, expected_length):
    # 调用被测试函数
    files = list_dir_local_with_sizes(dir_path)
    # 断言
    assert len(files) == expected_length

# 测试函数处理抛出异常的情况
def test_list_dir_local_with_sizes_error_handling():
    # 定义一个不存在的目录路径
    non_existent_dir_path = r'e:\nonexistent_directory'
    # 使用 pytest.raises 来捕获函数可能抛出的异常
    with pytest.raises(ListDirLocalError):
        # 在此调用被测试函数，预期会抛出异常
        list_dir_local_with_sizes(non_existent_dir_path)

if __name__ == "__main__":
   pytest.main([__file__, '-v'])