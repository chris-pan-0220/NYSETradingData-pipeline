import pytest
from nyse_data_pipeline.utils import Utils
# 定义测试用例数据
test_cases = [
    (1023, None, (1023, 'B')),
    (2048, None, (2.0, 'KB')),
    (7340032, None, (7.0, 'MB')),
    (1073741824, None, (1.0, 'GB')),
    (2048, 10, (2, 'KB', 204.8, 'B', '00:00:10'))
]

# 编写测试函数
@pytest.mark.parametrize("byte, duration, expected", test_cases)
def test_convert(byte, duration, expected):
    # 调用被测试的函数
    result = Utils.convert(byte, duration)
    # 验证结果是否符合预期
    assert result == expected

if __name__ == "__main__":
   pytest.main([__file__, '-v'])