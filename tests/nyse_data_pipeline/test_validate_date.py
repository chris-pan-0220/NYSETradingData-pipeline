import pytest
from nyse_data_pipeline.utils import Utils
@pytest.mark.parametrize("year, month, expected", [
    ('2012', '01', False),   # Year before lower bound
    ('2025', '01', False),   # Year after upper bound
    ('2019', '00', False),   # Invalid month (zero-padded)
    ('2019', '13', False),   # Invalid month (greater than 12)
    ('2019', '05', True),    # Valid year and month (current year)
    ('2018', '12', True),    # Valid year and month (previous year)
])
def test_validate_date(year, month, expected):
    assert Utils.validate_date(year, month) == expected

if __name__ == "__main__":
    pytest.main([__file__, '-v'])