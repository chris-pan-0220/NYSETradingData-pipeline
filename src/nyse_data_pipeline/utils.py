# utils.py
from datetime import datetime

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

    # Other utility functions can be added here
