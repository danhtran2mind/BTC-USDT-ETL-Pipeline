from datetime import timezone, timedelta
import re

def parse_timezone(tz_offset_str):
    """
    Parse a timezone offset string (e.g., '+07:00') and return a timezone object.
    
    Args:
        tz_offset_str (str): Timezone offset in format '[+-]HH:MM'
    
    Returns:
        timezone: A datetime.timezone object with the specified offset
    
    Raises:
        ValueError: If the timezone format is invalid
    """
    match = re.match(r'([+-])(\d{2}):(\d{2})', tz_offset_str)
    if not match:
        raise ValueError(f"Invalid timezone format: {tz_offset_str}")
    
    sign, hours, minutes = match.groups()
    hours, minutes = int(hours), int(minutes)
    if sign == '-':
        hours, minutes = -hours, -minutes
    
    return timezone(timedelta(hours=hours, minutes=minutes))