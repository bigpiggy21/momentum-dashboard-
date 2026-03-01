"""
NYSE Trading Calendar — algorithmic holiday computation.

Generates the complete list of NYSE market holidays for any year,
then builds a trading day calendar. Used by data_fetcher.py to
align multi-day and multi-week bar groupings with TradingView.

Holiday rules follow NYSE's published schedule:
  - New Year's Day: Jan 1
  - MLK Day: 3rd Monday in January
  - Presidents' Day: 3rd Monday in February
  - Good Friday: Friday before Easter Sunday
  - Memorial Day: Last Monday in May
  - Juneteenth: June 19 (observed since 2022)
  - Independence Day: July 4
  - Labor Day: 1st Monday in September
  - Thanksgiving: 4th Thursday in November
  - Christmas: December 25

Weekend adjustment: if a holiday falls on Saturday, the preceding
Friday is observed. If it falls on Sunday, the following Monday
is observed.

Source: https://www.nyse.com/trade/hours-calendars
"""

from datetime import date, timedelta
from functools import lru_cache


def _easter_sunday(year):
    """Compute Easter Sunday using the Anonymous Gregorian algorithm.
    Valid for any year in the Gregorian calendar."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month, day = divmod(h + l - 7 * m + 114, 31)
    return date(year, month, day + 1)


def _nth_weekday(year, month, weekday, n):
    """Find the nth occurrence of a weekday in a given month.
    weekday: 0=Mon, 1=Tue, ..., 6=Sun.  n: 1-based."""
    first = date(year, month, 1)
    # Days until first occurrence of target weekday
    offset = (weekday - first.weekday()) % 7
    first_occurrence = first + timedelta(days=offset)
    return first_occurrence + timedelta(weeks=n - 1)


def _last_weekday(year, month, weekday):
    """Find the last occurrence of a weekday in a given month."""
    # Start from 5th week back and find the last one
    if month == 12:
        last_day = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        last_day = date(year, month + 1, 1) - timedelta(days=1)
    offset = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=offset)


def _weekend_adjust(d):
    """Adjust a holiday date for weekends.
    Saturday -> Friday, Sunday -> Monday."""
    if d.weekday() == 5:  # Saturday
        return d - timedelta(days=1)
    elif d.weekday() == 6:  # Sunday
        return d + timedelta(days=1)
    return d


@lru_cache(maxsize=64)
def nyse_holidays(year):
    """Return a frozenset of NYSE market holiday dates for a given year."""
    holidays = []
    
    # New Year's Day — Jan 1
    # Special case: if Jan 1 falls on Saturday, NO Friday observance
    # (NYSE doesn't observe the prior-year Friday for New Year's)
    nyd = date(year, 1, 1)
    if nyd.weekday() == 5:
        pass  # Saturday: not observed (confirmed by NYSE 2028 calendar)
    elif nyd.weekday() == 6:
        holidays.append(date(year, 1, 2))  # Sunday -> Monday
    else:
        holidays.append(nyd)
    
    # MLK Day — 3rd Monday in January
    holidays.append(_nth_weekday(year, 1, 0, 3))  # 0 = Monday
    
    # Presidents' Day — 3rd Monday in February
    holidays.append(_nth_weekday(year, 2, 0, 3))
    
    # Good Friday — Friday before Easter
    easter = _easter_sunday(year)
    holidays.append(easter - timedelta(days=2))
    
    # Memorial Day — Last Monday in May
    holidays.append(_last_weekday(year, 5, 0))
    
    # Juneteenth — June 19 (observed since 2022)
    if year >= 2022:
        holidays.append(_weekend_adjust(date(year, 6, 19)))
    
    # Independence Day — July 4
    holidays.append(_weekend_adjust(date(year, 7, 4)))
    
    # Labor Day — 1st Monday in September
    holidays.append(_nth_weekday(year, 9, 0, 1))
    
    # Thanksgiving — 4th Thursday in November
    holidays.append(_nth_weekday(year, 11, 3, 4))  # 3 = Thursday
    
    # Christmas — December 25
    holidays.append(_weekend_adjust(date(year, 12, 25)))
    
    return frozenset(holidays)


def is_trading_day(d):
    """Check if a date is a NYSE trading day (not weekend, not holiday)."""
    if d.weekday() >= 5:  # Weekend
        return False
    return d not in nyse_holidays(d.year)


@lru_cache(maxsize=32)
def trading_days(start_year, end_year):
    """Return a sorted list of all NYSE trading days for a range of years.
    Includes start_year and end_year."""
    days = []
    d = date(start_year, 1, 1)
    end = date(end_year, 12, 31)
    while d <= end:
        if is_trading_day(d):
            days.append(d)
        d += timedelta(days=1)
    return days


def validate_against_nyse(year):
    """Validate our computed holidays against known NYSE holidays.
    Returns (matches, mismatches) tuple."""
    # Known NYSE holidays from their published calendar
    known = {
        2026: {
            date(2026, 1, 1),   # New Year's Day (Thursday)
            date(2026, 1, 19),  # MLK Day
            date(2026, 2, 16),  # Presidents' Day
            date(2026, 4, 3),   # Good Friday
            date(2026, 5, 25),  # Memorial Day
            date(2026, 6, 19),  # Juneteenth (Friday)
            date(2026, 7, 3),   # Independence Day observed (Friday)
            date(2026, 9, 7),   # Labor Day
            date(2026, 11, 26), # Thanksgiving
            date(2026, 12, 25), # Christmas (Friday)
        },
        2027: {
            date(2027, 1, 1),   # New Year's Day (Friday)
            date(2027, 1, 18),  # MLK Day
            date(2027, 2, 15),  # Presidents' Day
            date(2027, 3, 26),  # Good Friday
            date(2027, 5, 31),  # Memorial Day
            date(2027, 6, 18),  # Juneteenth observed (Friday)
            date(2027, 7, 5),   # Independence Day observed (Monday)
            date(2027, 9, 6),   # Labor Day
            date(2027, 11, 25), # Thanksgiving
            date(2027, 12, 24), # Christmas observed (Friday)
        },
        2028: {
            # Note: Jan 1, 2028 is Saturday — NOT observed per NYSE
            date(2028, 1, 17),  # MLK Day
            date(2028, 2, 21),  # Presidents' Day
            date(2028, 4, 14),  # Good Friday
            date(2028, 5, 29),  # Memorial Day
            date(2028, 6, 19),  # Juneteenth (Monday)
            date(2028, 7, 4),   # Independence Day (Tuesday)
            date(2028, 9, 4),   # Labor Day
            date(2028, 11, 23), # Thanksgiving
            date(2028, 12, 25), # Christmas (Monday)
        },
    }
    
    if year not in known:
        return None, None
    
    computed = nyse_holidays(year)
    expected = known[year]
    
    matches = computed & expected
    extra = computed - expected
    missing = expected - computed
    
    return {
        'matches': sorted(matches),
        'extra': sorted(extra),
        'missing': sorted(missing),
        'valid': len(extra) == 0 and len(missing) == 0,
    }


# Quick self-test when run directly
if __name__ == '__main__':
    for year in [2026, 2027, 2028]:
        result = validate_against_nyse(year)
        status = '✅' if result['valid'] else '❌'
        print(f"{year}: {status}  holidays={sorted(nyse_holidays(year))}")
        if result['extra']:
            print(f"  Extra (we have, NYSE doesn't): {result['extra']}")
        if result['missing']:
            print(f"  Missing (NYSE has, we don't): {result['missing']}")
    
    # Count trading days for 2026
    td = trading_days(2026, 2026)
    print(f"\n2026: {len(td)} trading days (NYSE says 251 for equities)")
    
    # Show holidays for a few more years
    for year in [2024, 2025]:
        h = sorted(nyse_holidays(year))
        print(f"\n{year} holidays ({len(h)}):")
        for d in h:
            print(f"  {d.strftime('%Y-%m-%d %A')}")
