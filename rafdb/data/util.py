from datetime import datetime, date


def ensure_date(d) -> datetime:
    if isinstance(d, datetime):
        return d

    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)
        
    try:
        return datetime.strptime(d, '%Y%m%d')
    except ValueError:
        pass

    return datetime.strptime(d, '%Y-%m-%d')


def ensure_datetime(d) -> datetime:
    if isinstance(d, datetime):
        return d

    if isinstance(d, date):
        return datetime(d.year, d.month, d.day)
        
    try:
        return datetime.strptime(d, '%Y%m%d')
    except ValueError:
        pass

    return datetime.strptime(d, '%Y-%m-%d')


def iter_dates(start_date, end_date=None):
    if isinstance(start_date, str):
        start_date = datetime.strptime(start_date, '%Y%m%d')
    if end_date is None:
        end_date = start_date
    if isinstance(end_date, str):
        end_date = datetime.strptime(end_date, '%Y%m%d')
    from datetime import timedelta
    return (start_date + timedelta(days=n) for n in range((end_date-start_date).days+1))
