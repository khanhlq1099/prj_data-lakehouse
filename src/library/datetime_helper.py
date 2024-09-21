from datetime import datetime, timezone, date, timedelta
from src.config.period import PERIOD_TYPE

def get_utc_timestamp(dt: datetime) -> str:
    return dt.replace(tzinfo=timezone.utc).timestamp()

def calc_period_range(exact_date: date, period_type: PERIOD_TYPE):
    from_date: date = None
    to_date: date = None

    if(period_type == PERIOD_TYPE.TODAY):
        from_date = exact_date
        to_date = exact_date
    elif(period_type == PERIOD_TYPE.YESTERDAY):
        from_date = exact_date + timedelta(days=-1)
        to_date = from_date
    elif(period_type == PERIOD_TYPE.MTD):
        from_date = exact_date.replace(day=1)
        to_date = exact_date
    elif(period_type == PERIOD_TYPE.QTD):
        quarter = round((exact_date.month - 1) / 3 + 1)
        from_date = datetime(exact_date.year, 3 * quarter - 2, 1)
        to_date = exact_date
    elif(period_type == PERIOD_TYPE.YTD):
        from_date = exact_date.replace(month=1, day=1)
        to_date = exact_date

    return from_date, to_date
