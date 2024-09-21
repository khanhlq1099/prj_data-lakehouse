from enum import Enum


class PERIOD_TYPE(Enum):
    PERIOD = "PERIOD"
    YTD = "YTD"
    QTD = "QTD"
    MTD = "MTD"
    TODAY = "TODAY"
    YESTERDAY = "YESTERDAY"

# print(PERIOD_TYPE.YTD.name)