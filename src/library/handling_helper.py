from datetime import datetime
from decimal import Decimal


def convert_str_to_datetime(s: str, format: str = "%d/%m/%Y") -> datetime:
    try:
        result = datetime.strptime(s, format)
    except:
        result = None
    return result


def convert_str_to_float(s: str) -> float:
    try:
        if len(s) == 0:
            result = None
        else:
            result = float(s)

        if result in [float("-inf"), float("inf")]:
            result = None
    except:
        result = None
    return result


def convert_str_to_decimal(s: str) -> Decimal:
    try:
        if len(s) == 0:
            result = None
        else:
            result = Decimal(s)
    except:
        result = None
    return result


def convert_str_to_int(s: str) -> int:
    try:
        if len(s) == 0:
            result = None
        else:
            result = int(s)
    except:
        result = None
    return result

def handle_price_thousand_vnd(string):
    find_dot = string.rfind(".")
    if string == '--': return None
    elif find_dot > 0: 
        str_list = string.split(".",1)
        
        if len(str_list[1]) == 2: temp = int(str_list[1]) * 10 
        else: temp = int(str_list[1]) * 100 

        price = int(str_list[0]) * 1000 + temp

        # print(price)
        return price
    else:  return (int(string) * 1000)

# handle_price_thousand_vnd('23.3')