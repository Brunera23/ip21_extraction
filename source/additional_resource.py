from datetime import datetime
import pandas as pd
import re


def time_todecaseconds (value):
    if pd.isna(value):
        return ""
    m = re.match(r"\+?(\d+):(\d+):([\d\.]+)", str(value))
    if m:
        horas, minutos, segundos = m.groups()
        total_seconds = int(horas) * 3600 + int(minutos) * 60 + float(segundos)
        total_tenths_seconds = int(total_seconds * 10)
        return total_tenths_seconds
    try:
        return int(float(value) * 10)
    except Exception:
        return value

def time_toseconds(value):
    if pd.isna(value):
        return ""
    m = re.match(r"\+?(\d+):(\d+):([\d\.]+)", str(value))
    if m:
        horas, minutos, segundos = m.groups()
        total = int(horas) * 3600 + int(minutos) * 60 + float(segundos)
        return int(total)
    try:
        return int(float(value))
    except Exception:
        return value
    
def on_off_to_binary(value):
    if isinstance(value, str):
        v = value.strip().upper()
        if v == "ON":
            return 1
        if v == "OFF":
            return 0
        if v in {"1", "0"}:
            return int(v)
    if value in {0, 1}:
        return value
    return ""    