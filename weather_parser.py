import requests
import pandas as pd
from datetime import datetime, timedelta

CITY = "Moscow"
LATITUDE = 55.7558
LONGITUDE = 37.6176

START_DATE = "2025-01-01"
END_DATE = "2025-12-01"

OUT_CSV = "moscow_hourly_weather.csv"
BASE_URL = "https://archive-api.open-meteo.com/v1/archive"


def download_hourly_weather(lat, lon, start_date, end_date, timezone="Europe/Moscow"):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": timezone,
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
    }
    r = requests.get(BASE_URL, params=params, timeout=300)
    r.raise_for_status()
    data = r.json()
    hourly = data["hourly"]
    return pd.DataFrame(
        {
            "datetime": hourly["time"],
            "temp_c": hourly["temperature_2m"],
            "precip_mm": hourly["precipitation"],
        }
    )


def daterange_chunks(start_str, end_str, step_days=60):
    start = datetime.fromisoformat(start_str)
    end = datetime.fromisoformat(end_str)
    cur = start
    delta = timedelta(days=step_days - 1)
    while cur <= end:
        nxt = min(cur + delta, end)
        yield cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d")
        cur = nxt + timedelta(days=1)


if __name__ == "__main__":
    dfs = []
    for s, e in daterange_chunks(START_DATE, END_DATE, step_days=60):
        df_part = download_hourly_weather(LATITUDE, LONGITUDE, s, e)
        dfs.append(df_part)
    df = pd.concat(dfs, ignore_index=True)
    df.insert(1, "city", CITY)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")
    print(len(df))