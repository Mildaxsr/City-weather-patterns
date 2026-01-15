import time
import requests
import pandas as pd
from datetime import date, datetime, timedelta

#НАСТРОЙКИ

CITY = "Moscow"
LATITUDE = 55.7558
LONGITUDE = 37.6176
TIMEZONE = "Europe/Moscow"

BASE_URL = "https://archive-api.open-meteo.com/v1/archive"

OUT_HOURLY = "moscow_hourly_weather_5y.csv"
OUT_DAILY = "moscow_daily_weather_5y.csv"
OUT_MONTHLY = "moscow_monthly_weather_5y.csv"

# последние 5 лет

START_DATE = date(2021, 1, 1)
END_DATE   = date(2025, 12, 31)

# ЗАГРУЗКА

def download_hourly_weather(lat, lon, start_date, end_date, retries=5):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,precipitation",
        "timezone": TIMEZONE,
        "temperature_unit": "celsius",
        "precipitation_unit": "mm",
    }

    for i in range(retries):
        try:
            r = requests.get(BASE_URL, params=params, timeout=300)
            r.raise_for_status()
            h = r.json().get("hourly", {})
            return pd.DataFrame({
                "datetime": h.get("time", []),
                "temp_c": h.get("temperature_2m", []),
                "precip_mm": h.get("precipitation", []),
            })
        except Exception as e:
            time.sleep(min(2 ** i, 30))

    raise RuntimeError(f"Failed {start_date} {end_date}")


def daterange_chunks(start, end, step_days=60):
    cur = start
    while cur <= end:
        nxt = min(cur + timedelta(days=step_days - 1), end)
        yield cur.isoformat(), nxt.isoformat()
        cur = nxt + timedelta(days=1)

#АГРЕГАЦИИ

def build_daily(df):
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["day"] = df["datetime"].dt.date

    daily = (
        df.groupby("day", as_index=False)
          .agg(
              avg_temp=("temp_c", "mean"),
              min_temp=("temp_c", "min"),
              max_temp=("temp_c", "max"),
              precip_sum=("precip_mm", "sum"),
          )
    )

    return daily.round(2)


def build_monthly(df):
    df = df.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["day"] = df["datetime"].dt.date
    df["month"] = df["datetime"].dt.to_period("M").astype(str)

    # средние за месяц
    m_temp = df.groupby("month", as_index=False).agg(avg_temp=("temp_c", "mean"))

    daily = build_daily(df)
    daily["month"] = pd.to_datetime(daily["day"]).dt.to_period("M").astype(str)

    m_precip = daily.groupby("month", as_index=False).agg(avg_precip=("precip_sum", "mean"))

    monthly = m_temp.merge(m_precip, on="month")

    # экстремальные дни месяца
    day_ext = (
        df.groupby(["month", "day"], as_index=False)
          .agg(day_max=("temp_c", "max"), day_min=("temp_c", "min"))
    )

    max_rows = day_ext.loc[day_ext.groupby("month")["day_max"].idxmax()]
    min_rows = day_ext.loc[day_ext.groupby("month")["day_min"].idxmin()]

    monthly = (
        monthly
        .merge(max_rows[["month", "day_max"]].rename(columns={"day_max": "max_temp"}), on="month")
        .merge(min_rows[["month", "day_min"]].rename(columns={"day_min": "min_temp"}), on="month")
    )

    return monthly.round(2)


if __name__ == "__main__":
    parts = []

    for s, e in daterange_chunks(START_DATE, END_DATE):
        df_part = download_hourly_weather(LATITUDE, LONGITUDE, s, e)
        parts.append(df_part)
        time.sleep(0.2)

    df_hourly = pd.concat(parts, ignore_index=True)
    df_hourly.insert(1, "city", CITY)
    df_hourly = df_hourly[["datetime", "city", "temp_c", "precip_mm"]]

    df_hourly.to_csv(OUT_HOURLY, index=False)

    df_daily = build_daily(df_hourly)
    df_daily.to_csv(OUT_DAILY, index=False)

    df_monthly = build_monthly(df_hourly)
    df_monthly.to_csv(OUT_MONTHLY, index=False)