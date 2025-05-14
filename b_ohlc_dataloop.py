import requests
import pandas as pd
import os
import time
from requests.exceptions import RequestException
from datetime import datetime, timedelta, timezone

BINANCE = "https://api.binance.com/api/v1/klines"
LIMIT = 1500
INTERVAL = '1m'
OUTPUT_DIR = "2024-OneMinuteFirstListersfrom2023"

COLS = ['datetime', 'open', 'high', 'low', 'close', 'volume','close_time', 'quote_vol', 'n_trades','taker_buy_vol', 'taker_buy_quote', 'ignore']

def to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def to_dataframe(rows):
    if not rows: return None
    df = pd.DataFrame(rows, columns=COLS)
    df['datetime'] = pd.to_datetime(df['datetime'], unit='ms', utc=True)
    df = df.set_index('datetime')
    return df.astype({
        'open':'float32','high':'float32','low':'float32',
        'close':'float32','volume':'float32',
        'quote_vol':'float32','taker_buy_vol':'float32',
        'taker_buy_quote':'float32','n_trades':'int32'
    })

def fetch_klines(symbol, interval, start_ms, end_ms):
    params = dict(symbol=symbol, interval=interval,
                    startTime=start_ms, endTime=end_ms,
                    limit=LIMIT)
    try:
        resp = requests.get(BINANCE, params=params, timeout=10)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None

    used = int(resp.headers.get("X-MBX-USED-WEIGHT-1M", 0))
    if resp.status_code == 429:
        retry = int(resp.headers.get("Retry-After", 240))
        print(f"429 hit; sleeping {retry}s")
        time.sleep(retry)
        return None

    rows = resp.json()
    print(used)

    if not rows:
        print(f"Error {resp.status_code} for {symbol} ({interval}): {resp.text} Limit: {used}")
        return None

    if used > 2200:
        print("Soft limit near; sleeping 60s")
        time.sleep(60)

    return to_dataframe(rows=rows)

def process_symbol(symbol, start_date, end_date):
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')
    start_ms = to_ms(start)
    end_ms = to_ms(end)

    output_file = os.path.join(OUTPUT_DIR, f"{symbol}_{INTERVAL}_{start.strftime('%Y-%m-%d')}_to_{end.strftime('%Y-%m-%d')}.parquet")
    print(f"Processing {symbol}: {start_date} → {end_date}  Output: {output_file}")

    all_data = pd.DataFrame()
    current_start_ms = start_ms

    while current_start_ms < end_ms:
        df_data = fetch_klines(symbol, INTERVAL, current_start_ms, end_ms)
       
        if df_data is not None and not df_data.empty:
            all_data = pd.concat([all_data, df_data])
            print(f"   Fetched {len(df_data)} rows ➟ {df_data.index[-1]} for {symbol}")
            current_start_ms = to_ms(df_data.index[-1] + pd.Timedelta(minutes=1))
            time.sleep(0.0109)
        else:
            break


    if not all_data.empty:
        all_data = all_data[~all_data.index.duplicated(keep='last')]
        all_data.to_parquet(output_file, engine='pyarrow', compression='snappy')
        print(f"   Data saved to {output_file}")
    else:
        print(f"   No data fetched for {symbol} between {start_date} and {end_date}")






if __name__ == "__main__":
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    date_ranges =  []


    for item in date_ranges:
        try:
            process_symbol(item['symbol'], item['start_date'], item['end_date'])
        except Exception as e:
            print(f"⚠️ Error processing {item['symbol']}: {e}")
