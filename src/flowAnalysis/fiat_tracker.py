from datetime import datetime
import pytz
import ccxt
import pandas as pd
FIAT_CURRENCIES = {"USD", "EUR", "RUB", "TRY", "JPY", "GBP", "AUD", "CAD", "CHF", "ZAR", "MXN", "SGD", "HKD"}
STABLECOINS = {"USDT", "USDC", "BUSD", "TUSD", "DAI", "GUSD", "USDP"}




def list_fiat_stable_pairs(exchange_name: str, fiat_hint: str = None, stable_hint: str = None):
    import ccxt

    # Define common fiat and stablecoin symbols
    fiat_currencies = {"USD", "EUR", "RUB", "TRY", "JPY", "GBP", "AUD", "CAD", "CHF", "ZAR", "MXN", "SGD", "HKD"}
    stablecoins = {"USDT", "USDC", "BUSD", "TUSD", "DAI", "GUSD", "USDP"}

    # Normalize input
    fiat_hint = fiat_hint.upper() if fiat_hint else None
    stable_hint = stable_hint.upper() if stable_hint else None

    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class()

    try:
        markets = exchange.load_markets()
    except Exception as e:
        print(f"❌ Error loading markets for {exchange_name}: {e}")
        return

    print(f"\n🔎 Fiat ⇄ Stablecoin Pairs on {exchange_name}:")

    found = False
    for symbol in markets:
        if "/" not in symbol:
            continue
        base, quote = symbol.split("/")

        # One side must be fiat, the other must be a stablecoin
        fiat_side = base in fiat_currencies or quote in fiat_currencies
        stable_side = base in stablecoins or quote in stablecoins

        # Must match fiat + stablecoin
        if not (fiat_side and stable_side):
            continue

        # Apply user-specified filters if present
        if fiat_hint and fiat_hint not in (base, quote):
            continue
        if stable_hint and stable_hint not in (base, quote):
            continue

        print(f" - {symbol}")
        found = True

    if not found:
        print("⚠️ No fiat-stablecoin pairs found with current filters.")




from datetime import datetime
import pytz
import ccxt
import pandas as pd

def fetch_fiat_stable_trades(exchange_name: str, base_symbol: str, quote_symbol: str, start_dt: datetime, end_dt: datetime):
    exchange_class = getattr(ccxt, exchange_name)
    exchange = exchange_class()

    base_upper = base_symbol.upper()
    quote_upper = quote_symbol.upper()

    pair_a = f"{base_upper}/{quote_upper}"
    pair_b = f"{quote_upper}/{base_upper}"

    markets = exchange.load_markets()

    if pair_a in markets:
        pair = pair_a
    elif pair_b in markets:
        pair = pair_b
    else:
        raise ValueError(f"❌ Neither {pair_a} nor {pair_b} supported on {exchange_name}")

    # ✅ Use actual CCXT pair for both symbol and adjusted_pair
    adjusted_pair = pair

    # Ensure datetime inputs are UTC-aware
    start_dt_utc = start_dt.replace(tzinfo=pytz.UTC)
    end_dt_utc = end_dt.replace(tzinfo=pytz.UTC)

    since = int(start_dt_utc.timestamp() * 1000)
    end_ts = int(end_dt_utc.timestamp() * 1000)

    print(f"\n🔎 Fetching trades for {pair} on {exchange_name} between {start_dt_utc} and {end_dt_utc} (UTC)...")
    print(f"🕒 Converted timestamp range: {pd.to_datetime(since, unit='ms')} to {pd.to_datetime(end_ts, unit='ms')}")

    # Optional quick test
    try:
        print(f"📦 Sample check: Fetching last 5 trades for {pair}...")
        recent = exchange.fetch_trades(pair, limit=5)
        print(f"✅ Got {len(recent)} recent trades")
    except Exception as e:
        print(f"⚠️ Error fetching recent trades: {e}")

    all_trades = []
    while since < end_ts:
        try:
            print(f"→ Fetching from {pd.to_datetime(since, unit='ms')} to {pd.to_datetime(end_ts, unit='ms')}")
            trades = exchange.fetch_trades(pair, since=since, limit=1000)
        except Exception as e:
            print(f"⚠️ Error fetching trades: {e}")
            break

        if not trades:
            print("→ No trades returned.")
            break

        all_trades.extend(trades)
        last_ts = trades[-1]['timestamp']
        if last_ts <= since:
            print("⚠️ Stuck at same timestamp, breaking to prevent infinite loop.")
            break

        since = last_ts + 1

    # Fallback to OHLCV if no trades
    if not all_trades:
        print("⚠️ No trade-level data — falling back to OHLCV...")

        try:
            ohlcv = exchange.fetch_ohlcv(pair, timeframe='1h', since=since)
            if not ohlcv:
                print("⚠️ No OHLCV data found either.")
                return None, None

            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            # Filter by time range
            start_dt_naive = start_dt_utc.replace(tzinfo=None)
            end_dt_naive = end_dt_utc.replace(tzinfo=None)
            df = df[(df['datetime'] >= start_dt_naive) & (df['datetime'] <= end_dt_naive)]

            # Compute volume metrics
            usdt_volume = df['volume'].sum()  # base asset volume
            fiat_volume = (df['volume'] * df['close']).sum()
            vwap = (df['close'] * df['volume']).sum() / df['volume'].sum()

            summary = {
                "exchange": exchange_name,
                "symbol": pair,                 # raw CCXT pair
                "adjusted_pair": adjusted_pair, # use actual pair for correct direction
                "usdt_volume": round(usdt_volume, 2),
                "fiat_volume": round(fiat_volume, 2),
                "vwap": round(vwap, 4),
                "trades": len(df),
                "start": start_dt,
                "end": end_dt,
            }

            return df, summary

        except Exception as e:
            print(f"⚠️ OHLCV fallback failed: {e}")
            return None, None


def interactive_fiat_tracker():
    """
    CLI prompt for fetching fiat → stablecoin trades from a CEX.
    Saves output to a labeled folder and optionally uploads to BigQuery.
    """
    from datetime import datetime
    import os
    import pandas as pd
    from flowAnalysis.fiat_tracker import fetch_fiat_stable_trades
    from bigQueryUtils import upload_fiat_trades_to_bq

    print("\n📊 Fiat → Stablecoin Market Trade Fetcher")

    exchange = input("🌐 Exchange (e.g. coinbase, binance): ").strip()
    base = input("💱 Base fiat currency (e.g. RUB, USD): ").strip()
    quote = input("🪙 Quote stablecoin (e.g. USDT, USDC): ").strip()

    # Time inputs
    while True:
        try:
            start_dt = datetime.strptime(input("🕐 Start datetime (YYYY-MM-DD HH:MM): ").strip(), "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(input("🕐 End datetime (YYYY-MM-DD HH:MM): ").strip(), "%Y-%m-%d %H:%M")
            break
        except ValueError:
            print("❌ Invalid format. Please use YYYY-MM-DD HH:MM")

    df, summary = fetch_fiat_stable_trades(exchange, base, quote, start_dt, end_dt)

    if df is None or df.empty:
        print("⚠️ No data returned.")
        return

    print("\n📈 Summary:")
    for k, v in summary.items():
        print(f"{k}: {v}")

    tag = f"{exchange}_{base}_{quote}_{start_dt.strftime('%Y-%m-%d_%H:%M')}"

    # Save to CSV
    save = input("\n💾 Save to CSV? (y/n): ").strip().lower()
    if save == "y":
        user_tag = input("🏷️ Enter a label for this time period (e.g. russian_sanctions): ").strip().replace(" ", "_")
        if user_tag:
            tag = user_tag

        folder = os.path.join("CEX_FIAT_to_USDT", tag)
        os.makedirs(folder, exist_ok=True)

        filename = f"{exchange}_{base.upper()}_{quote.upper()}_{start_dt.strftime('%Y-%m-%d_%H:%M')}_to_{end_dt.strftime('%Y%m%d_%H%M')}.csv"

        if 'timestamp' not in df.columns and 'datetime' in df.columns:
            df['timestamp'] = df['datetime']
            df.drop(columns=['datetime'], inplace=True)

        if df['timestamp'].dtype in ['float64', 'int64']:
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', utc=True, errors='coerce')
        else:
            df['timestamp'] = pd.to_datetime(df['timestamp'], utc=True, errors='coerce')

        df.rename(columns={"timestamp": "date"}, inplace=True)

        output_path = os.path.join(folder, filename)
        df.to_csv(output_path, index=False)
        print(f"✅ Saved to {output_path}")

    # Upload to BigQuery
    upload = input("\n🚀 Upload this file to BigQuery? (y/n): ").strip().lower()
    if upload == "y":
        # ✅ Add all expected fields
        for field in ['exchange', 'symbol', 'adjusted_pair', 'vwap', 'usdt_volume', 'fiat_volume']:
            if field in summary:
                df[field] = summary[field]

        df['label'] = tag

        for col in ["timestamp", "datetime", "Date_UTC_Time"]:
            if col in df.columns:
                df.drop(columns=[col], inplace=True)

        print("📋 Final columns being uploaded to BigQuery:")
        print(df.columns.tolist())

        upload_fiat_trades_to_bq(
            df=df,
            project_id="macropipeline",
            dataset_id="fiatToUSDTCEX",
            table_id=exchange,
            tag=tag
        )
