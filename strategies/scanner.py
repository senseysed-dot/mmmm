# -*- coding: utf-8 -*-
import os
import pandas as pd
import numpy as np
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import safe_filename

# 最少回傳股票數（不足時自動降分填充）
MIN_RESULTS = 10
# 最多回傳股票數
MAX_RESULTS = 30
# 通過門檻（滿分 9 分，需達此分才列入）
MIN_SCORE = 4

# 進出場比例常數
TARGET_PCT  = 1.10  # 目標價 = 收盤 × 110%（+10%）
STOP_PCT    = 0.95  # 停損價 = 收盤 × 95%（−5%）

# 空方力竭 RSI 判斷門檻
RSI_EXHAUSTION_LOW  = 40   # RSI 曾低於此值才視為超賣
RSI_EXHAUSTION_HIGH = 65   # RSI 尚未過熱（> 此值則多頭動能已充分釋放，不再計入）


# ──────────────────────────────────────────────────────────────
# MACD 計算輔助函式
# ──────────────────────────────────────────────────────────────

def _macd_hist(series, fast=12, slow=26, signal=9):
    """計算 MACD 柱體（histogram = MACD line − Signal line）"""
    if len(series.dropna()) < slow:
        return pd.Series(dtype=float, index=series.index)
    ema_f = series.ewm(span=fast,   adjust=False).mean()
    ema_s = series.ewm(span=slow,   adjust=False).mean()
    macd  = ema_f - ema_s
    sig   = macd.ewm(span=signal, adjust=False).mean()
    return macd - sig


def _hist_just_turned_positive(hist):
    """柱體剛從負轉正（轉正第一根紅柱）"""
    vals = hist.dropna()
    return len(vals) >= 2 and bool(vals.iloc[-1] > 0 and vals.iloc[-2] <= 0)


def _hist_positive(hist):
    """柱體目前為正（紅柱）"""
    vals = hist.dropna()
    return len(vals) >= 1 and bool(vals.iloc[-1] > 0)


def _hist_shrinking_upward(hist, n=3):
    """
    柱體持續 n 根縮短向上：
      • 最後 n 根均為負值，且連續遞增（向 0 收斂）
      • 或最後一根剛由負轉正（更強烈的即將轉正信號）
    """
    vals = hist.dropna().values
    if len(vals) < n:
        return False
    tail = vals[-n:]
    # 剛由負轉正也符合「即將轉正」
    if tail[-1] >= 0 and tail[-2] < 0:
        return True
    # 最後一根仍為負，但連續遞增
    if tail[-1] < 0:
        return all(tail[i] > tail[i - 1] for i in range(1, n))
    return False


def _hist_about_to_turn(hist, n=2):
    """
    柱體即將翻紅：
      • 最後 n+1 根中連續縮短（最後一根仍為負）
      • 或最後一根剛由負轉正
    """
    vals = hist.dropna().values
    if len(vals) < n + 1:
        return False
    tail = vals[-(n + 1):]
    # 剛翻正
    if tail[-1] > 0 and tail[-2] <= 0:
        return True
    # 負值且連續縮短
    if tail[-1] < 0:
        return all(tail[i] > tail[i - 1] for i in range(1, n + 1))
    return False


# ──────────────────────────────────────────────────────────────
# 多時間軸重採樣
# ──────────────────────────────────────────────────────────────

def _resample_weekly(daily_df):
    """日K → 週K（週末最後一根收盤）"""
    df = daily_df[['date', 'close']].copy()
    df['date'] = pd.to_datetime(df['date'])
    return df.set_index('date').sort_index()['close'].resample('W').last().dropna()


def _resample_monthly(daily_df):
    """日K → 月K（月末最後一根收盤）"""
    df = daily_df[['date', 'close']].copy()
    df['date'] = pd.to_datetime(df['date'])
    s = df.set_index('date').sort_index()['close']
    try:
        return s.resample('ME').last().dropna()   # pandas >= 2.2
    except ValueError:
        return s.resample('M').last().dropna()    # pandas < 2.2


# ──────────────────────────────────────────────────────────────
# RSI 計算
# ──────────────────────────────────────────────────────────────

def _calc_rsi(close, period=14):
    """計算 RSI(period)"""
    delta = close.diff()
    gain  = delta.where(delta > 0, 0.0).rolling(period).mean()
    loss  = (-delta.where(delta < 0, 0.0)).rolling(period).mean()
    rs    = gain / loss.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


# ──────────────────────────────────────────────────────────────
# 單支股票分析
# ──────────────────────────────────────────────────────────────

def _analyze_symbol(group, data_dir=None):
    """
    對單支股票計算多時間軸信號並計分。
    回傳 dict，資料不足時回傳 None。

    評分（滿分 9）：
      +2  月K MACD 柱體轉正第一根（最重要的趨勢反轉信號）
      +1  月K MACD 柱體已是紅柱（次級月K加分）
      +2  周K MACD 為紅柱（趨勢確認）
      +1  日K MACD 柱體即將翻紅（縮短向上）或剛翻紅
      +1  60分K MACD 柱體持續縮短向上（即將轉正）
      +1  量縮（成交量 < 5日均量，積累整理）
      +1  空方力竭（RSI 從低點回升，空方動能衰竭）
    """
    df = group.sort_values('date').reset_index(drop=True)
    if len(df) < 60:
        return None

    close  = df['close'].astype(float)
    volume = df['volume'].astype(float)
    symbol = str(df['symbol'].iloc[0])
    name   = str(df['name'].iloc[0]) if 'name' in df.columns else symbol

    # ── 日K MACD ──────────────────────────────────────────────
    daily_hist = _macd_hist(close)

    # ── 週K MACD ──────────────────────────────────────────────
    weekly_close = _resample_weekly(df)
    weekly_hist  = _macd_hist(weekly_close) if len(weekly_close) >= 26 else pd.Series(dtype=float)

    # ── 月K MACD ──────────────────────────────────────────────
    monthly_close = _resample_monthly(df)
    monthly_hist  = _macd_hist(monthly_close) if len(monthly_close) >= 26 else pd.Series(dtype=float)

    # ── 60分K MACD（可選，需先下載 60m 資料）─────────────────
    m60_hist = pd.Series(dtype=float)
    if data_dir:
        safe_name = safe_filename(name)
        m60_path  = os.path.join(data_dir, f"{symbol}_{safe_name}_60m.csv")
        if os.path.exists(m60_path):
            try:
                m60_df = pd.read_csv(m60_path)
                m60_df.columns = [c.lower() for c in m60_df.columns]
                if 'close' in m60_df.columns and len(m60_df) >= 35:
                    m60_hist = _macd_hist(m60_df['close'].astype(float).reset_index(drop=True))
            except Exception:
                pass

    # ── RSI(14) 日K ───────────────────────────────────────────
    rsi = _calc_rsi(close)

    # ── 計分 ──────────────────────────────────────────────────
    score = 0
    flags = []

    # 月K MACD（+2 / +1）
    if len(monthly_hist.dropna()) >= 2:
        if _hist_just_turned_positive(monthly_hist):
            score += 2
            flags.append('月K轉正首根')
        elif _hist_positive(monthly_hist):
            score += 1
            flags.append('月K紅柱')

    # 周K MACD 紅柱（+2）
    if len(weekly_hist.dropna()) >= 2 and _hist_positive(weekly_hist):
        score += 2
        flags.append('周K紅柱')

    # 日K MACD 即將翻紅 / 剛翻紅（+1）
    if len(daily_hist.dropna()) >= 3:
        if _hist_just_turned_positive(daily_hist):
            score += 1
            flags.append('日K剛翻紅')
        elif _hist_about_to_turn(daily_hist, n=2):
            score += 1
            flags.append('日K即將翻紅')

    # 60分K MACD 持續縮短向上（+1）
    if len(m60_hist.dropna()) >= 3 and _hist_shrinking_upward(m60_hist, n=3):
        score += 1
        flags.append('60m縮短向上')

    # 量縮（+1）
    vol_clean = volume.dropna()
    vol_ratio = np.nan
    avg_vol_5 = np.nan
    if len(vol_clean) >= 6:
        avg_vol_5 = float(vol_clean.iloc[-6:-1].mean())
        cur_vol   = float(vol_clean.iloc[-1])
        vol_ratio = cur_vol / avg_vol_5 if avg_vol_5 > 0 else np.nan
        if avg_vol_5 > 0 and cur_vol < avg_vol_5:
            score += 1
            flags.append('量縮')

    # 空方力竭：RSI 曾跌至低位後回升（+1）
    rsi_clean = rsi.dropna()
    rsi_now   = float(rsi_clean.iloc[-1]) if len(rsi_clean) > 0 else np.nan
    if len(rsi_clean) >= 10:
        rsi_low_10 = float(rsi_clean.iloc[-10:].min())
        if rsi_low_10 < RSI_EXHAUSTION_LOW and rsi_now > rsi_low_10 and rsi_now < RSI_EXHAUSTION_HIGH:
            score += 1
            flags.append('空方力竭')

    cur_close = float(close.iloc[-1])
    return {
        'symbol':  symbol,
        'name':    name,
        'score':   score,
        'close':   cur_close,
        'volume':  float(vol_clean.iloc[-1]) if len(vol_clean) > 0 else 0.0,
        'rsi':     round(rsi_now, 1) if not np.isnan(rsi_now) else 0.0,
        '量比':    round(vol_ratio, 2) if not np.isnan(vol_ratio) else 0.0,
        'signals': ' '.join(f'[{f}]' for f in flags),
        '進場參考': round(cur_close, 1),
        '目標價':   round(cur_close * TARGET_PCT, 1),
        '停損價':   round(cur_close * STOP_PCT, 1),
        # 內部統計旗標（scan_stocks 統計用，不對外輸出）
        '_月K轉正首根': '月K轉正首根' in flags,
        '_周K紅柱':     '周K紅柱'     in flags,
        '_日K信號':     any(f in flags for f in ('日K剛翻紅', '日K即將翻紅')),
        '_60m信號':     '60m縮短向上' in flags,
        '_量縮':        '量縮'        in flags,
        '_空方力竭':    '空方力竭'    in flags,
    }


# ──────────────────────────────────────────────────────────────
# 主篩選函式
# ──────────────────────────────────────────────────────────────

def scan_stocks(stock_data, data_dir=None):
    """
    多時間軸 MACD 空方力竭反轉篩選器。

    評分項目（滿分 9 分）：
        +2  月K MACD 柱體轉正第一根（空方力竭趨勢反轉最強信號）
        +1  月K MACD 柱體已是紅柱（次級月K加分）
        +2  周K MACD 為紅柱（中期趨勢確認）
        +1  日K MACD 柱體即將翻紅（縮短向上）或剛翻紅（進場時機）
        +1  60分K MACD 柱體持續縮短向上即將轉正（細緻進場時機）
        +1  量縮（成交量 < 5日均量，整理蓄勢）
        +1  空方力竭（RSI 曾跌至超賣後回升，空方動能衰竭）

    通過門檻：MIN_SCORE = 4
    輸入欄位：'symbol', 'date', 'close', 'volume'（可選 'name'）
    """
    stock_data = stock_data.copy()
    stock_data['close']  = pd.to_numeric(stock_data['close'],  errors='coerce')
    stock_data['volume'] = pd.to_numeric(stock_data['volume'], errors='coerce')
    stock_data = stock_data.dropna(subset=['close', 'volume'])
    stock_data = stock_data.sort_values(['symbol', 'date'])

    # ── 逐支計算多時間軸信號 ──────────────────────────────────
    records = []
    for symbol, group in stock_data.groupby('symbol'):
        res = _analyze_symbol(group, data_dir=data_dir)
        if res is not None:
            records.append(res)

    if not records:
        print("  [警告] 無有效個股資料，請確認資料長度是否足夠（需 ≥ 60 根日K）。")
        return pd.DataFrame()

    df_all = pd.DataFrame(records)
    total  = len(df_all)
    hi_score_count = int((df_all['score'] >= MIN_SCORE).sum())

    print(
        f"  [評分統計] 有效:{total} | "
        f"月K轉正首根:{int(df_all['_月K轉正首根'].sum())} | "
        f"周K紅柱:{int(df_all['_周K紅柱'].sum())} | "
        f"日K信號:{int(df_all['_日K信號'].sum())} | "
        f"量縮:{int(df_all['_量縮'].sum())} | "
        f"空方力竭:{int(df_all['_空方力竭'].sum())} | "
        f"分數≥{MIN_SCORE}:{hi_score_count}"
    )

    selected = df_all[df_all['score'] >= MIN_SCORE].copy()

    # 不足 MIN_RESULTS 時逐步降分補足
    threshold = MIN_SCORE - 1
    while len(selected) < MIN_RESULTS and threshold >= 2:
        extra = df_all[
            (df_all['score'] == threshold) &
            (~df_all['symbol'].isin(selected['symbol']))
        ]
        selected = pd.concat([selected, extra], ignore_index=True)
        threshold -= 1

    # 排序：分數高 → 量比大，取前 MAX_RESULTS
    result = selected.sort_values(['score', '量比'], ascending=False).head(MAX_RESULTS)

    # 輸出欄位（移除內部 _ 開頭旗標欄）
    out_cols = ['symbol', 'name', 'score', 'close', 'rsi', '量比',
                'signals', '進場參考', '目標價', '停損價']
    return result[[c for c in out_cols if c in result.columns]].reset_index(drop=True)

