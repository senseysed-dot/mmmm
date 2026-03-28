# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np

# 最少回傳股票數（不足時自動降分填充）
MIN_RESULTS = 15
# 最多回傳股票數
MAX_RESULTS = 30
# 通過門檻（滿分 8 分，需達此分才列入強勢）
MIN_SCORE = 4

def _macd(close_series, fast=12, slow=26, signal=9):
    """計算 MACD Diff（MACD line − Signal line）"""
    ema_fast   = close_series.ewm(span=fast,   adjust=False).mean()
    ema_slow   = close_series.ewm(span=slow,   adjust=False).mean()
    macd_line  = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line - signal_line   # > 0 表示 MACD 偏多

def scan_stocks(stock_data):
    """
    多指標評分篩選器：每個看多訊號得 1 分，分數 ≥ MIN_SCORE 入選。
    若入選數量 < MIN_RESULTS，自動降低門檻補足至 MIN_RESULTS 檔（依分數排序）。

    評分項目（共 8 分）:
        1. close > MA20      （股價在趨勢線之上）
        2. MA20 > MA60       （中期多頭排列）
        3. MA5 > MA20        （短期動能領先）
        4. RSI > 50          （動能偏多）
        5. RSI > 60          （動能強勢，額外加分）
        6. MACD Diff > 0     （MACD 偏多）
        7. 成交量 > 5日均量   （量能支撐）
        8. 成交量 > 20日均量  （中期量能擴張）

    輸入欄位：'symbol', 'date', 'close', 'volume'（可選 'name'）
    """
    # ── 1. 型別確保 ────────────────────────────────────────
    stock_data = stock_data.copy()
    stock_data['close']  = pd.to_numeric(stock_data['close'],  errors='coerce')
    stock_data['volume'] = pd.to_numeric(stock_data['volume'], errors='coerce')
    stock_data = stock_data.sort_values(['symbol', 'date']).reset_index(drop=True)

    # ── 2. 計算指標（grouped transform 保留 symbol 對應）──────
    grp_close = stock_data.groupby('symbol')['close']
    grp_vol   = stock_data.groupby('symbol')['volume']

    stock_data['ma5']           = grp_close.transform(lambda x: x.rolling(5).mean())
    stock_data['ma20']          = grp_close.transform(lambda x: x.rolling(20).mean())
    stock_data['ma60']          = grp_close.transform(lambda x: x.rolling(60).mean())
    stock_data['avg_vol_5']     = grp_vol.transform(lambda x: x.rolling(5).mean())
    stock_data['avg_vol_20']    = grp_vol.transform(lambda x: x.rolling(20).mean())

    # RSI(14)
    delta = grp_close.transform('diff')
    gain  = delta.where(delta > 0, 0).groupby(stock_data['symbol']).transform(lambda x: x.rolling(14).mean())
    loss  = (-delta.where(delta < 0, 0)).groupby(stock_data['symbol']).transform(lambda x: x.rolling(14).mean())
    rs    = gain / loss.replace(0, np.nan)
    stock_data['rsi'] = 100 - (100 / (1 + rs))

    # MACD diff（每支股票獨立計算）
    macd_vals = (
        stock_data.groupby('symbol')['close']
        .transform(lambda x: _macd(x))
    )
    stock_data['macd_diff'] = macd_vals

    # ── 3. 取每支股票最新一日 ────────────────────────────────
    latest = stock_data.groupby('symbol').tail(1).reset_index(drop=True)

    # 最少需要 MA20、RSI 有效（MA60 不強制，短期上市股也能參與）
    latest = latest.dropna(subset=['ma20', 'rsi', 'avg_vol_5'])

    # ── 4. 計分 ──────────────────────────────────────────────
    s = latest
    latest = latest.copy()
    latest['score'] = (
        (s['close']  > s['ma20']                 ).astype(int)   # +1
      + (s['ma20']   > s['ma60'].fillna(0)        ).astype(int)   # +1（MA60 不足時以 0 代替）
      + (s['ma5']    > s['ma20']                  ).astype(int)   # +1
      + (s['rsi']    > 50                         ).astype(int)   # +1
      + (s['rsi']    > 60                         ).astype(int)   # +1（額外加分）
      + (s['macd_diff'].fillna(0) > 0             ).astype(int)   # +1
      + (s['volume'] > s['avg_vol_5']             ).astype(int)   # +1
      + (s['volume'] > s['avg_vol_20'].fillna(0)  ).astype(int)   # +1
    )

    # ── 5. 篩選並列印統計 ────────────────────────────────────
    total = len(latest)
    hi_score = (latest['score'] >= MIN_SCORE).sum()
    print(
        f"  [評分統計] 有效股票:{total} | "
        f"close>MA20:{(s['close']>s['ma20']).sum()} | "
        f"MA20>MA60:{(s['ma20']>s['ma60'].fillna(0)).sum()} | "
        f"MA5>MA20:{(s['ma5']>s['ma20']).sum()} | "
        f"RSI>50:{(s['rsi']>50).sum()} | "
        f"MACD+:{(s['macd_diff'].fillna(0)>0).sum()} | "
        f"分數≥{MIN_SCORE}:{hi_score}"
    )

    selected = latest[latest['score'] >= MIN_SCORE].copy()

    # 若結果不足 MIN_RESULTS，逐步降分補足
    threshold = MIN_SCORE - 1
    while len(selected) < MIN_RESULTS and threshold >= 2:
        extra = latest[(latest['score'] == threshold) & (~latest['symbol'].isin(selected['symbol']))]
        selected = pd.concat([selected, extra], ignore_index=True)
        threshold -= 1

    # ── 6. 計算建議進出場 ────────────────────────────────────
    selected['進場參考'] = selected['close'].round(1)
    selected['目標價']   = (selected['close'] * 1.05).round(1)   # +5%
    selected['停損價']   = (selected['close'] * 0.95).round(1)   # -5%
    selected['量比']     = (selected['volume'] / selected['avg_vol_5']).round(2)

    # ── 7. 排序：分數高 → 量比大，取前 MAX_RESULTS ───────────
    result = selected.sort_values(['score', '量比'], ascending=False).head(MAX_RESULTS)

    base_cols = ['symbol', 'score', 'close', 'ma5', 'ma20', 'rsi', '量比', '進場參考', '目標價', '停損價']
    if 'name' in result.columns:
        return result[['symbol', 'name', 'score'] + base_cols[2:]]
    return result[base_cols]

