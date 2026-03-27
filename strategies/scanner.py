# -*- coding: utf-8 -*-
import pandas as pd

def scan_stocks(stock_data):
    """
    優化後的篩選器：自動計算技術指標並篩選強勢股
    注意：輸入的 stock_data 必須包含 'symbol', 'date', 'close', 'volume' 欄位
    """

    # 1. 確保數值型別正確，避免計算錯誤
    stock_data = stock_data.copy()
    stock_data['close'] = pd.to_numeric(stock_data['close'], errors='coerce')
    stock_data['volume'] = pd.to_numeric(stock_data['volume'], errors='coerce')

    # 2. 排序（groupby transform 需要按 symbol+date 排序才能正確計算 rolling window）
    stock_data = stock_data.sort_values(['symbol', 'date']).reset_index(drop=True)

    # 3. 使用 transform 計算各項指標（transform 不會丟失 symbol 欄位）
    grp_close = stock_data.groupby('symbol')['close']
    grp_vol   = stock_data.groupby('symbol')['volume']

    stock_data['ma20']        = grp_close.transform(lambda x: x.rolling(20).mean())
    stock_data['ma60']        = grp_close.transform(lambda x: x.rolling(60).mean())
    stock_data['avg_volume_5'] = grp_vol.transform(lambda x: x.rolling(5).mean())

    # RSI 計算
    delta = grp_close.transform('diff')
    gain  = delta.where(delta > 0, 0).groupby(stock_data['symbol']).transform(lambda x: x.rolling(14).mean())
    loss  = (-delta.where(delta < 0, 0)).groupby(stock_data['symbol']).transform(lambda x: x.rolling(14).mean())
    rs = gain / loss
    stock_data['rsi'] = 100 - (100 / (1 + rs))

    # 4. 只取每支股票最新一天的數據進行篩選（避免歷史舊資料干擾判斷）
    # 使用 tail(1) 確保取得實際最後一行，避免 last() 跨欄位混用不同日期的值
    latest = stock_data.groupby('symbol').tail(1).reset_index(drop=True)

    # 先過濾掉指標為 NaN 的股票（歷史資料不足者）
    latest = latest.dropna(subset=['ma20', 'ma60', 'rsi', 'avg_volume_5'])

    total = len(latest)
    cond_ma    = latest['ma20'] > latest['ma60']
    cond_vol   = latest['volume'] > (latest['avg_volume_5'] * 1.5)
    cond_rsi   = latest['rsi'] > 50

    print(f"  [篩選統計] 有效股票: {total} 支 | MA多頭: {cond_ma.sum()} | 放量: {cond_vol.sum()} | RSI>50: {cond_rsi.sum()} | 全部通過: {(cond_ma & cond_vol & cond_rsi).sum()}")

    # 5. 執行篩選：MA多頭 + 放量 + RSI強勢
    selected = latest[cond_ma & cond_vol & cond_rsi]

    # 6. 回傳精簡結果（僅回傳關鍵欄位以節省 Email 版面）
    result = selected.sort_values(by='volume', ascending=False)
    return result[['symbol', 'close', 'volume', 'ma20', 'ma60', 'rsi']]

