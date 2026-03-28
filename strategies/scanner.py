# -*- coding: utf-8 -*-
import pandas as pd

def scan_stocks(stock_data):
    """
    強勢股篩選器：自動計算技術指標並篩選強勢股。
    輸入的 stock_data 必須包含 'symbol', 'date', 'close', 'volume' 欄位；
    可選包含 'name' 欄位（股票名稱）。
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

    stock_data['ma20']         = grp_close.transform(lambda x: x.rolling(20).mean())
    stock_data['ma60']         = grp_close.transform(lambda x: x.rolling(60).mean())
    stock_data['avg_volume_5'] = grp_vol.transform(lambda x: x.rolling(5).mean())

    # RSI 計算
    delta = grp_close.transform('diff')
    gain  = delta.where(delta > 0, 0).groupby(stock_data['symbol']).transform(lambda x: x.rolling(14).mean())
    loss  = (-delta.where(delta < 0, 0)).groupby(stock_data['symbol']).transform(lambda x: x.rolling(14).mean())
    rs = gain / loss
    stock_data['rsi'] = 100 - (100 / (1 + rs))

    # 4. 只取每支股票最新一天的數據進行篩選
    latest = stock_data.groupby('symbol').tail(1).reset_index(drop=True)

    # 先過濾掉指標為 NaN 的股票（歷史資料不足者）
    latest = latest.dropna(subset=['ma20', 'ma60', 'rsi', 'avg_volume_5'])

    total = len(latest)
    cond_ma    = latest['ma20'] > latest['ma60']
    cond_vol   = latest['volume'] > (latest['avg_volume_5'] * 1.2)   # 放量：超過5日均量 20%
    cond_rsi   = latest['rsi'] > 50                                    # RSI 偏多

    print(f"  [篩選統計] 有效股票: {total} 支 | MA多頭: {cond_ma.sum()} | 放量: {cond_vol.sum()} | RSI>50: {cond_rsi.sum()} | 全部通過: {(cond_ma & cond_vol & cond_rsi).sum()}")

    # 5. 執行篩選：MA多頭 + 放量 + RSI強勢
    selected = latest[cond_ma & cond_vol & cond_rsi].copy()

    # 6. 計算建議進出場價位
    selected['進場參考'] = selected['close'].round(1)
    selected['目標價']   = (selected['close'] * 1.05).round(1)   # +5%
    selected['停損價']   = (selected['close'] * 0.95).round(1)   # -5%
    # 成交量倍數（相對5日均量）
    selected['量比'] = (selected['volume'] / selected['avg_volume_5']).round(2)

    # 7. 回傳結果，優先顯示股名
    result = selected.sort_values(by='volume', ascending=False)

    base_cols = ['symbol', 'close', 'ma20', 'ma60', 'rsi', '量比', '進場參考', '目標價', '停損價']
    if 'name' in result.columns:
        return result[['symbol', 'name'] + base_cols[1:]]
    return result[base_cols]

