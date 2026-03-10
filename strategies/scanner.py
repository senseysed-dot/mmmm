# -*- coding: utf-8 -*-
import pandas as pd

def scan_stocks(stock_data):
    """
    優化後的篩選器：自動計算技術指標並篩選強勢股
    注意：輸入的 stock_data 必須包含 'symbol', 'close', 'volume' 欄位
    """
    
    # 1. 確保數值型別正確，避免計算錯誤
    stock_data['close'] = pd.to_numeric(stock_data['close'], errors='coerce')
    stock_data['volume'] = pd.to_numeric(stock_data['volume'], errors='coerce')

    # 2. 按股票代號分組計算指標 (這對合併後的數據至關重要)
    def calculate_indicators(group):
        group = group.sort_values('date')
        group['ma20'] = group['close'].rolling(window=20).mean()
        group['ma60'] = group['close'].rolling(window=60).mean()
        group['avg_volume_5'] = group['volume'].rolling(window=5).mean()
        
        # RSI 計算
        delta = group['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        group['rsi'] = 100 - (100 / (1 + rs))
        return group

    stock_data = stock_data.groupby('symbol', group_keys=False).apply(calculate_indicators)

    # 3. 執行篩選
    # 篩選掉計算指標產生的 NaN 值，並執行你的策略
    selected = stock_data[
        (stock_data['ma20'] > stock_data['ma60']) & 
        (stock_data['volume'] > (stock_data['avg_volume_5'] * 1.5)) &
        (stock_data['rsi'] > 50)
    ]
    
    # 4. 回傳精簡結果 (僅回傳關鍵欄位以節省 Email 版面)
    result = selected.sort_values(by='volume', ascending=False)
    return result[['symbol', 'close', 'volume', 'ma20', 'ma60', 'rsi']]

print("篩選器已升級：已自動啟動技術指標計算與強勢股篩選模式")
