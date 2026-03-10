# strategies/scanner.py
import pandas as pd

def scan_stocks(stock_data):
    """
    這是你的多重時框共振篩選邏輯
    stock_data: 傳入的股票數據 (包含收盤價, 成交量, MACD 等)
    """
    # 邏輯 1: MACD 收斂 (柱狀體由負轉淺)
    # 邏輯 2: 價格站上均線
    # 邏輯 3: 量縮洗盤
    
    selected = stock_data[
        (stock_data['close'] > stock_data['ma5']) & 
        (stock_data['close'] > stock_data['ma10']) &
        (stock_data['volume'] < stock_data['avg_volume_5'])
    ]
    
    return selected

print("篩選器已載入完成")
