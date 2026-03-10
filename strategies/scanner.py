import pandas as pd

def scan_stocks(stock_data):
    """
    優化後的篩選邏輯：專注於多頭動能與量能突破
    
    篩選條件：
    1. 中長期趨勢：20日均線大於60日均線（確認多頭排列）
    2. 量能爆發：今日成交量大於近5日平均成交量的1.5倍（主力進場訊號）
    3. 動能強勁：RSI指標大於50（買方力道轉強）
    """
    
    # 執行篩選
    selected = stock_data[
        (stock_data['ma20'] > stock_data['ma60']) & 
        (stock_data['volume'] > (stock_data['avg_volume_5'] * 1.5)) &
        (stock_data['rsi'] > 50)
    ]
    
    # 回傳篩選結果，並按成交量排序 (量大優先)
    return selected.sort_values(by='volume', ascending=False)

print("篩選器已升級：已啟動多頭動能與量能突破篩選模式")
