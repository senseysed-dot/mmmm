# -*- coding: utf-8 -*-
import os
import time
import random
import pandas as pd
import yfinance as yf
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from datetime import datetime

# ========== 核心參數設定 ==========
MARKET_CODE = "tw-share"
# 將檔案直接存入 main.py 預期的 data/ 目錄下
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data") 
MERGED_FILE = os.path.join(DATA_DIR, f"{MARKET_CODE}_latest.csv")

MAX_WORKERS = 3 
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def merge_data():
    """將所有下載的個股資料合併為一份供篩選器使用"""
    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f != f"{MARKET_CODE}_latest.csv"]
    combined_data = []
    for f in all_files:
        try:
            df = pd.read_csv(os.path.join(DATA_DIR, f))
            # 加入代號欄位標識
            df['symbol'] = f.split('_')[0] 
            combined_data.append(df.tail(1)) # 取最新一筆
        except: continue
    
    if combined_data:
        final_df = pd.concat(combined_data)
        final_df.to_csv(MERGED_FILE, index=False)
        print(f"✅ 已合併 {len(combined_data)} 檔資料至 {MERGED_FILE}")

# ... (中間的 get_full_stock_list 與 download_stock_data 保持不變) ...

def main():
    items = get_full_stock_list()
    if not items: return {"total": 0, "success": 0, "fail": 0}
    
    # ... (執行 ThreadPoolExecutor 下載的部分保持不變) ...
    
    # 下載完成後，執行合併
    merge_data()
    
    report_stats = {
        "total": len(items),
        "success": stats["success"] + stats["exists"],
        "fail": stats["error"] + stats["empty"]
    }
    return report_stats

if __name__ == "__main__":
    main()
