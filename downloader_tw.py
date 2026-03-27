# -*- coding: utf-8 -*-
import os
import time
import random
import requests
import pandas as pd
import yfinance as yf
from io import StringIO
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from pathlib import Path
from datetime import datetime

# ========== 核心參數設定 ==========
MARKET_CODE = "tw-share"
# 確保資料統一存放在 data/ 目錄下
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
MERGED_FILE = os.path.join(DATA_DIR, f"{MARKET_CODE}_latest.csv")

# TWSE HTML 表格欄位名稱
TWSE_CODE_COL = '有價證券代號'
TWSE_NAME_COL = '有價證券名稱'

# 效能參數
MAX_WORKERS = 3 
Path(DATA_DIR).mkdir(parents=True, exist_ok=True)

def log(msg: str):
    print(f"{pd.Timestamp.now():%H:%M:%S}: {msg}")

def merge_data():
    """將 data/ 下的所有個股 CSV 合併為一份供 main.py 讀取（保留完整歷史，供策略計算指標用）"""
    log("🔄 正在合併所有個股數據...")
    all_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv') and f != f"{MARKET_CODE}_latest.csv"]
    combined_data = []
    
    for f in all_files:
        try:
            df = pd.read_csv(os.path.join(DATA_DIR, f))
            if not df.empty:
                symbol = f.split('_')[0]
                df = df.copy()
                df['symbol'] = symbol
                combined_data.append(df)
        except Exception as e:
            log(f"⚠️ 讀取 {f} 失敗，略過: {e}")
    
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
        final_df.to_csv(MERGED_FILE, index=False, encoding='utf-8-sig')
        log(f"✅ 合併完成，產出檔案: {MERGED_FILE} (共 {len(final_df)} 行，{len(combined_data)} 檔個股)")
    else:
        log("⚠️ 未找到可合併的個股資料。")

def get_full_stock_list():
    """獲取台股全市場清單"""
    url_configs = [
        {'name': 'listed', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=1&issuetype=1&Page=1&chklike=Y', 'suffix': '.TW'},
        {'name': 'otc', 'url': 'https://isin.twse.com.tw/isin/class_main.jsp?market=2&issuetype=4&Page=1&chklike=Y', 'suffix': '.TWO'},
    ]
    CODE_COL = TWSE_CODE_COL
    NAME_COL = TWSE_NAME_COL
    all_items = []
    for cfg in url_configs:
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            resp = requests.get(cfg['url'], timeout=15, headers=headers)
            resp.raise_for_status()

            # 嘗試不同的 header 列設定以應對頁面結構差異
            df = None
            failed_attempts = []
            for h in [0, 1, None]:
                try:
                    tables = pd.read_html(StringIO(resp.text), header=h)
                    for t in tables:
                        if CODE_COL in t.columns and NAME_COL in t.columns:
                            df = t
                            break
                    if df is not None:
                        break
                    failed_attempts.append(f"header={h}: 未找到目標欄位")
                except Exception as parse_err:
                    failed_attempts.append(f"header={h}: {parse_err}")

            if df is None or df.empty:
                for msg in failed_attempts:
                    log(f"  [{cfg['name']}] {msg}")
                log(f"⚠️ [{cfg['name']}] 無法在回應中找到含 '{CODE_COL}' 欄位的表格，請確認 TWSE 頁面格式是否改變。")
                continue

            count = 0
            for _, row in df.iterrows():
                code = str(row[CODE_COL]).strip()
                name = str(row[NAME_COL]).strip()
                if code.isdigit() and len(code) == 4:
                    all_items.append(f"{code}{cfg['suffix']}&{name}")
                    count += 1
            log(f"✅ [{cfg['name']}] 取得 {count} 支股票")
        except Exception as e:
            log(f"❌ [{cfg['name']}] 取得股票清單失敗: {e}")

    if not all_items:
        log("❌ 嚴重警告：股票清單為空！所有來源均無法取得資料，下載將中止。")
    else:
        log(f"📋 股票清單總計: {len(all_items)} 支")
    return list(set(all_items))

def download_stock_data(item):
    """下載單檔股票"""
    try:
        yf_tkr, name = item.split('&', 1)
        safe_name = "".join([c for c in name if c.isalnum() or c in (' ', '_', '-')]).strip()
        out_path = os.path.join(DATA_DIR, f"{yf_tkr}_{safe_name}.csv")
        
        # 快取：今日已下載則跳過
        if os.path.exists(out_path):
            mtime = datetime.fromtimestamp(os.path.getmtime(out_path)).date()
            if mtime == datetime.now().date(): return {"status": "exists", "tkr": yf_tkr}

        time.sleep(random.uniform(0.5, 1.0))
        hist = yf.Ticker(yf_tkr).history(period="6mo", timeout=10) # MA60 需 60 個交易日；取 6 個月提供足夠緩衝
        if not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            # 統一日期格式為 YYYY-MM-DD (tz_localize(None) 保留本地日期，不轉換為 UTC)
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "tkr": yf_tkr}
        return {"status": "empty", "tkr": yf_tkr}
    except Exception as e:
        log(f"⚠️ 下載 {item.split('&')[0]} 失敗: {e}")
        return {"status": "error", "tkr": item.split('&')[0]}

def main():
    items = get_full_stock_list()
    log(f"🚀 啟動下載，目標總數: {len(items)}")
    
    stats = {"success": 0, "exists": 0, "empty": 0, "error": 0}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(download_stock_data, it): it for it in items}
        for future in tqdm(as_completed(futures), total=len(items)):
            stats[future.result()["status"]] += 1
            
    # 完成下載後執行合併
    merge_data()
    
    report = {"total": len(items), "success": stats["success"] + stats["exists"], "fail": stats["error"] + stats["empty"]}
    log(f"📊 下載任務完成: {report}")
    return report

if __name__ == "__main__":
    main()
