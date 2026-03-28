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
MIN_CAPITAL_BILLION = 20  # 資本額門檻（億元），低於此值的股票將被排除
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
                # 從檔名解析代號與名稱（格式：1101.TW_台泥.csv）
                stem = f.replace('.csv', '')
                parts = stem.split('_', 1)
                symbol = parts[0]
                name = parts[1] if len(parts) > 1 else parts[0]
                df = df.copy()
                df['symbol'] = symbol
                df['name'] = name
                combined_data.append(df)
        except: continue
    
    if combined_data:
        final_df = pd.concat(combined_data, ignore_index=True)
        final_df.to_csv(MERGED_FILE, index=False, encoding='utf-8-sig')
        log(f"✅ 合併完成，產出檔案: {MERGED_FILE} (共 {len(final_df)} 行，{len(combined_data)} 檔個股)")
    else:
        log("⚠️ 未找到可合併的個股資料。")

def get_capital_filter():
    """
    從 TWSE / TPEX 公開資料取得實收資本額 ≥ MIN_CAPITAL_BILLION 億的股票代號集合。
    回傳 set of str（純數字代碼，如 '2330'）；若兩個來源皆失敗則回傳 None（呼叫端跳過篩選）。
    """
    min_cap = MIN_CAPITAL_BILLION * 1e8  # 單位：元（億 → 元 需乘以 1e8）
    valid = set()

    # --- 上市公司 (TWSE opendata) ---
    try:
        r = requests.get(
            'https://opendata.twse.com.tw/v1/opendata/t187ap03_L',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=15
        )
        r.raise_for_status()
        for row in r.json():
            code = str(row.get('公司代號', '')).strip()
            # 欄位名稱可能含括號，嘗試多種寫法
            cap_raw = str(row.get('實收資本額(元)', row.get('實收資本額', '0'))).replace(',', '').strip()
            try:
                if code.isdigit() and float(cap_raw) >= min_cap:
                    valid.add(code)
            except Exception:
                pass
        log(f"📋 上市資本額篩選：符合 ≥{MIN_CAPITAL_BILLION}億 共 {len(valid)} 支")
    except Exception as e:
        log(f"⚠️ 上市資本額 API 失敗: {e}")

    # --- 上櫃公司 (TPEX openapi) ---
    otc_before = len(valid)
    try:
        r2 = requests.get(
            'https://www.tpex.org.tw/openapi/v1/tpex_mainboard_companies_profile',
            headers={'User-Agent': 'Mozilla/5.0'}, timeout=15
        )
        r2.raise_for_status()
        for row in r2.json():
            code = str(row.get('SecuritiesCompanyCode', row.get('股票代號', ''))).strip()
            cap_raw = str(row.get('PaidInCapital', row.get('實收資本額', '0'))).replace(',', '').strip()
            try:
                cap = float(cap_raw)
                # TPEX API 資本額單位為千元（若數值 < 1e7 則推斷為千元格式），需乘以 1000 換算為元
                if cap < 1e7:
                    cap *= 1000
                if code.isdigit() and cap >= min_cap:
                    valid.add(code)
            except Exception:
                pass
        log(f"📋 上櫃資本額篩選：符合 ≥{MIN_CAPITAL_BILLION}億 共 {len(valid) - otc_before} 支")
    except Exception as e:
        log(f"⚠️ 上櫃資本額 API 失敗: {e}")

    if not valid:
        log(f"⚠️ 資本額篩選資料無法取得，將納入全部股票")
        return None  # None 代表不篩選

    log(f"✅ 資本額篩選完成：共 {len(valid)} 支股票符合 ≥{MIN_CAPITAL_BILLION}億")
    return valid


def get_full_stock_list():
    """獲取台股全市場清單（已過濾資本額 < 20億的股票）"""
    # 先取得資本額篩選集合（純數字代碼）
    capital_filter = get_capital_filter()

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
            for h in [0, 1, None]:
                try:
                    tables = pd.read_html(StringIO(resp.text), header=h)
                    for t in tables:
                        if CODE_COL in t.columns and NAME_COL in t.columns:
                            df = t
                            break
                    if df is not None:
                        break
                    log(f"  [{cfg['name']}] header={h} 未找到目標欄位，嘗試下一種設定...")
                except Exception as parse_err:
                    log(f"  [{cfg['name']}] header={h} 解析失敗: {parse_err}，嘗試下一種設定...")

            if df is None or df.empty:
                log(f"⚠️ [{cfg['name']}] 無法在回應中找到含 '{CODE_COL}' 欄位的表格，請確認 TWSE 頁面格式是否改變。")
                continue

            count = 0
            skipped = 0
            for _, row in df.iterrows():
                code = str(row[CODE_COL]).strip()
                name = str(row[NAME_COL]).strip()
                if code.isdigit() and len(code) == 4:
                    # 資本額篩選：capital_filter 為 None 表示無法取得資料，不篩選
                    if capital_filter is not None and code not in capital_filter:
                        skipped += 1
                        continue
                    all_items.append(f"{code}{cfg['suffix']}&{name}")
                    count += 1
            log(f"✅ [{cfg['name']}] 取得 {count} 支股票（已排除資本額不足 {skipped} 支）")
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
        hist = yf.Ticker(yf_tkr).history(period="6mo", timeout=10) # 取 6 個月確保 MA60 有足夠資料
        if not hist.empty:
            hist.reset_index(inplace=True)
            hist.columns = [c.lower() for c in hist.columns]
            # 統一日期格式為 YYYY-MM-DD (去除時區資訊)
            hist['date'] = pd.to_datetime(hist['date']).dt.tz_localize(None).dt.strftime('%Y-%m-%d')
            hist.to_csv(out_path, index=False, encoding='utf-8-sig')
            return {"status": "success", "tkr": yf_tkr}
        return {"status": "empty", "tkr": yf_tkr}
    except: return {"status": "error", "tkr": yf_tkr}

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
