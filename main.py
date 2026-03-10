# -*- coding: utf-8 -*-
import os
import argparse
import pandas as pd
from datetime import datetime

# 導入自定義模組
import downloader_tw
import notifier
from strategies.scanner import scan_stocks

def generate_markdown_report(selected_stocks):
    """將篩選結果轉為簡潔的 Markdown 表格"""
    if selected_stocks.empty:
        return "### 🚀 今日無符合強勢突破條件的標的。"
    
    # 格式化輸出
    table = selected_stocks.to_markdown(index=False, numalign="left", stralign="left")
    return f"### 🚀 今日強勢突破標的\n\n{table}"

def run_market_pipeline(market_id, market_name, emoji):
    print(f"\n{emoji} 啟動管線：{market_name}")
    
    # --- 統一設定路徑 ---
    DATA_DIR = 'data'
    CSV_FILE = f'{market_id}_latest.csv'
    CSV_PATH = os.path.join(DATA_DIR, CSV_FILE)
    
    # --- Step 0: 環境檢查 ---
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"✅ 已自動建立 {DATA_DIR}/ 目錄")
    
    # --- Step 1: 下載數據 (downloader_tw 會自動合併產生 latest.csv) ---
    if market_id == "tw-share":
        downloader_tw.main()
    
    # --- Step 2: 檔案路徑與存在性驗證 ---
    print(f"🔍 準備讀取數據路徑: {CSV_PATH}")
    if not os.path.exists(CSV_PATH):
        print(f"❌ 嚴重錯誤：找不到必要的資料檔: {CSV_PATH}")
        print(f"DEBUG: 目前 {DATA_DIR} 目錄下的檔案列表: {os.listdir(DATA_DIR) if os.path.exists(DATA_DIR) else '目錄不存在'}")
        return

    # --- Step 3: 篩選與報告 ---
    print(f"🔍 正在篩選 {market_name} 強勢股...")
    stock_data = pd.read_csv(CSV_PATH)
    selected = scan_stocks(stock_data)
    
    # --- Step 4: 生成並發送報告 ---
    md_content = generate_markdown_report(selected)
    
    agent = notifier.StockNotifier()
    subject = f"【強勢股】{market_name} 觀察清單 - {datetime.now().strftime('%Y-%m-%d')}"
    success = agent.send_markdown_report(subject=subject, markdown_content=md_content)
    
    if success:
        print("✅ 報告已成功寄送至您的信箱。")
    else:
        print("❌ 報告寄送失敗。")

def main():
    parser = argparse.ArgumentParser(description="Stock Monitor Pipeline")
    parser.add_argument('--market', type=str, default='tw-share')
    args = parser.parse_args()
    
    markets_config = {
        "tw-share": {"name": "台灣股市", "emoji": "🇹🇼"}
    }
    
    m_info = markets_config.get(args.market)
    if m_info:
        run_market_pipeline(args.market, m_info["name"], m_info["emoji"])
    else:
        print(f"❌ 不支援的市場: {args.market}")

if __name__ == "__main__":
    main()
