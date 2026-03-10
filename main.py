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
    
    # 確保欄位符合預期
    report_data = selected_stocks.copy()
    # 格式化輸出
    table = report_data.to_markdown(index=False, numalign="left", stralign="left")
    return f"### 🚀 今日強勢突破標的\n\n{table}"

def run_market_pipeline(market_id, market_name, emoji):
    print(f"\n{emoji} 啟動管線：{market_name}")
    
    # --- Step 1: 下載數據 ---
    # 觸發下載器更新最新 CSV
    if market_id == "tw-share":
        downloader_tw.main()
    
    # --- Step 2: 篩選與報告 ---
    print(f"🔍 正在篩選 {market_name} 強勢股...")
    
    # 讀取剛下載好的 CSV (請確保路徑正確)
    csv_path = f'data/{market_id}_latest.csv'
    if not os.path.exists(csv_path):
        print(f"❌ 找不到資料檔: {csv_path}")
        return

    stock_data = pd.read_csv(csv_path)
    selected = scan_stocks(stock_data)
    
    # --- Step 3: 生成並發送報告 ---
    md_content = generate_markdown_report(selected)
    
    agent = notifier.StockNotifier()
    # 使用我們在 notifier.py 中新增的 Markdown 發送方法
    success = agent.send_markdown_report(
        subject=f"【強勢股】{market_name} 觀察清單 - {datetime.now().strftime('%Y-%m-%d')}",
        markdown_content=md_content
    )
    
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
