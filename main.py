# -*- coding: utf-8 -*-
import os
import argparse
import traceback
from datetime import datetime, timedelta
import pandas as pd

# 導入自定義模組
import downloader_tw
# ... (其他下載器)
import notifier
from strategies.scanner import scan_stocks # 匯入新篩選器

def generate_markdown_report(selected_stocks):
    """將篩選結果轉為 Markdown 表格"""
    if selected_stocks.empty:
        return "### 🚀 今日無符合強勢突破條件的標的。"
    
    # 簡化欄位，僅顯示關鍵數據
    cols = ['stock_id', 'close', 'volume_ratio', 'rsi'] if 'volume_ratio' in selected_stocks.columns else ['stock_id', 'close', 'rsi']
    report_data = selected_stocks[cols].copy()
    report_data.columns = ['股票代號', '收盤價', '成交量倍數', 'RSI'][:len(cols)]
    
    table = report_data.to_markdown(index=False, numalign="left", stralign="left")
    return f"### 🚀 今日強勢突破標的\n\n{table}"

def run_market_pipeline(market_id, market_name, emoji):
    print(f"\n{emoji} 啟動管線：{market_name}")
    
    # --- Step 1: 下載數據 (假設已下載至 CSV 或記憶體中) ---
    # 這裡維持您原本的下載邏輯...
    # (省略下載程式碼以保持簡潔)

    # --- Step 2 & 3: 篩選與報告 ---
    if market_id == "tw-share":
        print("🔍 執行強勢股篩選...")
        # 假設 stock_data 是從 csv 讀取的 dataframe
        stock_data = pd.read_csv(f'data/{market_id}_latest.csv') 
        selected = scan_stocks(stock_data)
        
        # 生成 Markdown 表格
        md_content = generate_markdown_report(selected)
        
        # 發送郵件 (透過 notifier.py)
        agent = notifier.StockNotifier()
        agent.send_custom_email(
            subject=f"【監控報告】{market_name} 強勢股清單",
            body=md_content
        )
        print("✅ 報告已寄送")
    else:
        # 其他市場維持原本的矩陣分析流程
        pass

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--market', type=str, default='tw-share')
    args = parser.parse_args()
    
    # 市場配置
    markets_config = {"tw-share": {"name": "台灣股市", "emoji": "🇹🇼"}}
    
    m_info = markets_config.get(args.market)
    if m_info:
        run_market_pipeline(args.market, m_info["name"], m_info["emoji"])

if __name__ == "__main__":
    main()
