# -*- coding: utf-8 -*-
import os
import time
import argparse
import pandas as pd
from datetime import datetime, timedelta

# 導入自定義模組
import downloader_tw
import notifier
from strategies.scanner import scan_stocks


# ──────────────────────────────────────────────
# 時間工具
# ──────────────────────────────────────────────

def get_taiwan_time():
    """回傳台灣時間（UTC+8）"""
    return datetime.utcnow() + timedelta(hours=8)

def is_market_open():
    """判斷台灣股市是否正在開盤中（週一至週五 09:00–13:30 台灣時間）"""
    now = get_taiwan_time()
    if now.weekday() >= 5:  # 週六/日
        return False
    open_t  = now.replace(hour=9,  minute=0,  second=0, microsecond=0)
    close_t = now.replace(hour=13, minute=30, second=0, microsecond=0)
    return open_t <= now <= close_t


# ──────────────────────────────────────────────
# 單次掃描邏輯
# ──────────────────────────────────────────────

def run_single_scan(market_id, market_name):
    """讀取已下載的 CSV 並執行策略篩選，回傳結果 DataFrame"""
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    CSV_PATH = os.path.join(DATA_DIR, f'{market_id}_latest.csv')

    if not os.path.exists(CSV_PATH):
        print(f"❌ 找不到資料檔: {CSV_PATH}")
        return None

    stock_data = pd.read_csv(CSV_PATH)
    unique_stocks = stock_data['symbol'].nunique() if 'symbol' in stock_data.columns else 0
    print(f"  載入 {len(stock_data)} 行資料，共 {unique_stocks} 支個股")

    selected = scan_stocks(stock_data, data_dir=DATA_DIR)
    print(f"  符合條件的強勢股: {len(selected)} 支")
    return selected


# ──────────────────────────────────────────────
# 完整流程（下載 + 首次掃描）
# ──────────────────────────────────────────────

def run_market_pipeline(market_id, market_name, emoji):
    """下載數據並執行一次掃描（供首次啟動使用）"""
    print(f"\n{emoji} 啟動管線：{market_name}")

    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_DIR = os.path.join(BASE_DIR, 'data')
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
        print(f"✅ 已自動建立 {DATA_DIR}/ 目錄")

    # Step 1：下載數據
    if market_id == "tw-share":
        downloader_tw.main()

    # Step 2：掃描
    return run_single_scan(market_id, market_name)


# ──────────────────────────────────────────────
# 盤中定時掃描迴圈
# ──────────────────────────────────────────────

def run_scheduled_loop(market_id, market_name, emoji):
    """
    開盤後每 30 分鐘執行一次掃描，直到收盤。
    - 資料只在第一次啟動時下載（快取當日），之後重複使用同一份 CSV。
    - 每次掃描結果直接推播至 Telegram。
    - 程式開始與結束各發送一則 Telegram 通知。
    """
    agent = notifier.StockNotifier()
    agent.notify_start(market_name)

    scan_count = 0

    # 首次啟動：下載數據
    print(f"\n{emoji} 首次啟動，開始下載 {market_name} 數據...")
    selected = run_market_pipeline(market_id, market_name, emoji)
    if selected is not None:
        scan_count += 1
        scan_time = get_taiwan_time().strftime("%H:%M")
        print(f"  第 {scan_count} 次掃描 ({scan_time})，強勢股: {len(selected)} 支")
        agent.send_telegram_report(selected, scan_time, market_name)

    # 持續迴圈：每 30 分鐘掃描一次
    while is_market_open():
        # 計算下次掃描時間與今日收盤時間（UTC），每分鐘喚醒一次確認是否繼續
        now_utc = datetime.utcnow()
        market_close_utc = now_utc.replace(
            hour=5, minute=30, second=0, microsecond=0  # 台灣 13:30 = UTC 05:30
        )
        wait_until = min(now_utc + timedelta(minutes=30), market_close_utc)
        while datetime.utcnow() < wait_until:
            time.sleep(60)

        if not is_market_open():
            break

        scan_count += 1
        scan_time = get_taiwan_time().strftime("%H:%M")
        print(f"\n⏰ 第 {scan_count} 次掃描 ({scan_time})")
        selected = run_single_scan(market_id, market_name)
        if selected is not None:
            agent.send_telegram_report(selected, scan_time, market_name)


    agent.notify_end(market_name)
    print(f"\n✅ 台股收盤，共執行 {scan_count} 次掃描。")


# ──────────────────────────────────────────────
# CLI 入口
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Stock Monitor Pipeline")
    parser.add_argument('--market', type=str, default='tw-share')
    parser.add_argument('--once', action='store_true',
                        help='只執行一次掃描後退出（預設為盤中持續掃描模式）')
    args = parser.parse_args()

    markets_config = {
        "tw-share": {"name": "台灣股市", "emoji": "🇹🇼"}
    }

    m_info = markets_config.get(args.market)
    if not m_info:
        print(f"❌ 不支援的市場: {args.market}")
        return

    market_id   = args.market
    market_name = m_info["name"]
    emoji       = m_info["emoji"]

    if args.once:
        # 單次執行模式（相容舊版行為）
        agent = notifier.StockNotifier()
        agent.notify_start(market_name)
        selected = run_market_pipeline(market_id, market_name, emoji)
        scan_time = get_taiwan_time().strftime("%H:%M")
        if selected is not None:
            agent.send_telegram_report(selected, scan_time, market_name)
        agent.notify_end(market_name)
    else:
        # 預設：盤中持續掃描模式
        run_scheduled_loop(market_id, market_name, emoji)


if __name__ == "__main__":
    main()
