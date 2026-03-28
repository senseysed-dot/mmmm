# -*- coding: utf-8 -*-
import os
import re
import requests
import resend
from datetime import datetime, timedelta

class StockNotifier:
    def __init__(self):
        self.tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.resend_api_key = os.getenv("RESEND_API_KEY")
        self.receiver_email = os.getenv("REPORT_RECEIVER_EMAIL", "senseysed@gmail.com")
        
        if self.resend_api_key:
            resend.api_key = self.resend_api_key

    def get_now_time_str(self):
        now_utc8 = datetime.utcnow() + timedelta(hours=8)
        return now_utc8.strftime("%Y-%m-%d %H:%M")

    # ──────────────────────────────────────────────
    # Telegram（主要通知管道）
    # ──────────────────────────────────────────────

    def send_telegram(self, message):
        """發送 Telegram 訊息（HTML 格式）"""
        if not self.tg_token or not self.tg_chat_id:
            print("⚠️ Telegram 環境變數未設定（TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID）")
            return False
        
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {
            "chat_id": self.tg_chat_id,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        try:
            resp = requests.post(url, json=payload, timeout=10)
            resp.raise_for_status()
            return True
        except Exception as e:
            print(f"⚠️ Telegram 發送失敗: {e}")
            return False

    def notify_start(self, market_name="台股"):
        """程式啟動通知"""
        now = self.get_now_time_str()
        msg = (
            f"🚀 <b>{market_name}強勢股掃描系統啟動</b>\n"
            f"⏰ {now}\n"
            f"📡 每30分鐘掃描一次，直至收盤\n"
            f"────────────────"
        )
        return self.send_telegram(msg)

    def notify_end(self, market_name="台股"):
        """程式結束通知"""
        now = self.get_now_time_str()
        msg = (
            f"🏁 <b>{market_name}掃描系統已結束</b>\n"
            f"⏰ {now}\n"
            f"✅ 今日任務完成，明日見！"
        )
        return self.send_telegram(msg)

    def send_telegram_report(self, stocks_df, scan_time, market_name="台股"):
        """
        發送精簡的股票掃描結果至 Telegram。
        stocks_df：scan_stocks() 回傳的 DataFrame
        scan_time：掃描時間字串（如 "09:30"）
        """
        if stocks_df is None or stocks_df.empty:
            msg = (
                f"📭 <b>{market_name} {scan_time} 掃描結果</b>\n"
                f"目前無符合條件的強勢股"
            )
            return self.send_telegram(msg)

        lines = [
            f"📊 <b>{market_name}強勢掃描｜{scan_time}</b>",
            f"🔥 共發現 <b>{len(stocks_df)}</b> 支強勢股",
            "━━━━━━━━━━━━━━━━"
        ]

        for _, row in stocks_df.iterrows():
            sym = str(row.get('symbol', '')).split('.')[0]   # 去除 .TW / .TWO 後綴
            nm  = str(row.get('name', sym))
            close     = row.get('close', 0)
            rsi       = row.get('rsi', 0)
            vol_ratio = row.get('量比', 0)
            entry     = row.get('進場參考', close)
            target    = row.get('目標價', 0)
            stop      = row.get('停損價', 0)
            score     = int(row.get('score', 0)) if 'score' in row.index else None
            score_str = f"｜⭐{score}分" if score is not None else ""

            lines.append(
                f"📈 <b>{sym} {nm}</b>{score_str}\n"
                f"   💵 收:{close:.1f}｜RSI:{rsi:.1f}｜量比:{vol_ratio:.1f}x\n"
                f"   🎯 進:{entry:.1f}｜目標:{target:.1f}｜停損:{stop:.1f}"
            )
            signals = str(row.get('signals', ''))
            if signals:
                lines.append(f"   📌 {signals}")
            lines.append("────────────────")

        # Telegram 單則訊息上限 4096 字元，超過時分批發送
        full_msg = "\n".join(lines)
        if len(full_msg) <= 4096:
            return self.send_telegram(full_msg)

        # 分批發送（每批最多 10 筆）
        header = "\n".join(lines[:3])
        batch_lines = lines[3:]
        chunk_size = 16  # 每筆 4 行（內容3行+分隔線1行），每批最多 4 筆
        ok = self.send_telegram(header)
        for i in range(0, len(batch_lines), chunk_size):
            chunk = "\n".join(batch_lines[i:i + chunk_size])
            ok = self.send_telegram(chunk) and ok
        return ok

    # ──────────────────────────────────────────────
    # Email（備用通知，透過 Resend）
    # ──────────────────────────────────────────────

    def _markdown_to_html(self, markdown_content):
        """將 Markdown 文字（含表格）轉換為 HTML"""
        lines = markdown_content.split('\n')
        result = []
        in_table = False
        first_row = True

        for line in lines:
            stripped = line.strip()
            if stripped.startswith('### '):
                result.append(f'<h3>{stripped[4:]}</h3>')
            elif '|' in stripped:
                cells = [c.strip() for c in stripped.strip('|').split('|')]
                # Skip separator row (e.g., |:---|:---|)
                if all(re.match(r'^[-: ]+$', c) for c in cells if c):
                    continue
                if not in_table:
                    result.append('<table style="border-collapse:collapse;width:100%;font-size:14px;">')
                    in_table = True
                    first_row = True
                if first_row:
                    cells_html = ''.join(
                        f'<th style="padding:6px 12px;background:#2c3e50;color:white;text-align:left;">{c}</th>'
                        for c in cells
                    )
                    result.append(f'<tr>{cells_html}</tr>')
                    first_row = False
                else:
                    cells_html = ''.join(
                        f'<td style="padding:6px 12px;border-bottom:1px solid #ddd;">{c}</td>'
                        for c in cells
                    )
                    result.append(f'<tr>{cells_html}</tr>')
            else:
                if in_table:
                    result.append('</table>')
                    in_table = False
                if stripped:
                    result.append(f'<p>{stripped}</p>')

        if in_table:
            result.append('</table>')

        return '\n'.join(result)

    def send_markdown_report(self, subject, markdown_content):
        """發送 Markdown 表格報告（Email via Resend）"""
        if not self.resend_api_key:
            print("⚠️ 缺少 Resend API Key，無法寄信。")
            return False

        html_body = self._markdown_to_html(markdown_content)
        html_content = f"""
        <html>
        <body style="font-family: sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 720px; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">
                <h2 style="color: #2c3e50;">市場監控報告</h2>
                <div style="background: #f9f9f9; padding: 15px; border-radius: 5px;">
                    {html_body}
                </div>
                <p style="font-size: 12px; color: #888; margin-top: 20px;">
                    由自動化監控系統發送。
                </p>
            </div>
        </body>
        </html>
        """

        try:
            resend.Emails.send({
                "from": "StockMonitor <onboarding@resend.dev>",
                "to": self.receiver_email,
                "subject": subject,
                "html": html_content
            })
            print(f"✅ 報告已寄送至 {self.receiver_email}")
            return True
        except Exception as e:
            print(f"❌ 郵件寄送失敗: {e}")
            return False

