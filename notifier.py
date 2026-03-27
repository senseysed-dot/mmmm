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
        return now_utc8.strftime("%Y-%m-%d %H:%M:%S")

    def send_telegram(self, message):
        """發送 Telegram 即時通知"""
        if not self.tg_token or not self.tg_chat_id:
            return False
        
        url = f"https://api.telegram.org/bot{self.tg_token}/sendMessage"
        payload = {"chat_id": self.tg_chat_id, "text": message, "parse_mode": "HTML"}
        try:
            requests.post(url, json=payload, timeout=10)
            return True
        except Exception as e:
            print(f"⚠️ Telegram 發送失敗: {e}")
            return False

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
        """
        🚀 專門用於發送 Markdown 表格報告的新方法
        """
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
            
            # 同步發送 Telegram 通知
            self.send_telegram(f"📊 {subject} 已送達信箱。")
            return True
        except Exception as e:
            print(f"❌ 郵件寄送失敗: {e}")
            return False
