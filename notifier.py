# -*- coding: utf-8 -*-
import os
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

    def send_markdown_report(self, subject, markdown_content):
        """
        🚀 專門用於發送 Markdown 表格報告的新方法
        """
        if not self.resend_api_key:
            print("⚠️ 缺少 Resend API Key，無法寄信。")
            return False

        # 將 Markdown 簡單轉為 HTML 格式以利 Email 呈現
        # 這裡將內容包裹在簡單的 HTML 容器中
        html_content = f"""
        <html>
        <body style="font-family: sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; padding: 20px; border: 1px solid #ddd; border-radius: 8px;">
                <h2 style="color: #2c3e50;">市場監控報告</h2>
                <div style="background: #f4f4f4; padding: 15px; border-radius: 5px;">
                    {markdown_content.replace('|', '</td><td>').replace('---', '').replace('###', '<h3>')}
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
