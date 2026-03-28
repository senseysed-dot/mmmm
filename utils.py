# -*- coding: utf-8 -*-
"""共用工具函式"""


def safe_filename(name: str) -> str:
    """將股票名稱轉為安全的檔名片段（去除特殊字元，保留英數、空白、底線、短橫線）"""
    return "".join(c for c in name if c.isalnum() or c in (' ', '_', '-')).strip()
