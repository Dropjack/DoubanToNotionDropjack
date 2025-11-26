#!/usr/bin/env python3
"""
douban_notion_core.py

核心逻辑模块：用 ISBN 从豆瓣网页抓取图书信息，并写入 Notion 数据库。

这里不做命令行、不做 GUI，也不关心 Token/DBID 放在哪儿。
外部只需要调用：

    from douban_notion_core import run_import

    result = run_import(token, database_id, isbn)

返回值是一个 dict，里面包含：
    {
        "book": {...},        # 从豆瓣解析出来的原始数据
        "properties": {...},  # 即将写入 Notion 的 properties
        "page": {...},        # Notion /pages 返回的 JSON
    }
"""

import sys
import re
import time
from typing import Dict, Any, Optional, List

import requests
from bs4 import BeautifulSoup

# Notion 版本 & 基础 URL
NOTION_VERSION = "2022-06-28"
NOTION_BASE_URL = "https://api.notion.com/v1"

# 你在 Notion 里真正用到的 5 个字段
REQUIRED_PROPERTIES = ["书名", "出版社", "作者", "译者", "出版日期"]


# -------------------- 小工具函数 --------------------


def collapse_spaces(text: str) -> str:
    """
    将连续空白字符（空格、换行、tab 等）压缩成一个空格。
    比如：
        "[美]      丹·布朗" -> "[美] 丹·布朗"
    """
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


# -------------------- 豆瓣相关 --------------------


def fetch_book_from_douban(isbn: str) -> Dict[str, Any]:
    """
    用 ISBN 直接爬豆瓣图书页面，而不是走已经挂掉的 v2 API。

    目标页面：
        https://book.douban.com/isbn/{isbn}/

    返回结构大致是：
        {
            "title": "...",
            "publisher": "...",
            "author": ["..."],
            "translator": ["..."],
            "pubdate": "2018-8",   # 原始字符串，后面再转成 YYYY-MM-DD
        }
    """
    url = f"https://book.douban.com/isbn/{isbn}/"
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        ),
        "Referer": "https://book.douban.com/",
    }

    try:
        resp = requests.get(url, headers=headers, timeout=10)
    except requests.RequestException as e:
        raise RuntimeError(f"请求豆瓣网页时网络错误：{e}")

    if resp.status_code == 404:
        raise RuntimeError(f"豆瓣上找不到这个 ISBN 对应的图书页面：{url}")
    if resp.status_code != 200:
        raise RuntimeError(
            f"请求豆瓣网页失败，状态码 {resp.status_code}，"
            f"返回内容前 200 字符：{resp.text[:200]}"
        )

    soup = BeautifulSoup(resp.text, "html.parser")

    # 1. 标题
    title_tag = soup.find("span", attrs={"property": "v:itemreviewed"})
    if not title_tag:
        raise RuntimeError("在豆瓣页面中找不到书名标签（v:itemreviewed），页面结构可能变了。")
    title = collapse_spaces(title_tag.get_text(strip=True))

    # 2. 信息区
    info_div = soup.find("div", id="info")
    if not info_div:
        raise RuntimeError("在豆瓣页面中找不到 id='info' 的信息区，页面结构可能变了。")

    labels = info_div.find_all("span", class_="pl")

    publisher = ""
    pubdate_raw: Optional[str] = None
    authors: List[str] = []
    translators: List[str] = []
    pages: Optional[str] = None
    producer: Optional[str] = None   # 出品方 / 出品人
    binding: Optional[str] = None    # 封面类型


    for span in labels:
        label_text = span.get_text(strip=True)  # 例如："作者", "出版社:", "出版年:"
        label = label_text.rstrip("：:")        # 去掉结尾冒号，统一成 "作者"/"出版社"/"出版年"

        value_parts: List[str] = []
        for sibling in span.next_siblings:
            # 一直到 <br> 为止
            if getattr(sibling, "name", None) == "br":
                break
            if getattr(sibling, "name", None) == "a":
                text = sibling.get_text(strip=True)
                if text:
                    value_parts.append(text)
            else:
                text = str(sibling).strip()
                if text:
                    cleaned = text.replace("&nbsp;", " ").strip()
                    if cleaned and cleaned != "/":
                        value_parts.append(cleaned)

        raw_value = " ".join(value_parts).strip()
        raw_value = raw_value.lstrip(":：").strip()  # 开头可能再带一个冒号

        if not raw_value:
            continue

        if label.startswith("作者"):
            cleaned = raw_value.replace(" / ", "/")
            authors = [
                collapse_spaces(v)
                for v in cleaned.split("/")
                if v.strip()
            ]
        elif label.startswith("译者"):
            cleaned = raw_value.replace(" / ", "/")
            translators = [
                collapse_spaces(v)
                for v in cleaned.split("/")
                if v.strip()
            ]
        elif label.startswith("出版社"):
            publisher = collapse_spaces(raw_value)
        elif label.startswith("出版年"):
            pubdate_raw = raw_value
        elif label.startswith("出品方"):
            producer = collapse_spaces(raw_value)
        elif label.startswith("装帧"):
            binding = collapse_spaces(raw_value)
        elif label.startswith("页数"):
            # 页数通常是数字，有时可能带“页”
            pages_text = collapse_spaces(raw_value)
            pages = re.sub(r"\D+", "", pages_text)  # 提取纯数字

    book_data: Dict[str, Any] = {
        "title": title,
        "publisher": publisher,
        "author": authors,
        "translator": translators,
        "pubdate": pubdate_raw,
        "producer": producer,
        "binding": binding,
        "pages": pages,
    }

    return book_data


def convert_pubdate(raw_pubdate: Optional[str]) -> Optional[str]:
    """
    把豆瓣的 pubdate 转成 YYYY-MM-DD。

    你的规则：
    - 只有年份 => 补 01-01
    - 有年月 => 补 01 号
    - 不信任具体日子，就算豆瓣写了，我们也不太在意
    """
    if not raw_pubdate:
        return None

    raw = raw_pubdate.strip()
    parts = raw.split("-")

    try:
        year = int(parts[0])
    except (ValueError, IndexError):
        return None

    month = 1
    day = 1

    if len(parts) >= 2:
        try:
            month = int(parts[1])
        except ValueError:
            month = 1

    if len(parts) == 3:
        try:
            day = int(parts[2])
        except ValueError:
            day = 1

    month = max(1, min(month, 12))
    day = max(1, min(day, 31))

    return f"{year:04d}-{month:02d}-{day:02d}"


# -------------------- Notion 相关 --------------------


def notion_headers(token: str) -> Dict[str, str]:
    if not token:
        raise RuntimeError("Notion Token 为空。")
    return {
        "Authorization": f"Bearer {token}",
        "Notion-Version": NOTION_VERSION,
        "Content-Type": "application/json",
    }


def rich_text_property(text: Optional[str]) -> Dict[str, Any]:
    if not text:
        return {"rich_text": []}
    return {"rich_text": [{"text": {"content": text}}]}


def title_property(text: Optional[str]) -> Dict[str, Any]:
    if not text:
        text = ""
    return {"title": [{"text": {"content": text}}]}


def date_property(date_str: Optional[str]) -> Dict[str, Any]:
    if not date_str:
        return {"date": None}
    return {"date": {"start": date_str}}


def number_property(value: Optional[str]) -> Dict[str, Any]:
    """
    Notion 数字类型字段。
    """
    if not value:
        return {"number": None}
    try:
        return {"number": int(value)}
    except ValueError:
        return {"number": None}
        
        
def select_property(value: Optional[str]) -> Dict[str, Any]:
    """
    Notion 单选（select）类型字段。
    """
    if not value:
        return {"select": None}
    return {"select": {"name": value}}


def build_notion_properties(book: Dict[str, Any]) -> Dict[str, Any]:
    """
    根据豆瓣解析结果，构造 Notion properties。
    """
    title = book.get("title") or ""

    authors = book.get("author") or []
    translators = book.get("translator") or []

    author_str = collapse_spaces(", ".join(authors)) if authors else ""
    translator_str = collapse_spaces(", ".join(translators)) if translators else ""

    publisher = collapse_spaces(book.get("publisher") or "")
    pubdate_raw = book.get("pubdate")
    pubdate = convert_pubdate(pubdate_raw)

    # 页数字符串（可能为空）
    pages_raw = book.get("pages")
    pages_str = str(pages_raw).strip() if pages_raw is not None else ""

    properties = {
        "书名": title_property(title),
        "出版社": rich_text_property(publisher),
        "作者": rich_text_property(author_str),
        "译者": rich_text_property(translator_str),
        "出版日期": date_property(pubdate),

        # 下面三个字段名，必须和你 Notion 数据库里的列名完全一致
        "出品方": rich_text_property(book.get("producer") or ""),
        "封面类型": select_property(book.get("binding") or ""),
        "页数": number_property(pages_str),
    }

    return properties


def create_notion_page(
    token: str,
    database_id: str,
    properties: Dict[str, Any],
) -> Dict[str, Any]:
    """
    在指定数据库中新建一行，带一点超时 + 重试。
    """
    if not database_id:
        raise RuntimeError("Notion Database ID 为空。")

    url = f"{NOTION_BASE_URL}/pages"
    payload = {
        "parent": {"database_id": database_id},
        "properties": properties,
    }

    last_error = None
    for attempt in range(3):
        try:
            resp = requests.post(
                url,
                headers=notion_headers(token),
                json=payload,
                timeout=30,
            )
            break
        except requests.exceptions.ReadTimeout as e:
            last_error = e
            print(
                f"[WARN] 写入 Notion 超时（第 {attempt + 1} 次），准备重试……",
                file=sys.stderr,
            )
            time.sleep(2)
    else:
        raise RuntimeError(
            f"连续多次写入 Notion 超时，请检查网络。最后一次错误：{last_error}"
        )

    if resp.status_code != 200:
        raise RuntimeError(
            f"创建 Notion 页面失败，状态码 {resp.status_code}，返回：{resp.text[:500]}"
        )

    return resp.json()


# -------------------- 对外暴露的统一入口 --------------------


def run_import(token: str, database_id: str, isbn: str) -> Dict[str, Any]:
    """
    对外统一入口：给定 token / database_id / isbn，完成一次导入。

    返回：
        {
            "book": {...},        # 豆瓣解析结果
            "properties": {...},  # Notion properties
            "page": {...},        # Notion 返回的 JSON
        }
    """
    if not isbn:
        raise RuntimeError("ISBN 为空。")

    book = fetch_book_from_douban(isbn)
    properties = build_notion_properties(book)
    page = create_notion_page(token, database_id, properties)

    return {
        "book": book,
        "properties": properties,
        "page": page,
    }
