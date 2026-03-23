import os
import re
import sqlite3
import argparse
from datetime import datetime, date
from io import StringIO
from typing import Iterable, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup


DB_PATH = os.path.join("data", "agri_prices.sqlite")
BASE_INDEX = "https://www.agri.cn/sj/jcyj/index{}.htm"
TARGET_KEYWORD = "畜产品和饲料集贸市场价格情况"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) PriceMonitorBot/1.0"


def init_db(db_path: str = DB_PATH) -> None:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT NOT NULL UNIQUE,
                title TEXT,
                publish_date TEXT,
                publish_datetime TEXT,
                iso_year INTEGER,
                iso_week INTEGER,
                week_label TEXT,
                total_week_in_year INTEGER,
                unit TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS prices (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER NOT NULL,
                item TEXT NOT NULL,
                price_this_week REAL,
                price_last_year REAL,
                price_prev_week REAL,
                yoy_pct REAL,
                wow_pct REAL,
                unit TEXT,
                UNIQUE(article_id, item),
                FOREIGN KEY(article_id) REFERENCES articles(id)
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def fetch_url(url: str) -> Optional[str]:
    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=20)
    except requests.RequestException:
        return None
    if resp.status_code != 200:
        return None
    resp.encoding = resp.apparent_encoding
    return resp.text


def iter_index_pages(max_pages: int = 50) -> Iterable[Tuple[str, str]]:
    for i in range(1, max_pages + 1):
        suffix = "" if i == 1 else f"_{i-1}"
        url = BASE_INDEX.format(suffix)
        html = fetch_url(url)
        if html is None:
            break
        yield url, html


def extract_target_links(index_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")
    links: List[str] = []
    for a in soup.find_all("a"):
        title = (a.get("title") or "").strip()
        text = a.get_text(strip=True)
        if TARGET_KEYWORD in title or TARGET_KEYWORD in text:
            href = a.get("href")
            if not href:
                continue
            full = urljoin(index_url, href)
            links.append(full)
    # dedupe while preserving order
    seen = set()
    result = []
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        result.append(link)
    return result


def parse_publish_date(soup: BeautifulSoup) -> Tuple[Optional[str], Optional[str]]:
    meta = soup.find("meta", attrs={"name": "publishdate"})
    if not meta:
        return None, None
    content = meta.get("content", "").strip()
    if not content:
        return None, None
    # content like "2026-03-10 09:38:00"
    dt_str = content
    date_str = content.split(" ")[0]
    return date_str, dt_str


def parse_title(soup: BeautifulSoup) -> Optional[str]:
    if soup.title and soup.title.get_text(strip=True):
        title = soup.title.get_text(strip=True)
        return title.replace("中国农业农村信息网_", "").strip()
    return None


def parse_week_label(title: Optional[str]) -> Optional[str]:
    if not title:
        return None
    m = re.search(r"(\d+月第\d+周)", title)
    return m.group(1) if m else None


def parse_total_week_from_table(df: pd.DataFrame) -> Optional[int]:
    try:
        cell = str(df.iloc[0, 0])
    except Exception:
        return None
    m = re.search(r"总第(\d+)周", cell)
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def parse_unit_from_table(df: pd.DataFrame) -> Optional[str]:
    try:
        cell = str(df.iloc[1, 0])
    except Exception:
        return None
    if "单位" in cell:
        return cell.replace("单位：", "").strip()
    return None


def to_float(value: str) -> Optional[float]:
    if value is None:
        return None
    s = str(value).strip()
    if s == "" or s in {"--", "—"}:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_table(html: str) -> Tuple[pd.DataFrame, Optional[str], Optional[int]]:
    dfs = pd.read_html(StringIO(html))
    if not dfs:
        raise ValueError("No table found")
    df = dfs[0]
    unit = parse_unit_from_table(df)
    total_week = parse_total_week_from_table(df)
    # header row is usually index 2
    header = df.iloc[2].tolist()
    df_body = df.iloc[3:].copy()
    df_body.columns = header
    return df_body, unit, total_week


def filter_price_rows(df_body: pd.DataFrame) -> pd.DataFrame:
    # Keep rows where "本周" is numeric
    def is_numeric(x: str) -> bool:
        return to_float(x) is not None

    mask = df_body["本周"].apply(is_numeric)
    return df_body[mask].copy()


def iso_week_from_date(date_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not date_str:
        return None, None
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return None, None
    iso = d.isocalendar()
    return int(iso.year), int(iso.week)


def upsert_article(conn: sqlite3.Connection, article: dict) -> int:
    conn.execute(
        """
        INSERT OR IGNORE INTO articles
        (url, title, publish_date, publish_datetime, iso_year, iso_week, week_label, total_week_in_year, unit)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            article["url"],
            article.get("title"),
            article.get("publish_date"),
            article.get("publish_datetime"),
            article.get("iso_year"),
            article.get("iso_week"),
            article.get("week_label"),
            article.get("total_week_in_year"),
            article.get("unit"),
        ),
    )
    conn.commit()
    cur = conn.execute("SELECT id FROM articles WHERE url = ?", (article["url"],))
    row = cur.fetchone()
    if not row:
        raise RuntimeError("Failed to fetch article id")
    return int(row[0])


def insert_prices(conn: sqlite3.Connection, article_id: int, unit: Optional[str], df_body: pd.DataFrame) -> int:
    inserted = 0
    for _, row in df_body.iterrows():
        item = str(row.get("项目", "")).strip()
        if not item:
            continue
        data = (
            article_id,
            item,
            to_float(row.get("本周")),
            to_float(row.get("上年同期")),
            to_float(row.get("前一周")),
            to_float(row.get("同比%")),
            to_float(row.get("环比%")),
            unit,
        )
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO prices
            (article_id, item, price_this_week, price_last_year, price_prev_week, yoy_pct, wow_pct, unit)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            data,
        )
        if cur.rowcount:
            inserted += cur.rowcount
    conn.commit()
    return inserted


def article_exists(conn: sqlite3.Connection, url: str) -> bool:
    cur = conn.execute("SELECT 1 FROM articles WHERE url = ? LIMIT 1", (url,))
    return cur.fetchone() is not None


def update(max_pages: int = 3) -> None:
    init_db(DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        new_articles = 0
        new_prices = 0
        links: List[str] = []
        page_count = 0
        print(f"Start update (max_pages={max_pages})")
        for index_url, html in iter_index_pages(max_pages=max_pages):
            page_count += 1
            print(f"[Index] page {page_count}: {index_url}")
            links.extend(extract_target_links(index_url, html))
        if page_count == 0:
            print("No index page fetched, exit.")
            return
        # dedupe while preserving order
        seen = set()
        links = [x for x in links if not (x in seen or seen.add(x))]

        total_links = len(links)
        if total_links == 0:
            print("No target links found, exit.")
            return

        for idx, url in enumerate(links, start=1):
            print(f"[Detail] {idx}/{total_links}: {url}")
            if article_exists(conn, url):
                print("  - already exists, skip")
                continue
            detail_html = fetch_url(url)
            if not detail_html:
                print("  - fetch failed, skip")
                continue
            soup = BeautifulSoup(detail_html, "lxml")
            title = parse_title(soup)
            publish_date, publish_dt = parse_publish_date(soup)
            iso_year, iso_week = iso_week_from_date(publish_date)
            week_label = parse_week_label(title)
            df_body, unit, total_week = parse_table(detail_html)
            df_body = filter_price_rows(df_body)
            article_id = upsert_article(
                conn,
                {
                    "url": url,
                    "title": title,
                    "publish_date": publish_date,
                    "publish_datetime": publish_dt,
                    "iso_year": iso_year,
                    "iso_week": iso_week,
                    "week_label": week_label,
                    "total_week_in_year": total_week,
                    "unit": unit,
                },
            )
            inserted = insert_prices(conn, article_id, unit, df_body)
            if inserted > 0:
                new_articles += 1
                new_prices += inserted
            print(f"  - inserted rows: {inserted}")

        print(f"Done. New articles: {new_articles}, new prices: {new_prices}")
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full",
        action="store_true",
        help="抓取更多索引页（默认只抓3页）",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="自定义抓取索引页数量（会覆盖--full默认值）",
    )
    args = parser.parse_args()
    if args.max_pages is not None:
        pages = args.max_pages
    else:
        pages = 50 if args.full else 3
    update(max_pages=pages)
