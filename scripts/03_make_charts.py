import os
import re
import sqlite3
from datetime import date

import matplotlib.pyplot as plt
from matplotlib import font_manager
import pandas as pd


DB_PATH = os.path.join("data", "agri_prices.sqlite")
CHART_DIR = "charts"


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    return name.strip() or "chart"


def configure_chinese_font() -> None:
    preferred = [
        "Microsoft YaHei",
        "SimHei",
        "Noto Sans CJK SC",
        "PingFang SC",
        "Source Han Sans SC",
    ]
    available = {f.name for f in font_manager.fontManager.ttflist}
    for name in preferred:
        if name in available:
            plt.rcParams["font.sans-serif"] = [name, "DejaVu Sans"]
            plt.rcParams["axes.unicode_minus"] = False
            return
    # Fallback: try first available CJK-like font name
    for name in sorted(available):
        if any(key in name for key in ["CJK", "Hei", "YaHei", "Han", "PingFang"]):
            plt.rcParams["font.sans-serif"] = [name, "DejaVu Sans"]
            plt.rcParams["axes.unicode_minus"] = False
            return


def load_data(conn: sqlite3.Connection) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT
            p.item,
            a.iso_year,
            a.iso_week,
            a.publish_date,
            p.price_this_week
        FROM prices p
        JOIN articles a ON a.id = p.article_id
        WHERE a.iso_year IS NOT NULL AND a.iso_week IS NOT NULL
        ORDER BY a.iso_year, a.iso_week
        """,
        conn,
    )
    return df


def iso_to_date(iso_year: int, iso_week: int) -> date:
    return date.fromisocalendar(int(iso_year), int(iso_week), 1)


def unit_for_item(item: str) -> str:
    # Based on typical reporting: most are 元/公斤, some are 元/只
    per_head = {
        "商品代蛋雏鸡",
        "商品代肉雏鸡",
    }
    return "元/只" if item in per_head else "元/公斤"


def make_charts() -> None:
    configure_chinese_font()
    os.makedirs(CHART_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        df = load_data(conn)
        if df.empty:
            print("No data to chart.")
            return

        df["week_start"] = df.apply(lambda r: iso_to_date(r["iso_year"], r["iso_week"]), axis=1)
        for item, g in df.groupby("item"):
            g = g.sort_values("week_start")
            unit = unit_for_item(item)
            plt.figure(figsize=(8, 4))
            plt.plot(g["week_start"], g["price_this_week"], marker="o", linewidth=1.5)
            plt.title(f"中国畜产品和饲料集贸市场价格 - {item}")
            plt.xlabel("ISO周（周一日期）")
            plt.ylabel(f"价格（{unit}）")
            plt.grid(True, alpha=0.3)
            plt.figtext(
                0.99,
                0.01,
                "数据来源：中国农业农村信息网",
                ha="right",
                va="bottom",
                fontsize=8,
                alpha=0.8,
            )
            fname = sanitize_filename(item)
            out_path = os.path.join(CHART_DIR, f"{fname}.png")
            plt.tight_layout()
            plt.savefig(out_path, dpi=150)
            plt.close()
        print(f"Charts saved to {CHART_DIR}")
    finally:
        conn.close()


if __name__ == "__main__":
    make_charts()
