import os
import re
import sqlite3
from datetime import date

import matplotlib.pyplot as plt
import pandas as pd


DB_PATH = os.path.join("data", "agri_prices.sqlite")
CHART_DIR = "charts"


def sanitize_filename(name: str) -> str:
    name = re.sub(r"[\\/:*?\"<>|]+", "_", name)
    return name.strip() or "chart"


def configure_plot_font() -> None:
    plt.rcParams["font.sans-serif"] = ["DejaVu Sans"]
    plt.rcParams["axes.unicode_minus"] = False


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
    # Based on typical reporting: most are CNY/kg, some are CNY/head
    per_head = {
        "商品代蛋雏鸡",
        "商品代肉雏鸡",
    }
    return "CNY/head" if item in per_head else "CNY/kg"


def english_item_name(item: str) -> str:
    mapping = {
        "生猪": "Live Hogs",
        "仔猪": "Piglets",
        "猪肉": "Pork",
        "鸡蛋": "Eggs",
        "鸡肉": "Chicken",
        "牛肉": "Beef",
        "羊肉": "Mutton",
        "玉米": "Corn",
        "豆粕": "Soybean Meal",
        "育肥猪配合饲料": "Fattening Pig Compound Feed",
        "肉鸡配合饲料": "Broiler Compound Feed",
        "蛋鸡配合饲料": "Layer Compound Feed",
        "商品代肉雏鸡": "Commercial Broiler Chicks",
        "商品代蛋雏鸡": "Commercial Layer Chicks",
        "主产省份生鲜乳": "Raw Milk in Major Producing Provinces",
        "主产省份活牛": "Live Cattle in Major Producing Provinces",
        "主产省份活羊": "Live Sheep in Major Producing Provinces",
        "主产省份鸡蛋": "Eggs in Major Producing Provinces",
    }
    return mapping.get(item, item)


def make_charts() -> None:
    configure_plot_font()
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
            item_en = english_item_name(item)
            plt.figure(figsize=(8, 4))
            plt.plot(g["week_start"], g["price_this_week"], marker="o", linewidth=1.5)
            plt.title(f"China Livestock and Feed Market Prices - {item_en}")
            plt.xlabel("ISO week start date")
            plt.ylabel(f"Price ({unit})")
            plt.grid(True, alpha=0.3)
            plt.figtext(
                0.99,
                0.01,
                "Source: China Agriculture and Rural Affairs Information Network",
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
