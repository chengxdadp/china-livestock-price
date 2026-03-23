import os
import sqlite3


DB_PATH = os.path.join("data", "agri_prices.sqlite")


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


if __name__ == "__main__":
    init_db()
    print(f"DB initialized at {DB_PATH}")
