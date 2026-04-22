"""匯出 strategy_signal.db 為 docs/signals.json，供 GitHub Pages 靜態面板讀取"""
import json
import os
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SIGNAL_DB = os.path.join(BASE_DIR, 'strategy_signal.db')
OUT_PATH = os.path.join(BASE_DIR, 'docs', 'signals.json')


def main():
    conn = sqlite3.connect(SIGNAL_DB)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT trading_date, close_price, vote1, vote2, vote3, vote4, "
        "total_votes, tech_score, target_position, position, "
        "daily_pnl, cumulative_pnl, updated_at "
        "FROM daily_signals ORDER BY trading_date"
    ).fetchall()
    conn.close()

    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    with open(OUT_PATH, 'w', encoding='utf-8') as f:
        json.dump([dict(r) for r in rows], f, ensure_ascii=False, separators=(',', ':'))
    print(f'[export] {len(rows)} rows -> {OUT_PATH}')


if __name__ == '__main__':
    main()
