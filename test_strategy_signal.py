"""
Tests for strategy_signal.py
Uses temporary SQLite databases with synthetic price data.
"""
import sqlite3
import os
import tempfile
import numpy as np
import pandas as pd
import pytest

# --------------- Fixtures ---------------

def _trading_dates(start, end):
    """Generate weekday-only dates (simulating trading days)."""
    dates = pd.bdate_range(start, end)
    return dates


def _random_walk_prices(n, base=10000, seed=42):
    """Generate synthetic OHLCV via random walk."""
    rng = np.random.RandomState(seed)
    close = base + np.cumsum(rng.randint(-100, 101, size=n))
    opens = close + rng.randint(-50, 51, size=n)
    highs = np.maximum(opens, close) + rng.randint(0, 80, size=n)
    lows = np.minimum(opens, close) - rng.randint(0, 80, size=n)
    volumes = rng.randint(50000, 200000, size=n)
    return opens, highs, lows, close, volumes


@pytest.fixture
def tmp_dbs(tmp_path):
    """Create temporary tx_futures.db and stock_index.db with synthetic data."""
    # Need enough data before 2020-01-01 for rolling(90) warm-up,
    # plus data after 2020-01-01 for actual signal computation.
    dates = _trading_dates('2019-01-01', '2020-06-30')
    n = len(dates)
    opens, highs, lows, closes, volumes = _random_walk_prices(n)

    # ---- tx_futures.db ----
    tx_path = str(tmp_path / 'tx_futures.db')
    conn_tx = sqlite3.connect(tx_path)
    conn_tx.execute("""
        CREATE TABLE tx_futures (
            trading_date TEXT,
            contract TEXT,
            delivery_month TEXT,
            open_price INTEGER,
            high_price INTEGER,
            low_price INTEGER,
            close_price INTEGER,
            price_change INTEGER,
            change_percent REAL,
            volume INTEGER,
            settlement_price TEXT,
            open_interest TEXT,
            session TEXT,
            PRIMARY KEY (trading_date, contract, delivery_month, session)
        )
    """)
    for i, d in enumerate(dates):
        date_str = d.strftime('%Y/%m/%d')
        dm = d.strftime('%Y%m')
        conn_tx.execute(
            "INSERT INTO tx_futures VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (date_str, 'TX', dm, int(opens[i]), int(highs[i]), int(lows[i]),
             int(closes[i]), 0, '0.00%', int(volumes[i]), '-', '-', '一般')
        )
    conn_tx.commit()
    conn_tx.close()

    # ---- stock_index.db (tpex_index) ----
    idx_path = str(tmp_path / 'stock_index.db')
    conn_idx = sqlite3.connect(idx_path)
    conn_idx.execute("""
        CREATE TABLE tpex_index (
            date TEXT PRIMARY KEY,
            open REAL, high REAL, low REAL, close REAL,
            trading_volume REAL, trading_value REAL,
            transactions INTEGER, updated_at TEXT
        )
    """)
    tpex_base = 150.0
    rng = np.random.RandomState(99)
    tpex_closes = tpex_base + np.cumsum(rng.uniform(-2, 2, size=n))
    for i, d in enumerate(dates):
        date_str = d.strftime('%Y-%m-%d')
        c = round(float(tpex_closes[i]), 2)
        conn_idx.execute(
            "INSERT INTO tpex_index VALUES (?,?,?,?,?,?,?,?,?)",
            (date_str, c - 1, c + 1, c - 2, c, 1e9, 1e10, 50000, '2020-01-01T00:00:00')
        )
    conn_idx.commit()
    conn_idx.close()

    return tx_path, idx_path


@pytest.fixture
def patched_signal(tmp_dbs, monkeypatch):
    """Import strategy_signal with DB paths pointing to temp files."""
    tx_path, idx_path = tmp_dbs
    import strategy_signal as mod
    monkeypatch.setattr(mod, 'TX_DB', tx_path)
    monkeypatch.setattr(mod, 'INDEX_DB', idx_path)
    return mod


# --------------- Tests ---------------

class TestComputeSignals:
    """Tests for compute_signals()."""

    def test_returns_dataframe(self, patched_signal):
        result = patched_signal.compute_signals()
        assert isinstance(result, pd.DataFrame)
        assert len(result) > 0

    def test_required_columns(self, patched_signal):
        result = patched_signal.compute_signals()
        expected_cols = [
            'trading_date', 'close_price',
            'vote1', 'vote2', 'vote3', 'vote4',
            'total_votes', 'tech_score', 'target_position',
            'position', 'daily_pnl', 'cumulative_pnl', 'updated_at',
        ]
        for col in expected_cols:
            assert col in result.columns, f"Missing column: {col}"

    def test_votes_in_valid_range(self, patched_signal):
        result = patched_signal.compute_signals()
        for v in ['vote1', 'vote2', 'vote3', 'vote4']:
            assert set(result[v].unique()).issubset({-1, 0, 1}), f"{v} out of range"

    def test_total_votes_range(self, patched_signal):
        result = patched_signal.compute_signals()
        assert result['total_votes'].min() >= -4
        assert result['total_votes'].max() <= 4

    def test_total_votes_equals_sum(self, patched_signal):
        result = patched_signal.compute_signals()
        computed_sum = result['vote1'] + result['vote2'] + result['vote3'] + result['vote4']
        pd.testing.assert_series_equal(
            result['total_votes'].astype(int),
            computed_sum.astype(int),
            check_names=False,
        )

    def test_target_position_logic(self, patched_signal):
        """target_position: >=2 → 1, <=-2 → -1, else 0."""
        result = patched_signal.compute_signals()
        for _, row in result.iterrows():
            tv = row['total_votes']
            tp = row['target_position']
            if tv >= 2:
                assert tp == 1, f"votes={tv} should give target=1, got {tp}"
            elif tv <= -2:
                assert tp == -1, f"votes={tv} should give target=-1, got {tp}"
            else:
                assert tp == 0, f"votes={tv} should give target=0, got {tp}"

    def test_dates_after_2020(self, patched_signal):
        """All output dates should be >= 2020-01-01."""
        result = patched_signal.compute_signals()
        min_date = result['trading_date'].min()
        assert min_date >= '2020-01-01', f"Got date {min_date} before 2020"

    def test_cumulative_pnl_is_cumsum(self, patched_signal):
        result = patched_signal.compute_signals()
        expected = result['daily_pnl'].cumsum()
        np.testing.assert_allclose(
            result['cumulative_pnl'].values, expected.values, atol=1e-6
        )

    def test_position_values(self, patched_signal):
        result = patched_signal.compute_signals()
        assert set(result['position'].unique()).issubset({-1, 0, 1})

    def test_tech_score_range(self, patched_signal):
        result = patched_signal.compute_signals()
        assert result['tech_score'].min() >= 0
        assert result['tech_score'].max() <= 2


class TestSaveToDb:
    """Tests for save_to_db()."""

    def test_creates_daily_signals_table(self, patched_signal, tmp_path):
        result = patched_signal.compute_signals()
        db_path = str(tmp_path / 'test_signal.db')
        patched_signal.SIGNAL_DB = db_path

        patched_signal.save_to_db(result)

        conn = sqlite3.connect(db_path)
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        assert 'daily_signals' in tables
        assert 'update_log' in tables
        conn.close()

    def test_row_count_matches(self, patched_signal, tmp_path):
        result = patched_signal.compute_signals()
        db_path = str(tmp_path / 'test_signal.db')
        patched_signal.SIGNAL_DB = db_path

        patched_signal.save_to_db(result)

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM daily_signals").fetchone()[0]
        assert count == len(result)
        conn.close()

    def test_update_log_recorded(self, patched_signal, tmp_path):
        result = patched_signal.compute_signals()
        db_path = str(tmp_path / 'test_signal.db')
        patched_signal.SIGNAL_DB = db_path

        patched_signal.save_to_db(result)

        conn = sqlite3.connect(db_path)
        log = conn.execute("SELECT * FROM update_log").fetchall()
        assert len(log) == 1
        assert log[0][2] == len(result)  # rows column
        conn.close()

    def test_replace_on_second_save(self, patched_signal, tmp_path):
        """Calling save_to_db twice should replace data, not duplicate."""
        result = patched_signal.compute_signals()
        db_path = str(tmp_path / 'test_signal.db')
        patched_signal.SIGNAL_DB = db_path

        patched_signal.save_to_db(result)
        patched_signal.save_to_db(result)

        conn = sqlite3.connect(db_path)
        count = conn.execute("SELECT COUNT(*) FROM daily_signals").fetchone()[0]
        assert count == len(result)  # replaced, not appended
        log_count = conn.execute("SELECT COUNT(*) FROM update_log").fetchone()[0]
        assert log_count == 2  # log keeps both entries
        conn.close()
