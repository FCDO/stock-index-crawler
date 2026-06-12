# -*- coding: utf-8 -*-
"""
market_analogue.py - 市場狀態類比工具（描述性，非交易訊號）

═══ 定位聲明（誠實邊界）═══
本工具回答「今天的市場狀態像歷史上哪些時期？那些時期之後發生了什麼？」
產出為【樣本內敘述統計】：
  - 模式由 5 個狀態維度的組合定義（32 種），同一份資料既定義模式又統計事後報酬
    → 統計上不構成前瞻預測力證據（多重比較 + 選擇偏誤）
  - 波段在時間上群聚、前瞻視窗互相重疊 → 有效樣本小於表面 n
  - 一律附樣本數、95% 信賴區間、與無條件基準對照，供讀者自行折扣
  - 與部署路徑完全隔離：不接部位、不進 signal_ledger、不是 alpha 宣稱

═══ 狀態維度（事前固定；為描述覆蓋面而選，未對前瞻報酬掃描）═══
全部以換月調整連續序列（adjC）於 t 日收盤前資訊計算，前瞻視窗自 t+1 起：
  1. TREND: adjC > SMA200          長期趨勢（教科書慣用 200 日）
  2. MOM  : adjC > adjC[t-60]      中期動能（60 日 ≈ 一季）
  3. VOL  : 20日年化波動 > 其 252 日滾動中位數   波動 regime（中位切分，無門檻參數）
  4. HIGH : 距 252 日高點回檔 < 5%  價格位階（5% 為慣用「回檔」口語門檻）
  5. CHIP : 大額交易人特定法人淨部位 > 其 20 日均   籌碼（沿用既有 LT 定義）
有效樣本自 2004-08 起（受 LT 資料 2004-07 限制），約 5,300 個交易日。

報酬定義：日報酬 = 換月調整點數變化 / 前日收盤價；H 日前瞻報酬 = 日報酬加總。
波段（episode）= 同一模式的連續交易日；波段內報酬 = 起點收盤至末日收盤。
"""
import math
import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
HORIZONS = (5, 10, 20)
DIM_LABELS = [
    ('趨勢多 (>SMA200)', '趨勢空 (<SMA200)'),
    ('動能正 (60日)', '動能負 (60日)'),
    ('高波動', '低波動'),
    ('近高點 (<5%)', '回檔中 (>5%)'),
    ('大戶偏多', '大戶偏空'),
]


def wilson(k, n, z=1.96):
    if n == 0:
        return float('nan'), float('nan')
    p = k / n
    den = 1 + z * z / n
    ctr = (p + z * z / (2 * n)) / den
    hw = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return max(0.0, ctr - hw), min(1.0, ctr + hw)


def _hstats(fwd_list):
    """每個 horizon 的 n / 勝率(CI) / 平均報酬(±1.96SE)，輸入 dict H -> array"""
    out = []
    for H in HORIZONS:
        x = np.asarray(fwd_list[H], dtype=float)
        x = x[~np.isnan(x)]
        n = len(x)
        if n == 0:
            out.append(dict(h=H, n=0, win=None, lo=None, hi=None, mean=None, half=None))
            continue
        k = int((x > 0).sum())
        lo, hi = wilson(k, n)
        se = x.std(ddof=1) / math.sqrt(n) if n > 1 else float('nan')
        out.append(dict(h=H, n=n, win=k / n * 100, lo=lo * 100, hi=hi * 100,
                        mean=float(x.mean() * 100),
                        half=float(1.96 * se * 100) if n > 1 else None))
    return out


def compute():
    # ---- 載入 ----
    conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
    df = pd.read_sql_query("""
        WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
                   FROM tx_futures WHERE session='一般')
        SELECT trading_date, open_price, close_price, delivery_month FROM R WHERE rn=1
        ORDER BY trading_date""", conn)
    conn.close()
    conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
    lt = pd.read_sql_query(
        "SELECT date, t5_spec_long - t5_spec_short AS lt_net FROM large_trader_tx ORDER BY date", conn)
    conn.close()

    df['trading_date'] = pd.to_datetime(df['trading_date'])
    lt['date'] = pd.to_datetime(lt['date'])
    df = df.merge(lt, left_on='trading_date', right_on='date', how='left').drop(columns='date')
    df['lt_net'] = df['lt_net'].ffill(limit=5)

    n = len(df)
    o = df['open_price'].values.astype(float)
    c = df['close_price'].values.astype(float)
    dm = df['delivery_month'].values
    dates = df['trading_date'].dt.strftime('%Y-%m-%d').values

    roll_first = np.zeros(n, dtype=bool)
    roll_first[1:] = dm[1:] != dm[:-1]
    adj_ret = np.zeros(n)
    adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
    adjC = c[0] + np.cumsum(adj_ret)
    pct = np.zeros(n)
    pct[1:] = adj_ret[1:] / c[:-1]

    # ---- 5 個維度（t 日收盤資訊）----
    sma200 = pd.Series(adjC).rolling(200).mean().values
    mom60 = adjC - np.concatenate([np.full(60, np.nan), adjC[:-60]])
    vol20 = pd.Series(pct).rolling(20).std().values * math.sqrt(252)
    volmed = pd.Series(vol20).rolling(252).median().values
    hi252 = pd.Series(adjC).rolling(252).max().values
    dd = (adjC - hi252) / c
    ltv = df['lt_net'].values.astype(float)
    ltma = pd.Series(ltv).rolling(20).mean().values

    bits = np.stack([
        (adjC > sma200), (mom60 > 0), (vol20 > volmed), (dd > -0.05), (ltv > ltma),
    ]).astype(int)
    valid = ~(np.isnan(sma200) | np.isnan(mom60) | np.isnan(vol20)
              | np.isnan(volmed) | np.isnan(dd) | np.isnan(ltv) | np.isnan(ltma))
    pid = np.where(valid, (bits[0] * 16 + bits[1] * 8 + bits[2] * 4 + bits[3] * 2 + bits[4]), -1)

    # ---- 前瞻報酬 ----
    cums = np.concatenate([[0.0], np.cumsum(pct)])
    fwd = {}
    for H in HORIZONS:
        f = np.full(n, np.nan)
        f[:n - H] = cums[1 + H:] - cums[1:n - H + 1]
        fwd[H] = f

    # ---- 波段切分（同模式連續日；invalid 中斷）----
    episodes = []   # dict(pid, s, e, length)  s/e 為索引
    s = None
    for t in range(n):
        if s is None:
            if pid[t] >= 0:
                s = t
        elif pid[t] != pid[s]:
            episodes.append(dict(pid=int(pid[s]), s=s, e=t - 1, length=t - s))
            s = t if pid[t] >= 0 else None
    if s is not None:
        episodes.append(dict(pid=int(pid[s]), s=s, e=n - 1, length=n - s))

    cur_ep = episodes[-1] if episodes and episodes[-1]['e'] == n - 1 and pid[n - 1] >= 0 else None
    cur_pid = int(pid[n - 1]) if pid[n - 1] >= 0 else None
    run_len = cur_ep['length'] if cur_ep else 0

    # ---- 當前模式的歷史波段（排除進行中的這段，避免自我參照）----
    hist_eps = [e for e in episodes if e['pid'] == cur_pid and e is not cur_ep]
    onset_fwd = {H: [fwd[H][e['s']] for e in hist_eps] for H in HORIZONS}
    dayd_eps = [e for e in hist_eps if e['length'] >= run_len]
    dayd_fwd = {H: [fwd[H][e['s'] + run_len - 1] for e in dayd_eps] for H in HORIZONS}
    base_fwd = {H: fwd[H][valid] for H in HORIZONS}

    # ---- 波段表（最近 12 段 + 進行中）----
    def ep_row(e, ongoing=False):
        ep_ret = float((cums[e['e'] + 1] - cums[e['s'] + 1]) * 100)   # 起點收盤 -> 末日收盤
        row = dict(start=dates[e['s']], end=dates[e['e']], length=e['length'],
                   ep_ret=ep_ret, ongoing=ongoing)
        for H in HORIZONS:
            v = fwd[H][e['s']]
            row[f'fwd{H}'] = None if np.isnan(v) else float(v * 100)
        return row

    ep_rows = [ep_row(e) for e in hist_eps[-12:]]
    if cur_ep:
        ep_rows.append(ep_row(cur_ep, ongoing=True))

    # ---- 模式分布（脈絡用）----
    vd = int(valid.sum())
    dist = []
    for p in range(32):
        days = int((pid == p).sum())
        if days > 0:
            dist.append(dict(pid=p, desc=pattern_desc(p), days=days, freq=days / vd * 100))
    dist.sort(key=lambda x: -x['days'])

    cur_bits = [int(bits[k][n - 1]) for k in range(5)]
    factors = [
        dict(name='長期趨勢', state=DIM_LABELS[0][1 - cur_bits[0]], on=cur_bits[0],
             detail=f"收盤距 SMA200: {(adjC[n-1]-sma200[n-1])/c[n-1]*100:+.1f}%"),
        dict(name='中期動能', state=DIM_LABELS[1][1 - cur_bits[1]], on=cur_bits[1],
             detail=f"60 日變動: {mom60[n-1]/c[n-1]*100:+.1f}%"),
        dict(name='波動 regime', state=DIM_LABELS[2][1 - cur_bits[2]], on=cur_bits[2],
             detail=f"20日年化波動 {vol20[n-1]*100:.1f}% / 中位 {volmed[n-1]*100:.1f}%"),
        dict(name='價格位階', state=DIM_LABELS[3][1 - cur_bits[3]], on=cur_bits[3],
             detail=f"距 252 日高點: {dd[n-1]*100:+.1f}%"),
        dict(name='大額交易人', state=DIM_LABELS[4][1 - cur_bits[4]], on=cur_bits[4],
             detail=f"特定法人淨部位 {ltv[n-1]:+,.0f} 口 (20日均 {ltma[n-1]:+,.0f})"),
    ]

    return dict(
        meta=dict(as_of=dates[n - 1], computed_at=datetime.now().isoformat(timespec='seconds'),
                  valid_from=dates[np.argmax(valid)], valid_days=vd,
                  n_patterns=len(dist), horizons=list(HORIZONS)),
        current=dict(pattern_id=cur_pid, desc=pattern_desc(cur_pid), bits=cur_bits,
                     run_len=run_len, factors=factors,
                     freq=float((pid == cur_pid).sum() / vd * 100) if cur_pid is not None else None,
                     n_hist_episodes=len(hist_eps),
                     low_sample=len(hist_eps) < 8),
        stats=dict(onset=_hstats(onset_fwd), dayd=_hstats(dayd_fwd), dayd_n_eps=len(dayd_eps),
                   baseline=_hstats(base_fwd)),
        episodes=ep_rows,
        dist=dist[:8],
    )


def pattern_desc(p):
    if p is None or p < 0:
        return '--'
    b = [(p >> 4) & 1, (p >> 3) & 1, (p >> 2) & 1, (p >> 1) & 1, p & 1]
    return '｜'.join(DIM_LABELS[k][1 - b[k]] for k in range(5))


if __name__ == '__main__':
    r = compute()
    m, cu, st = r['meta'], r['current'], r['stats']
    print(f"[analogue] as of {m['as_of']}  valid {m['valid_from']}+ ({m['valid_days']} days)")
    print(f"[analogue] pattern #{cu['pattern_id']} run={cu['run_len']}d "
          f"freq={cu['freq']:.1f}% hist_episodes={cu['n_hist_episodes']}")
    for blk, lbl in ((st['onset'], 'onset'), (st['dayd'], f"day{cu['run_len']}"), (st['baseline'], 'base')):
        line = "  ".join(
            f"H{s['h']}: n={s['n']}" + (f" win={s['win']:.0f}%[{s['lo']:.0f},{s['hi']:.0f}] mean={s['mean']:+.2f}%"
                                        if s['n'] else "")
            for s in blk)
        print(f"[analogue] {lbl:<6} {line}")
