# -*- coding: utf-8 -*-
"""
market_analogue.py - 市場狀態類比工具（描述性，非交易訊號）

═══ 定位聲明（誠實邊界）═══
本工具回答「今天的市場狀態像歷史上哪些時期？那些時期之後發生了什麼？」
產出為【樣本內敘述統計】：
  - 同一份資料既定義狀態又統計事後報酬 → 不構成前瞻預測力證據
  - 波段/類比日在時間上群聚、前瞻視窗互相重疊 → 有效樣本小於表面 n
  - 一律附樣本數、95% 信賴區間、與無條件基準對照，供讀者自行折扣
  - 與部署路徑完全隔離：不接部位、不進 signal_ledger、不是 alpha 宣稱

═══ 13 個狀態維度（事前固定；為描述覆蓋面而選，未對前瞻報酬掃描）═══
全部以 t 日收盤前可得資訊計算，前瞻視窗自 t+1 起。★ = 粗分類模式核心維度。
價格（TX 換月調整連續序列 adjC）:
  ★ 1. TREND   adjC vs SMA200                     長期趨勢
  ★ 2. MOM60   60 日變動                           中期動能
    3. MOM20   20 日變動                           短期動能
  ★ 4. VOLR    20日年化波動 vs 其 252 日中位數      波動水準
    5. VOLD    20日波動 vs 其 60 日均               波動方向
  ★ 6. HIGH    距 252 日高點回檔 5% 內/外           價格位階
    7. RSI14   Wilder RSI 14                       擺盪位置
籌碼（institutional.db）:
  ★ 8. CHIP    大額交易人特定法人淨部位 vs 20日均    主力籌碼（2004-07+）
    9. FSPOT   外資現貨買賣超 20 日合計之 252 日 z   外資動向（2007+）
   10. MARGIN  融資餘額 20 日增減之 252 日 z         散戶槓桿（2001+）
   11. PCOI    TXO P/C 未平倉比 vs 252 日中位        選擇權偏態（2008+ 含暖身）
外部/跨市場:
   12. SPXTR   S&P500 vs 其 SMA200（asof 對齊 U<=D-1）美股趨勢
   13. RS20    櫃買 20 日報酬 - 加權 20 日報酬        風險偏好

═══ 雙引擎 ═══
A. 粗分類模式：5 個核心維度 → 32 模式（離散維度多了樣本密度指數級下降，
   故僅用核心 5 維），波段切分與事後統計。有效樣本 2004-08 起。
B. 全因子相似日：13 維連續值全樣本 z 分數、等權歐氏距離（權重不調），
   取最相似 30 天，相鄰類比日至少間隔 10 個交易日（去群聚），
   候選需有完整 20 日前瞻視窗。有效樣本 2008 起（受 P/C 暖身限制）。
   z 正規化以「截至今日」全樣本計算 — 本工具只查詢當下，對查詢日即為 PIT。

報酬定義：日報酬 = 換月調整點數變化 / 前日收盤價；H 日前瞻報酬 = 日報酬加總。
"""
import math
import os
import sqlite3
from datetime import datetime

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
HORIZONS = (5, 10, 20)
N_SIMILAR = 30      # 相似日數量（事前固定）
MIN_GAP = 10        # 類比日最小間隔（交易日，事前固定）
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


def _roll(x, w, fn):
    return getattr(pd.Series(x).rolling(w), fn)().values


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
    fs = pd.read_sql_query("SELECT date, foreign_net FROM spot_flows ORDER BY date", conn)
    mg = pd.read_sql_query("SELECT date, margin_lots FROM margin_totals ORDER BY date", conn)
    pc = pd.read_sql_query("SELECT date, pc_oi_ratio FROM pc_ratio ORDER BY date", conn)
    conn.close()
    conn = sqlite3.connect(os.path.join(BASE, 'stock_index.db'))
    tw = pd.read_sql_query("SELECT date, close AS tw_close FROM twse_index ORDER BY date", conn)
    tp = pd.read_sql_query("SELECT date, close AS tp_close FROM tpex_index ORDER BY date", conn)
    conn.close()
    spx = pd.read_sql_query("SELECT date, close AS spx_close FROM spx_daily ORDER BY date",
                            sqlite3.connect(os.path.join(BASE, 'us_market.db')))

    df['trading_date'] = pd.to_datetime(df['trading_date'])
    for d in (lt, fs, mg, pc, tw, tp, spx):
        d['date'] = pd.to_datetime(d['date'])

    # SPX：美股日曆上先算趨勢，再 asof 對齊（U 日資料自 U+1 可用）
    spx['spx_sma200'] = spx['spx_close'].rolling(200).mean()
    spx['spx_gap'] = (spx['spx_close'] - spx['spx_sma200']) / spx['spx_close']
    spx['avail_from'] = spx['date'] + pd.Timedelta(days=1)
    df = pd.merge_asof(df.sort_values('trading_date'),
                       spx[['avail_from', 'spx_gap']].sort_values('avail_from'),
                       left_on='trading_date', right_on='avail_from',
                       direction='backward').drop(columns='avail_from')

    for d in (lt, fs, mg, pc, tw, tp):
        df = df.merge(d, left_on='trading_date', right_on='date', how='left').drop(columns='date')
    for col in ('lt_net', 'foreign_net', 'margin_lots', 'pc_oi_ratio', 'tw_close', 'tp_close'):
        df[col] = df[col].ffill(limit=5)

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

    # ---- 13 維因子（連續值；t 日收盤資訊）----
    sma200 = _roll(adjC, 200, 'mean')
    f_trend = (adjC - sma200) / c                                       # 1 ★
    f_mom60 = (adjC - np.concatenate([np.full(60, np.nan), adjC[:-60]])) / c   # 2 ★
    f_mom20 = (adjC - np.concatenate([np.full(20, np.nan), adjC[:-20]])) / c   # 3
    vol20 = _roll(pct, 20, 'std') * math.sqrt(252)
    volmed = _roll(vol20, 252, 'median')
    f_volr = vol20 - volmed                                             # 4 ★
    f_vold = vol20 - _roll(vol20, 60, 'mean')                           # 5
    hi252 = _roll(adjC, 252, 'max')
    f_dd = (adjC - hi252) / c                                           # 6 ★
    delta = np.diff(adjC, prepend=adjC[0])
    up = pd.Series(np.where(delta > 0, delta, 0.0)).ewm(alpha=1 / 14, adjust=False).mean().values
    dn = pd.Series(np.where(delta < 0, -delta, 0.0)).ewm(alpha=1 / 14, adjust=False).mean().values
    f_rsi = np.where(dn > 0, 100 - 100 / (1 + up / np.where(dn > 0, dn, 1)), 100.0)   # 7
    f_rsi[:30] = np.nan
    ltv = df['lt_net'].values.astype(float)
    ltma = _roll(ltv, 20, 'mean')
    f_chip = ltv - ltma                                                 # 8 ★
    fs20 = _roll(df['foreign_net'].values.astype(float), 20, 'sum')
    f_fspot = (fs20 - _roll(fs20, 252, 'mean')) / _roll(fs20, 252, 'std')        # 9
    mgv = df['margin_lots'].values.astype(float)
    mg20 = mgv - np.concatenate([np.full(20, np.nan), mgv[:-20]])
    f_margin = (mg20 - _roll(mg20, 252, 'mean')) / _roll(mg20, 252, 'std')       # 10
    pcv = df['pc_oi_ratio'].values.astype(float)
    f_pcoi = pcv - _roll(pcv, 252, 'median')                            # 11
    f_spx = df['spx_gap'].values.astype(float)                          # 12
    twc = df['tw_close'].values.astype(float)
    tpc = df['tp_close'].values.astype(float)
    f_rs = (tpc / np.concatenate([np.full(20, np.nan), tpc[:-20]])
            - twc / np.concatenate([np.full(20, np.nan), twc[:-20]]))   # 13

    FMAT = np.column_stack([f_trend, f_mom60, f_mom20, f_volr, f_vold, f_dd, f_rsi,
                            f_chip, f_fspot, f_margin, f_pcoi, f_spx, f_rs])

    # ---- 粗分類模式（5 核心維度；與前版完全相同的定義）----
    bits = np.stack([
        (adjC > sma200), (f_mom60 > 0), (vol20 > volmed), (f_dd > -0.05), (ltv > ltma),
    ]).astype(int)
    valid = ~(np.isnan(sma200) | np.isnan(f_mom60) | np.isnan(vol20)
              | np.isnan(volmed) | np.isnan(f_dd) | np.isnan(ltv) | np.isnan(ltma))
    pid = np.where(valid, (bits[0] * 16 + bits[1] * 8 + bits[2] * 4 + bits[3] * 2 + bits[4]), -1)

    # ---- 前瞻報酬 ----
    cums = np.concatenate([[0.0], np.cumsum(pct)])
    fwd = {}
    for H in HORIZONS:
        f = np.full(n, np.nan)
        f[:n - H] = cums[1 + H:] - cums[1:n - H + 1]
        fwd[H] = f

    # ---- 波段切分 ----
    episodes = []
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

    hist_eps = [e for e in episodes if e['pid'] == cur_pid and e is not cur_ep]
    onset_fwd = {H: [fwd[H][e['s']] for e in hist_eps] for H in HORIZONS}
    dayd_eps = [e for e in hist_eps if e['length'] >= run_len]
    dayd_fwd = {H: [fwd[H][e['s'] + run_len - 1] for e in dayd_eps] for H in HORIZONS}
    base_fwd = {H: fwd[H][valid] for H in HORIZONS}

    def ep_row(e, ongoing=False):
        ep_ret = float((cums[e['e'] + 1] - cums[e['s'] + 1]) * 100)
        row = dict(start=dates[e['s']], end=dates[e['e']], length=e['length'],
                   ep_ret=ep_ret, ongoing=ongoing)
        for H in HORIZONS:
            v = fwd[H][e['s']]
            row[f'fwd{H}'] = None if np.isnan(v) else float(v * 100)
        return row

    ep_rows = [ep_row(e) for e in hist_eps[-12:]]
    if cur_ep:
        ep_rows.append(ep_row(cur_ep, ongoing=True))

    vd = int(valid.sum())
    dist_tbl = []
    for p in range(32):
        days = int((pid == p).sum())
        if days > 0:
            dist_tbl.append(dict(pid=p, desc=pattern_desc(p), days=days, freq=days / vd * 100))
    dist_tbl.sort(key=lambda x: -x['days'])

    # ---- 引擎 B：全因子相似日（13 維 z 距離、top 30、最小間隔 10 日）----
    vfull = ~np.isnan(FMAT).any(axis=1)
    similar = dict(ok=False)
    if vfull[n - 1] and vfull.sum() > 300:
        mu = FMAT[vfull].mean(axis=0)
        sd = FMAT[vfull].std(axis=0)
        Z = (FMAT - mu) / np.where(sd > 0, sd, 1)
        q = Z[n - 1]
        cand = vfull.copy()
        cand[n - 1 - max(HORIZONS):] = False          # 需完整 20 日前瞻（亦排除今日近鄰）
        dist = np.full(n, np.inf)
        idx = np.where(cand)[0]
        dist[idx] = np.sqrt(((Z[idx] - q) ** 2).sum(axis=1))
        picked = []
        for t in np.argsort(dist):
            if not np.isfinite(dist[t]) or len(picked) >= N_SIMILAR:
                break
            if all(abs(int(t) - p) >= MIN_GAP for p in picked):
                picked.append(int(t))
        sim_fwd = {H: [fwd[H][t] for t in picked] for H in HORIZONS}
        base2_fwd = {H: fwd[H][cand] for H in HORIZONS}
        rows = []
        for t in sorted(picked, key=lambda t: dist[t])[:12]:
            rows.append(dict(date=dates[t], dist=float(dist[t]),
                             pid=int(pid[t]) if pid[t] >= 0 else None,
                             fwd5=float(fwd[5][t] * 100), fwd10=float(fwd[10][t] * 100),
                             fwd20=float(fwd[20][t] * 100)))
        similar = dict(ok=True, n_sel=len(picked), n_pool=int(cand.sum()),
                       min_gap=MIN_GAP, valid_from=dates[int(np.argmax(vfull))],
                       days=rows, stats=_hstats(sim_fwd), baseline=_hstats(base2_fwd))

    # ---- 13 維因子卡片 ----
    i = n - 1
    e8 = 1e8  # 億
    factors = [
        dict(name='★ 長期趨勢', on=int(bits[0][i]), state=DIM_LABELS[0][1 - bits[0][i]],
             detail=f"收盤距 SMA200: {f_trend[i]*100:+.1f}%"),
        dict(name='★ 中期動能', on=int(bits[1][i]), state=DIM_LABELS[1][1 - bits[1][i]],
             detail=f"60 日變動: {f_mom60[i]*100:+.1f}%"),
        dict(name='短期動能', on=int(f_mom20[i] > 0), state='20日漲' if f_mom20[i] > 0 else '20日跌',
             detail=f"20 日變動: {f_mom20[i]*100:+.1f}%"),
        dict(name='★ 波動水準', on=int(bits[2][i]), state=DIM_LABELS[2][1 - bits[2][i]],
             detail=f"20日年化 {vol20[i]*100:.1f}% / 中位 {volmed[i]*100:.1f}%"),
        dict(name='波動方向', on=int(f_vold[i] > 0), state='波動升' if f_vold[i] > 0 else '波動降',
             detail=f"vs 60日均: {f_vold[i]*100:+.1f}pp"),
        dict(name='★ 價格位階', on=int(bits[3][i]), state=DIM_LABELS[3][1 - bits[3][i]],
             detail=f"距 252 日高點: {f_dd[i]*100:+.1f}%"),
        dict(name='RSI(14)', on=int(f_rsi[i] > 50), state=f"RSI {f_rsi[i]:.0f}",
             detail='高於 50' if f_rsi[i] > 50 else '低於 50'),
        dict(name='★ 大額交易人', on=int(bits[4][i]), state=DIM_LABELS[4][1 - bits[4][i]],
             detail=f"淨部位 {ltv[i]:+,.0f} 口 (20日均 {ltma[i]:+,.0f})"),
        dict(name='外資現貨', on=int(fs20[i] > 0), state='20日買超' if fs20[i] > 0 else '20日賣超',
             detail=f"{fs20[i]/e8:+,.0f} 億 (z={f_fspot[i]:+.1f})"),
        dict(name='融資動向', on=int(mg20[i] > 0), state='融資增' if mg20[i] > 0 else '融資減',
             detail=f"20日 {mg20[i]:+,.0f} 張 (z={f_margin[i]:+.1f})"),
        dict(name='P/C 未平倉比', on=int(f_pcoi[i] > 0), state='偏高' if f_pcoi[i] > 0 else '偏低',
             detail=f"{pcv[i]:.2f} (vs 252日中位 {f_pcoi[i]:+.2f})"),
        dict(name='美股趨勢', on=int(f_spx[i] > 0), state='SPX>SMA200' if f_spx[i] > 0 else 'SPX<SMA200',
             detail=f"距離: {f_spx[i]*100:+.1f}%"),
        dict(name='櫃買相對強弱', on=int(f_rs[i] > 0), state='中小型強' if f_rs[i] > 0 else '中小型弱',
             detail=f"20日相對: {f_rs[i]*100:+.1f}pp"),
    ]

    return dict(
        meta=dict(as_of=dates[n - 1], computed_at=datetime.now().isoformat(timespec='seconds'),
                  valid_from=dates[int(np.argmax(valid))], valid_days=vd,
                  n_patterns=len(dist_tbl), horizons=list(HORIZONS), n_dims=FMAT.shape[1]),
        current=dict(pattern_id=cur_pid, desc=pattern_desc(cur_pid),
                     run_len=run_len, factors=factors,
                     freq=float((pid == cur_pid).sum() / vd * 100) if cur_pid is not None else None,
                     n_hist_episodes=len(hist_eps),
                     low_sample=len(hist_eps) < 8),
        stats=dict(onset=_hstats(onset_fwd), dayd=_hstats(dayd_fwd), dayd_n_eps=len(dayd_eps),
                   baseline=_hstats(base_fwd)),
        episodes=ep_rows,
        similar=similar,
        dist=dist_tbl[:8],
    )


def pattern_desc(p):
    if p is None or p < 0:
        return '--'
    b = [(p >> 4) & 1, (p >> 3) & 1, (p >> 2) & 1, (p >> 1) & 1, p & 1]
    return '｜'.join(DIM_LABELS[k][1 - b[k]] for k in range(5))


if __name__ == '__main__':
    r = compute()
    m, cu, st, sim = r['meta'], r['current'], r['stats'], r['similar']
    print(f"[analogue] as of {m['as_of']}  dims={m['n_dims']}  pattern-valid {m['valid_from']}+ ({m['valid_days']} days)")
    print(f"[analogue] pattern #{cu['pattern_id']} run={cu['run_len']}d "
          f"freq={cu['freq']:.1f}% hist_episodes={cu['n_hist_episodes']}")
    blocks = [(st['onset'], 'onset'), (st['dayd'], f"day{cu['run_len']}"), (st['baseline'], 'base')]
    if sim.get('ok'):
        print(f"[analogue] similar: pool={sim['n_pool']} ({sim['valid_from']}+) sel={sim['n_sel']} gap>={sim['min_gap']}d")
        blocks += [(sim['stats'], 'sim'), (sim['baseline'], 'simbase')]
    for blk, lbl in blocks:
        line = "  ".join(
            f"H{s['h']}: n={s['n']}" + (f" win={s['win']:.0f}%[{s['lo']:.0f},{s['hi']:.0f}] mean={s['mean']:+.2f}%"
                                        if s['n'] else "")
            for s in blk)
        print(f"[analogue] {lbl:<7} {line}")
