# -*- coding: utf-8 -*-
"""
risk_layer_research2.py - 部位層研究 #2：趨勢艙的槓桿/減碼時機（非因子挖掘）

═══ 定位與家族性錯誤記錄 ═══
使用者目標：以趨勢艙為基底，找「適合加槓桿 / 該減碼」的時機。
部位層不產生方向預測，不在封槍範圍；但本檔為部位層【第二次】測試
（#1 = risk_layer_research.py 的只減倉波動率目標化，未過採納規則）——
累積測試越多，邊緣性通過越該打折。本輪 4 組設定，跑完部位層也封存。

═══ 核心方法論：時機價值必須與「等平均曝險的固定槓桿」對照 ═══
把趨勢艙無腦乘上 2 倍，損益與 MDD 同步線性放大——那不是時機，是膽量。
「何時加槓桿」有價值，若且唯若：動態槓桿在【相同 dev 平均曝險】下，
風險調整後優於固定槓桿。故對照組 = 固定 L̄ × 趨勢（L̄ = 動態版 dev 期
在市日的平均槓桿），而非裸趨勢艙。

═══ 事前承諾（運行前寫定）═══
- 基底訊號：SMA200 趨勢艙（多/空手），定義凍結不動
- 槓桿規則（文獻: Moreira & Muir 2017 波動率管理）：
    L_t = clip(σ_target_N / σ_{t-1}, 0.5, Lmax) × trend_t
    σ_N = 換月調整日報酬 N 日滾動 std，N ∈ {20, 60}（與部位層#1 相同兩窗，不新增）
    σ_target_N = dev 2001-2019 σ_N 中位數（同#1，不重調）
    Lmax ∈ {1.5, 2.0}；Lmin = 0.5 固定。共 4 組，無其他旋鈕
- 執行：粒度 0.05（小型/微型契約）、再平衡帶 0.10、成本 1.5 點/邊 × |ΔL|、
  結算日強平——與部位層#1 完全相同引擎
- 判定（dev 2001-2019；兩半段 2001-2010 / 2011-2019）：
  採納候選 = (a) 動態 devSh >= 等曝險固定版 devSh + 0.10
           且 (b) 動態 devMDD >= 等曝險固定版 devMDD（不更深）
           且 (c) 兩半段 Sh 皆 >= 固定版同段 − 0.10
  通過 → 以紙上艙形式加入 signal_ledger 前瞻追蹤（不直接實倉）；未過 → 僅記錄
- 2020-2026 僅描述性（窗已燒毀）
- 無論結果，另附純算術的固定槓桿對照表（1.0/1.5/2.0×，線性縮放非挖掘），
  供使用者按 MDD 預算選固定槓桿
"""
import math
import os
import sqlite3

import numpy as np
import pandas as pd

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5
GRAN = 0.05
BAND = 0.10

# ================= 資料（與部位層#1 相同）=================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
M = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, open_price, close_price, delivery_month FROM R WHERE rn=1
    ORDER BY trading_date""", conn)
conn.close()
M['trading_date'] = pd.to_datetime(M['trading_date'])
n = len(M)
yr = M['trading_date'].dt.year.values
o = M['open_price'].values.astype(float)
c = M['close_price'].values.astype(float)
dm = M['delivery_month'].values
settle = np.zeros(n, dtype=bool)
settle[:-1] = dm[:-1] != dm[1:]
roll_first = np.zeros(n, dtype=bool)
roll_first[1:] = dm[1:] != dm[:-1]

adj_ret = np.zeros(n)
adj_ret[1:] = np.where(roll_first[1:], c[1:] - o[1:], c[1:] - c[:-1])
adjC = c[0] + np.cumsum(adj_ret)

DEV = (yr >= 2001) & (yr <= 2019)
H1 = (yr >= 2001) & (yr <= 2010)
H2 = (yr >= 2011) & (yr <= 2019)
TEST = (yr >= 2020)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


# 趨勢艙（凍結定義）
sma200 = pd.Series(adjC).rolling(200).mean().values
above = shift1((adjC > sma200).astype(float))
sig_tr = np.where(np.isnan(shift1(sma200)) | np.isnan(above), 0, np.where(above == 1, 1, 0)).astype(float)


def frac_engine(raw, scale):
    pnl = np.zeros(n)
    positions = np.zeros(n)
    pos = 0.0
    for t in range(1, n):
        p_prev = pos
        if settle[t]:
            pnl[t] = (p_prev * (o[t] - c[t - 1]) if not roll_first[t] else 0.0) - COST * abs(p_prev)
            pos = 0.0
            positions[t] = pos
            continue
        tgt = raw[t] * scale[t]
        tgt = round(tgt / GRAN) * GRAN
        if not (np.sign(tgt) != np.sign(pos) or abs(tgt - pos) >= BAND):
            tgt = pos
        ovn = 0.0 if roll_first[t] else p_prev * (o[t] - c[t - 1])
        pnl[t] = ovn + tgt * (c[t] - o[t]) - COST * abs(tgt - p_prev)
        pos = tgt
        positions[t] = pos
    return pnl, positions


def stats(pnl, mask):
    x = pnl[mask]
    sd = x.std()
    sh = x.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min() if len(x) else 0.0
    ys = pd.Series(x).groupby(yr[mask]).sum()
    return dict(tot=x.sum(), sharpe=sh, mdd=mdd, posy=int((ys > 0).sum()), ny=len(ys))


def row(label, pnl, posn=None):
    d = stats(pnl, DEV)
    h1 = stats(pnl, H1)['sharpe']
    h2 = stats(pnl, H2)['sharpe']
    t = stats(pnl, TEST)
    avgL = np.nan
    if posn is not None:
        inmkt = DEV & (sig_tr > 0)
        avgL = posn[inmkt].mean() if inmkt.any() else np.nan
    print(f"{label:<26} {d['tot']:>9,.0f} {d['sharpe']:>6.2f} {d['mdd']:>8,.0f} {d['posy']:>4}/19 "
          f"{h1:>6.2f} {h2:>6.2f} {avgL:>5.2f} | {t['tot']:>9,.0f} {t['sharpe']:>6.2f} {t['mdd']:>9,.0f}")
    return d, h1, h2


print("=" * 112)
print("部位層研究 #2：趨勢艙槓桿時機   dev 2001-2019（test 僅描述）")
print("=" * 112)
print(f"{'設定':<26} {'dev淨':>9} {'devSh':>6} {'devMDD':>8} {'正年':>6} {'半1Sh':>6} {'半2Sh':>6} {'均L':>5} | {'test淨':>9} {'testSh':>6} {'testMDD':>9}")
print("-" * 112)

# 裸趨勢艙（參考）
pnl_raw, pos_raw = frac_engine(sig_tr, np.ones(n))
row('趨勢 1.0x（參考）', pnl_raw, pos_raw)
print("-" * 112)

verdicts = []
today = {}
for N in (20, 60):
    sig_ma = pd.Series(adj_ret).rolling(N).std().values
    s_tgt = np.nanmedian(sig_ma[DEV])
    base_scale = s_tgt / shift1(sig_ma)
    base_scale[np.isnan(base_scale)] = 1.0
    for LMAX in (1.5, 2.0):
        scale = np.clip(base_scale, 0.5, LMAX)
        pnl_dyn, pos_dyn = frac_engine(sig_tr, scale)
        inmkt = DEV & (sig_tr > 0)
        Lbar = scale[inmkt].mean()
        pnl_const, pos_const = frac_engine(sig_tr, np.full(n, Lbar))
        nm = f'VT{N} cap{LMAX}'
        dc, ch1, ch2 = row(f'固定 {Lbar:.2f}x（對照）', pnl_const, pos_const)
        dd, dh1, dh2 = row(f'動態 {nm}', pnl_dyn, pos_dyn)
        ok = (dd['sharpe'] >= dc['sharpe'] + 0.10 and dd['mdd'] >= dc['mdd']
              and dh1 >= ch1 - 0.10 and dh2 >= ch2 - 0.10)
        verdicts.append((nm, ok))
        print(f"{'':<26} 判定: {'通過採納候選' if ok else '未過'}"
              f"（需 dynSh>={dc['sharpe']+0.10:.2f} 且 dynMDD>={dc['mdd']:,.0f} 且 兩半段不劣0.10）")
        print("-" * 112)
        today[nm] = scale[n - 1]

print(f"\n>>> 4 組判定: " + "  ".join(f"{nm}:{'PASS' if ok else 'FAIL'}" for nm, ok in verdicts))

# ================= 純算術固定槓桿對照（非挖掘：線性縮放）=================
print()
print("=" * 112)
print("附錄：固定槓桿純算術對照（趨勢艙 × k，供按 MDD 預算選擇；TX 1 點 = NT$200）")
print("=" * 112)
d1 = stats(pnl_raw, DEV)
t1 = stats(pnl_raw, TEST)
for k in (1.0, 1.5, 2.0):
    print(f"  {k:.1f}x: dev 淨 {d1['tot']*k:>9,.0f} / MDD {d1['mdd']*k:>8,.0f} 點 (NT${d1['mdd']*k*200/1e4:,.0f} 萬/口)   "
          f"test 淨 {t1['tot']*k:>9,.0f} / MDD {t1['mdd']*k:>9,.0f} 點")
print("  （固定槓桿 Sharpe 不變＝{:.2f}/{:.2f}；變的只有絕對損益與 MDD，純風險偏好選擇）".format(d1['sharpe'], t1['sharpe']))

# 今日讀數
i = n - 1
print(f"\n今日（{M['trading_date'].iloc[-1].date()}）讀數: 趨勢={'在市' if sig_tr[i] else '空手'}  "
      + "  ".join(f"{nm}建議L={v:.2f}" for nm, v in today.items()))
print("\n[done] 部位層 #2 結束；規則為檔頭事前承諾，部位層研究就此封存。")
