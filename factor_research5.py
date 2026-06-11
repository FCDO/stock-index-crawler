# -*- coding: utf-8 -*-
"""
因子研究 第6輪（最終輪，封槍輪）：外資現貨 / 大額交易人 / 融資餘額
（累積: R1-2=22, R3=16, R4=12, R5=7, 本輪 8 因子 (+至多1組合) → 總計 65-66 組）

事前承諾（資料回補完成前寫定；VRP/TVIX 因 TAIFEX 無歷史資料可得而放棄，記錄在案）：

[FS 外資現貨買賣超]（TWSE 市場合計, NT$, 2007+；文獻方向: 順流跟隨）
  FS_MA(5)  : sign(5日均買賣超)
  FS_MA(20) : sign(20日均買賣超)
  FS_Z(252) : 單日買賣超 z>+1 做多 / z<-1 做空 / 其餘空手
  dev 2008-2019(12y), 半段 2008-2013/2014-2019, 正年>=8/12

[LT 大額交易人特定法人]（TX 全契約 999999 前五大特定法人淨未平倉, 2004+；方向: 跟隨）
  LT_SIGN   : sign(淨部位)
  LT_MA(5)  : sign(5日均淨部位)
  LT_TREND(20): 淨部位 vs 20日均
  dev 2005-2019(15y), 半段 2005-2012/2013-2019, 正年>=10/15

[MG 融資餘額]（TWSE 市場融資餘額(張), 2001+；文獻方向: 變化極端值逆勢）
  MG_Z(20)  : 20日增量 z>+1.5 做空 / z<-1.5 做多 / 其餘空手（z 基於 252 日滾動）
  MG_Z(60)  : 60日增量 同上
  dev 2003-2019(17y), 半段 2003-2011/2012-2019, 正年>=11/17

共同門檻: 淨 Sharpe>=0.7、家族高原(>=2組Sh>=0.5)、兩半段皆正、合格參數取中位。
存活者 test 2020-2026 一次性；若 >=2 家族存活，等權多數決組合亦一次性測試。
本輪結束後封槍：不再追加日線因子輪次。
"""
import pandas as pd
import numpy as np
import sqlite3
import os

BASE = os.path.dirname(os.path.abspath(__file__))
COST = 1.5

# ================= 資料 =================
conn = sqlite3.connect(os.path.join(BASE, 'tx_futures.db'))
M = pd.read_sql_query("""
    WITH R AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY trading_date ORDER BY delivery_month) rn
               FROM tx_futures WHERE session='一般')
    SELECT trading_date, open_price, close_price, delivery_month FROM R WHERE rn=1
    ORDER BY trading_date
""", conn)
conn.close()
conn = sqlite3.connect(os.path.join(BASE, 'institutional.db'))
spot = pd.read_sql_query("SELECT date, foreign_net FROM spot_flows ORDER BY date", conn)
lt = pd.read_sql_query("SELECT date, t5_spec_long, t5_spec_short FROM large_trader_tx ORDER BY date", conn)
mg = pd.read_sql_query("SELECT date, margin_lots FROM margin_totals ORDER BY date", conn)
conn.close()

M['trading_date'] = pd.to_datetime(M['trading_date'])
for d in (spot, lt, mg):
    d['date'] = pd.to_datetime(d['date'])
M = (M.merge(spot, left_on='trading_date', right_on='date', how='left').drop(columns='date')
      .merge(lt, left_on='trading_date', right_on='date', how='left').drop(columns='date')
      .merge(mg, left_on='trading_date', right_on='date', how='left').drop(columns='date'))

n = len(M)
yr = M['trading_date'].dt.year.values
o = M['open_price'].values.astype(float)
c = M['close_price'].values.astype(float)
dm = M['delivery_month'].values
settle = np.zeros(n, dtype=bool)
settle[:-1] = dm[:-1] != dm[1:]

fs = M['foreign_net'].values.astype(float)
net5 = (M['t5_spec_long'] - M['t5_spec_short']).values.astype(float)
mlots = M['margin_lots'].values.astype(float)
TEST = (yr >= 2020)


def shift1(x):
    out = np.empty(len(x), dtype=float)
    out[0] = np.nan
    out[1:] = x[:-1]
    return out


def ma(x, N):
    return pd.Series(x).rolling(N).mean().values


def sd_(x, N):
    return pd.Series(x).rolling(N).std().values


def run_cont(sig, cost=COST):
    pnl = np.zeros(n)
    pos = 0
    ep = np.nan
    for t in range(1, n):
        p = 0.0
        tc = 0.0
        if settle[t]:
            if pos == 1:
                p = o[t] - ep
            elif pos == -1:
                p = ep - o[t]
            if pos != 0:
                tc = cost
            pos = 0
            ep = np.nan
        else:
            tgt = 0 if np.isnan(sig[t]) else int(sig[t])
            if tgt != pos:
                if pos == 1:
                    p += o[t] - ep
                elif pos == -1:
                    p += ep - o[t]
                tc += cost * abs(tgt - pos)
                pos = tgt
                ep = o[t] if pos != 0 else np.nan
            if pos == 1:
                p += c[t] - ep
                ep = c[t]
            elif pos == -1:
                p += ep - c[t]
                ep = c[t]
        pnl[t] = p - tc
    return pnl


def stats(pnl, mask):
    x = pnl[mask]
    sd = x.std()
    sh = x.mean() / sd * np.sqrt(250) if sd > 0 else 0.0
    cum = x.cumsum()
    mdd = (cum - np.maximum.accumulate(cum)).min() if len(x) else 0.0
    ys = pd.Series(x).groupby(yr[mask]).sum()
    return dict(tot=x.sum(), sharpe=sh, mdd=mdd, posy=int((ys > 0).sum()), nyears=len(ys), ys=ys)


def sgn(x1):
    return np.where(np.isnan(x1), 0, np.where(x1 > 0, 1, -1))


def Y(a, b):
    return (yr >= a) & (yr <= b)


# ================= 候選 =================
def fs_z():
    z = (fs - ma(fs, 252)) / sd_(fs, 252)
    x = shift1(z)
    return np.where(np.isnan(x), 0, np.where(x > 1, 1, np.where(x < -1, -1, 0)))


def mg_z(N):
    dN = mlots - np.concatenate([np.full(N, np.nan), mlots[:-N]])
    z = (dN - ma(dN, 252)) / sd_(dN, 252)
    x = shift1(z)
    return np.where(np.isnan(x), 0, np.where(x > 1.5, -1, np.where(x < -1.5, 1, 0)))


FAMS = {
    'FS': dict(DEV=Y(2008, 2019), H1=Y(2008, 2013), H2=Y(2014, 2019), posy=8,
               cands=[('FS_MA(5)', 5, sgn(shift1(ma(fs, 5)))),
                      ('FS_MA(20)', 20, sgn(shift1(ma(fs, 20)))),
                      ('FS_Z(252)', 252, fs_z())]),
    'LT': dict(DEV=Y(2005, 2019), H1=Y(2005, 2012), H2=Y(2013, 2019), posy=10,
               cands=[('LT_SIGN', 1, sgn(shift1(net5))),
                      ('LT_MA(5)', 5, sgn(shift1(ma(net5, 5)))),
                      ('LT_TREND(20)', 20, sgn(shift1(net5 - ma(net5, 20))))]),
    'MG': dict(DEV=Y(2003, 2019), H1=Y(2003, 2011), H2=Y(2012, 2019), posy=11,
               cands=[('MG_Z(20)', 20, mg_z(20)),
                      ('MG_Z(60)', 60, mg_z(60))]),
}

pnl_long = run_cont(np.ones(n))
results = {}
print("=" * 108)
print("第6輪（最終輪）  各家族 dev 視窗依資料起點: FS 2008-2019 / LT 2005-2019 / MG 2003-2019")
print("=" * 108)
for fam, cfg in FAMS.items():
    bl = stats(pnl_long, cfg['DEV'])
    print(f"\n[{fam}] 基準[永遠做多] dev: 淨 {bl['tot']:,.0f}  Sharpe {bl['sharpe']:.2f}  正年 {bl['posy']}/{bl['nyears']}")
    print(f"{'設定':<14} {'淨損益':>9} {'Sharpe':>7} {'正年':>6} {'前半':>9} {'後半':>9} {'MDD':>8}")
    print("-" * 70)
    for name, prm, sig in cfg['cands']:
        pnl = run_cont(sig)
        st = stats(pnl, cfg['DEV'])
        h1, h2 = stats(pnl, cfg['H1']), stats(pnl, cfg['H2'])
        results[name] = dict(family=fam, param=prm, sig=sig, pnl=pnl, dev=st,
                             h1=h1['tot'], h2=h2['tot'])
        print(f"{name:<14} {st['tot']:>9,.0f} {st['sharpe']:>7.2f} {st['posy']:>4}/{st['nyears']} "
              f"{h1['tot']:>9,.0f} {h2['tot']:>9,.0f} {st['mdd']:>8,.0f}")

# ================= 判定 =================
print()
print("=" * 108)
print("判定: Sharpe>=0.7 且 正年達標 且 家族高原(>=2組Sh>=0.5) 且 兩半段皆正; 中位參數")
print("=" * 108)
survivors = []
for fam, cfg in FAMS.items():
    names = [nm for nm, _, _ in cfg['cands']]
    sh = {nm: results[nm]['dev']['sharpe'] for nm in names}
    best = max(sh, key=sh.get)
    plateau = sum(1 for v in sh.values() if v >= 0.5) >= 2
    ok = sh[best] >= 0.7 and results[best]['dev']['posy'] >= cfg['posy'] and plateau
    if ok:
        oks = sorted([x for x in names if sh[x] >= 0.5], key=lambda x: results[x]['param'])
        pick = oks[len(oks) // 2]
        if results[pick]['h1'] > 0 and results[pick]['h2'] > 0:
            survivors.append(pick)
            print(f"[PASS] {fam} 入選 {pick} (Sh={sh[pick]:.2f}, 半段 {results[pick]['h1']:+,.0f}/{results[pick]['h2']:+,.0f})")
        else:
            print(f"[FAIL] {fam} {pick} 兩半段不一致 ({results[pick]['h1']:+,.0f}/{results[pick]['h2']:+,.0f})")
    else:
        print(f"[FAIL] {fam} 最佳 {best}(Sh={sh[best]:.2f}, 正年{results[best]['dev']['posy']}, 高原{'O' if plateau else 'X'})")

print(f"\n>>> 最終輪存活: {survivors if survivors else '（無）'}")

# ================= 一次性最終測試 =================
if survivors:
    print()
    print("=" * 108)
    print("一次性最終測試 2020-2026/06")
    print("=" * 108)
    bt = stats(pnl_long, TEST)
    for nm in survivors:
        st = stats(results[nm]['pnl'], TEST)
        print(f"{nm:<16} 淨 {st['tot']:>8,.0f}  Sharpe {st['sharpe']:5.2f}  正年 {st['posy']}/{st['nyears']}  MDD {st['mdd']:>8,.0f}")
        print(f"  年度: " + "  ".join(f"{y}:{v:+,.0f}" for y, v in st['ys'].items()))
    print(f"{'永遠做多':<16} 淨 {bt['tot']:>8,.0f}  Sharpe {bt['sharpe']:5.2f}  正年 {bt['posy']}/{bt['nyears']}  MDD {bt['mdd']:>8,.0f}")
    if len(survivors) >= 2:
        vote = np.sum([results[nm]['sig'] for nm in survivors], axis=0)
        comb = np.sign(vote)
        pnl_c = run_cont(comb)
        # 組合 dev 參考用最窄共同窗(取家族 dev 起點最大者)
        starts = {'FS': 2008, 'LT': 2005, 'MG': 2003}
        a = max(starts[results[nm]['family']] for nm in survivors)
        d = stats(pnl_c, Y(a, 2019))
        t = stats(pnl_c, TEST)
        print(f"\n組合(等權多數決, dev {a}-2019): 淨 {d['tot']:,.0f} Sh {d['sharpe']:.2f} | test 淨 {t['tot']:,.0f} Sh {t['sharpe']:.2f} MDD {t['mdd']:,.0f}")

print("\n[done] 第6輪結束 — 累積 65+ 組測試, 日線因子研究就此封槍。")
