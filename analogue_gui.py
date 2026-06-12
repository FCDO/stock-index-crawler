# -*- coding: utf-8 -*-
"""
市場狀態類比工具 - Web GUI（描述性，非交易訊號）
零外部依賴（Python 標準庫 + market_analogue 引擎）
使用方式：python analogue_gui.py 或雙擊 open_analogue.bat → http://127.0.0.1:8788
"""
import http.server
import json
import os
import threading
import webbrowser

import market_analogue

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PORT = 8788

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-Hant">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>市場狀態類比（描述性工具）</title>
<style>
*{margin:0;padding:0;box-sizing:border-box}
:root{
  --bg:#0d1117;--card:#161b22;--border:#30363d;
  --text:#e6edf3;--dim:#8b949e;--green:#3fb950;
  --red:#f85149;--blue:#58a6ff;--yellow:#d29922;
  --card-r:12px;
}
body{background:var(--bg);color:var(--text);
  font-family:'Segoe UI','Microsoft JhengHei UI',sans-serif;
  padding:20px;max-width:1150px;margin:0 auto;line-height:1.5}
h1{font-size:1.4rem;font-weight:600}
.header{display:flex;justify-content:space-between;align-items:center;
  margin-bottom:6px;flex-wrap:wrap;gap:8px}
.badge{background:#3d2e00;color:var(--yellow);border:1px solid var(--yellow);
  border-radius:20px;padding:3px 14px;font-size:.82rem;font-weight:600}
.sub{color:var(--dim);font-size:.85rem;margin-bottom:18px}

.card{background:var(--card);border:1px solid var(--border);
  border-radius:var(--card-r);padding:22px;margin-bottom:18px}
.pattern-desc{font-size:1.35rem;font-weight:700;margin:6px 0 4px}
.pattern-meta{color:var(--dim);font-size:.9rem;margin-bottom:14px}
.pattern-meta b{color:var(--text)}
.factors{display:grid;grid-template-columns:repeat(auto-fit,minmax(195px,1fr));gap:10px}
.factor{background:#1c2128;border:1px solid var(--border);border-radius:8px;padding:10px 12px}
.factor .name{font-size:.75rem;color:var(--dim)}
.factor .state{font-size:1.02rem;font-weight:700;margin:2px 0}
.factor .state.on{color:var(--green)}.factor .state.off{color:var(--red)}
.factor .detail{font-size:.78rem;color:var(--dim);font-family:'Consolas',monospace}
.warn-chip{display:inline-block;background:#3d1d1d;color:var(--red);border:1px solid var(--red);
  border-radius:6px;padding:2px 10px;font-size:.8rem;margin-left:10px}

.section-title{font-size:1rem;font-weight:600;margin:18px 0 10px;color:var(--dim)}
table{width:100%;border-collapse:collapse;font-size:.88rem}
th{background:#1c2128;color:var(--dim);font-weight:600;padding:10px 8px;text-align:center;white-space:nowrap}
td{padding:9px 8px;text-align:center;border-top:1px solid var(--border);
  font-family:'Consolas','Courier New',monospace;white-space:nowrap}
td.l,th.l{text-align:left}
.table-wrap{background:var(--card);border:1px solid var(--border);
  border-radius:var(--card-r);overflow-x:auto;margin-bottom:18px}
td.pos{color:var(--green)}td.neg{color:var(--red)}td.dimc{color:var(--dim)}
.ci{color:var(--dim);font-size:.78rem}
tr.base td{background:#10151c;color:var(--blue)}
tr.ongoing td{background:#15201a}
.tag-ongoing{color:var(--green);font-size:.78rem;border:1px solid var(--green);
  border-radius:4px;padding:0 6px;margin-left:6px}

.caveats{background:#2d2306;border:1px solid var(--yellow);border-radius:var(--card-r);
  padding:16px 20px;margin-bottom:18px;font-size:.86rem}
.caveats b{color:var(--yellow)}
.caveats li{margin:6px 0 0 18px}
.btn-bar{display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:24px}
.btn{background:var(--blue);color:#fff;border:none;border-radius:8px;
  padding:10px 24px;font-size:.95rem;cursor:pointer;font-weight:600}
.btn:hover{opacity:.85}.btn:disabled{opacity:.4}
.btn-status{color:var(--dim);font-size:.85rem}
</style>
</head>
<body>

<div class="header">
  <h1>市場狀態類比</h1>
  <span class="badge">描述性工具・非交易訊號</span>
</div>
<div class="sub" id="metaLine">--</div>

<div class="card">
  <div class="pattern-meta" id="curDate">--</div>
  <div class="pattern-desc" id="patternDesc">--</div>
  <div class="pattern-meta" id="patternMeta">--</div>
  <div class="factors" id="factorsArea"></div>
</div>

<div class="section-title">引擎 B｜全因子相似日類比（13 維 z 距離・等權重・最相似 30 天・間隔 ≥10 日）</div>
<div class="table-wrap"><table>
  <thead><tr>
    <th class="l">條件</th><th>樣本</th>
    <th>後5日勝率</th><th>後5日均報酬</th>
    <th>後10日勝率</th><th>後10日均報酬</th>
    <th>後20日勝率</th><th>後20日均報酬</th>
  </tr></thead>
  <tbody id="simStatsBody"></tbody>
</table></div>

<div class="section-title">最相似的 12 個歷史交易日（距離越小越像；含當時所屬模式）</div>
<div class="table-wrap"><table>
  <thead><tr>
    <th>日期</th><th>z距離</th><th class="l">當時模式</th>
    <th>後5日</th><th>後10日</th><th>後20日</th>
  </tr></thead>
  <tbody id="simDaysBody"></tbody>
</table></div>

<div class="section-title">引擎 A｜粗分類模式事後統計（僅 ★ 核心 5 維；後 5 / 10 / 20 日，請與基準列對照）</div>
<div class="table-wrap"><table>
  <thead><tr>
    <th class="l">條件</th><th>樣本</th>
    <th>後5日勝率</th><th>後5日均報酬</th>
    <th>後10日勝率</th><th>後10日均報酬</th>
    <th>後20日勝率</th><th>後20日均報酬</th>
  </tr></thead>
  <tbody id="statsBody"></tbody>
</table></div>

<div class="section-title">符合當前模式的歷史波段（最近 12 段）</div>
<div class="table-wrap"><table>
  <thead><tr>
    <th>起點</th><th>迄日</th><th>持續天數</th><th>波段內報酬</th>
    <th>後5日</th><th>後10日</th><th>後20日</th>
  </tr></thead>
  <tbody id="epBody"></tbody>
</table></div>

<div class="section-title">最常見模式分布（脈絡參考）</div>
<div class="table-wrap"><table>
  <thead><tr><th class="l">模式</th><th>天數</th><th>佔比</th></tr></thead>
  <tbody id="distBody"></tbody>
</table></div>

<div class="caveats">
  <b>解讀前必讀（本工具的誠實邊界）</b>
  <li>本頁為<b>樣本內敘述統計</b>：狀態定義與事後統計用同一份歷史資料，且 32 種模式 × 3 個視窗存在大量多重比較——「好看的數字」是選擇偏誤的預期產物，不構成預測力證據。</li>
  <li>相似日引擎為 13 維等權 z 距離（權重事前固定、不調參）；「相似」由這 13 個維度定義，市場可能在未納入的維度上完全不同。</li>
  <li>波段在時間上群聚、前瞻視窗互相重疊，有效樣本比表面 n 更小；信賴區間以獨立樣本假設計算，實際更寬。</li>
  <li>解讀建議：先看藍色基準列。只有條件統計的<b>整個信賴區間</b>離開基準時才值得多看一眼——即使如此，它描述的也只是過去。</li>
  <li>本工具與 72 組事前承諾因子研究、signal_ledger 部署路徑<b>完全隔離</b>，不接任何部位決策。</li>
</div>

<div class="btn-bar">
  <button class="btn" id="btnRefresh" onclick="refresh()">重新計算（重讀資料庫）</button>
  <span class="btn-status" id="btnStatus"></span>
</div>

<script>
function pc(v,d=1){return v==null?'--':(v>0?'+':'')+v.toFixed(d)+'%'}
function cls(v){return v==null?'dimc':v>0?'pos':v<0?'neg':'dimc'}

function statCells(s){
  if(!s||!s.n)return '<td class="dimc" colspan="2">n=0</td>';
  return `<td class="${s.win>=50?'pos':'neg'}">${s.win.toFixed(0)}% <span class="ci">[${s.lo.toFixed(0)},${s.hi.toFixed(0)}]</span></td>`+
         `<td class="${cls(s.mean)}">${pc(s.mean,2)} <span class="ci">&plusmn;${s.half==null?'--':s.half.toFixed(2)}</span></td>`;
}

function render(d){
  const m=d.meta,cu=d.current,st=d.stats;
  document.getElementById('metaLine').textContent=
    `資料截至 ${m.as_of}｜狀態維度 ${m.n_dims} 維｜模式樣本 ${m.valid_from} 起共 ${m.valid_days.toLocaleString()} 個交易日｜計算於 ${m.computed_at.replace('T',' ')}`;
  document.getElementById('curDate').textContent=`${m.as_of} 收盤狀態`;
  document.getElementById('patternDesc').textContent=`粗分類模式 #${cu.pattern_id}｜${cu.desc}`;
  document.getElementById('patternMeta').innerHTML=
    `已持續 <b>${cu.run_len}</b> 個交易日｜此模式歷史佔比 <b>${cu.freq.toFixed(1)}%</b>｜`+
    `歷史波段 <b>${cu.n_hist_episodes}</b> 段`+
    (cu.low_sample?'<span class="warn-chip">歷史樣本不足，統計僅供參考</span>':'');

  const fa=document.getElementById('factorsArea');fa.innerHTML='';
  cu.factors.forEach(f=>{
    fa.innerHTML+=`<div class="factor"><div class="name">${f.name}</div>`+
      `<div class="state ${f.on?'on':'off'}">${f.state}</div>`+
      `<div class="detail">${f.detail}</div></div>`;
  });

  // 引擎 B：相似日
  const ssb=document.getElementById('simStatsBody');ssb.innerHTML='';
  const sdb=document.getElementById('simDaysBody');sdb.innerHTML='';
  if(d.similar&&d.similar.ok){
    const sm=d.similar;
    [[`最相似 ${sm.n_sel} 天（${sm.valid_from} 起，池 ${sm.n_pool.toLocaleString()} 天）`,sm.stats,sm.n_sel,''],
     [`無條件基準（相似日樣本池）`,sm.baseline,sm.n_pool,'base']
    ].forEach(([lbl,blk,nn,rc])=>{
      let h=`<tr class="${rc}"><td class="l">${lbl}</td><td>${nn.toLocaleString()}</td>`;
      blk.forEach(s=>{h+=statCells(s)});
      ssb.innerHTML+=h+'</tr>';
    });
    sm.days.forEach(r=>{
      sdb.innerHTML+=`<tr><td>${r.date}</td><td>${r.dist.toFixed(2)}</td>`+
        `<td class="l">#${r.pid==null?'--':r.pid}</td>`+
        `<td class="${cls(r.fwd5)}">${pc(r.fwd5)}</td>`+
        `<td class="${cls(r.fwd10)}">${pc(r.fwd10)}</td>`+
        `<td class="${cls(r.fwd20)}">${pc(r.fwd20)}</td></tr>`;
    });
  }else{
    ssb.innerHTML='<tr><td class="l dimc" colspan="8">今日資料不足（13 維未全數有效），相似日引擎暫停</td></tr>';
  }

  const sb=document.getElementById('statsBody');sb.innerHTML='';
  const rows=[
    [`模式起點（剛發生）`, st.onset, st.onset[0]?st.onset[0].n:0, ''],
    [`持續到第 ${cu.run_len} 天後（同目前）`, st.dayd, st.dayd_n_eps, ''],
    [`無條件基準（全樣本）`, st.baseline, m.valid_days, 'base'],
  ];
  rows.forEach(([lbl,blk,n,rc])=>{
    let h=`<tr class="${rc}"><td class="l">${lbl}</td><td>${n.toLocaleString()}</td>`;
    blk.forEach(s=>{h+=statCells(s)});
    sb.innerHTML+=h+'</tr>';
  });

  const eb=document.getElementById('epBody');eb.innerHTML='';
  d.episodes.slice().reverse().forEach(e=>{
    eb.innerHTML+=`<tr class="${e.ongoing?'ongoing':''}">`+
      `<td>${e.start}${e.ongoing?'<span class="tag-ongoing">進行中</span>':''}</td>`+
      `<td>${e.end}</td><td>${e.length}</td>`+
      `<td class="${cls(e.ep_ret)}">${pc(e.ep_ret)}</td>`+
      `<td class="${cls(e.fwd5)}">${pc(e.fwd5)}</td>`+
      `<td class="${cls(e.fwd10)}">${pc(e.fwd10)}</td>`+
      `<td class="${cls(e.fwd20)}">${pc(e.fwd20)}</td></tr>`;
  });

  const db=document.getElementById('distBody');db.innerHTML='';
  d.dist.forEach(p=>{
    db.innerHTML+=`<tr><td class="l">#${p.pid}｜${p.desc}</td>`+
      `<td>${p.days.toLocaleString()}</td><td>${p.freq.toFixed(1)}%</td></tr>`;
  });
}

async function refresh(){
  const btn=document.getElementById('btnRefresh'),st=document.getElementById('btnStatus');
  btn.disabled=true;st.textContent='計算中...';
  try{
    const r=await fetch('/api/analogue');
    render(await r.json());
    st.textContent='已更新 ('+new Date().toLocaleTimeString()+')';
  }catch(e){st.textContent='錯誤: '+e}
  finally{btn.disabled=false}
}

render(/*__DATA__*/);
</script>
</body>
</html>"""


class AnalogueHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/':
            try:
                data = market_analogue.compute()
                html = HTML_TEMPLATE.replace('/*__DATA__*/', json.dumps(data, ensure_ascii=False))
                self._respond(200, 'text/html; charset=utf-8', html.encode('utf-8'))
            except Exception as e:
                self._respond(500, 'text/plain; charset=utf-8', str(e).encode('utf-8'))
        elif self.path == '/api/analogue':
            try:
                body = json.dumps(market_analogue.compute(), ensure_ascii=False).encode('utf-8')
                self._respond(200, 'application/json; charset=utf-8', body)
            except Exception as e:
                body = json.dumps({'error': str(e)}).encode('utf-8')
                self._respond(500, 'application/json; charset=utf-8', body)
        else:
            self.send_error(404)

    def _respond(self, code, content_type, body):
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


if __name__ == '__main__':
    url = f'http://127.0.0.1:{PORT}'
    server = http.server.HTTPServer(('127.0.0.1', PORT), AnalogueHandler)
    print(f'[analogue-gui] Serving at {url}')
    print('[analogue-gui] Press Ctrl+C to stop')
    threading.Timer(0.5, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\n[analogue-gui] Stopped')
        server.server_close()
