"""
styles.py - التصميم v17.0 (خفيف جداً)
"""

def get_styles():
    return get_main_css()

def get_main_css():
    return """<style>
@import url('https://fonts.googleapis.com/css2?family=Tajawal:wght@400;700;900&display=swap');
*{font-family:'Tajawal',sans-serif!important}
.main .block-container{max-width:1400px;padding:1rem 2rem}
.stat-card{background:#1A1A2E;border-radius:12px;padding:16px;text-align:center;border:1px solid #333344}
.stat-card:hover{box-shadow:0 4px 16px rgba(108,99,255,.15);border-color:#6C63FF}
.stat-card .num{font-size:2.2rem;font-weight:900;margin:4px 0}
.stat-card .lbl{font-size:.85rem;color:#8B8B8B}
.cmp-table{width:100%;border-collapse:separate;border-spacing:0;border-radius:8px;overflow:hidden;font-size:.88rem}
.cmp-table thead th{background:#16213e;color:#fff;padding:10px 8px;font-weight:700;text-align:center;border-bottom:2px solid #6C63FF;position:sticky;top:0;z-index:10}
.cmp-table tbody tr:nth-child(even){background:rgba(26,26,46,.4)}
.cmp-table tbody tr:hover{background:rgba(108,99,255,.1)!important}
.cmp-table td{padding:8px 6px;text-align:center;border-bottom:1px solid rgba(51,51,68,.4);vertical-align:middle}
.td-our{background:rgba(108,99,255,.06)!important;border-right:3px solid #6C63FF;text-align:right!important;font-weight:600;color:#B8B4FF;max-width:250px;word-wrap:break-word}
.td-comp{background:rgba(255,152,0,.06)!important;border-left:3px solid #ff9800;text-align:right!important;font-weight:600;color:#FFD180;max-width:250px;word-wrap:break-word}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:.75rem;font-weight:700}
.b-high{background:rgba(255,23,68,.15);color:#FF1744;border:1px solid #FF1744}
.b-med{background:rgba(255,214,0,.15);color:#FFD600;border:1px solid #FFD600}
.b-low{background:rgba(0,200,83,.15);color:#00C853;border:1px solid #00C853}
.conf-bar{width:100%;height:6px;background:rgba(255,255,255,.08);border-radius:3px;overflow:hidden}
.conf-fill{height:100%;border-radius:3px}
.vs-row{display:grid;grid-template-columns:1fr 36px 1fr;gap:10px;align-items:center;padding:12px;background:#1A1A2E;border-radius:8px;margin:5px 0;border:1px solid #333344}
.vs-badge{background:linear-gradient(135deg,#667eea,#764ba2);color:#fff;width:32px;height:32px;border-radius:50%;display:flex;align-items:center;justify-content:center;font-weight:900;font-size:.7rem}
.our-s{text-align:right;padding:8px;background:rgba(108,99,255,.04);border-radius:6px;border-right:3px solid #6C63FF}
.comp-s{text-align:left;padding:8px;background:rgba(255,152,0,.04);border-radius:6px;border-left:3px solid #ff9800}
.action-btn{display:inline-block;padding:4px 10px;border-radius:6px;font-size:.75rem;font-weight:700;cursor:pointer;margin:2px;border:1px solid}
.btn-approve{background:rgba(0,200,83,.1);color:#00C853;border-color:#00C853}
.btn-remove{background:rgba(255,23,68,.1);color:#FF1744;border-color:#FF1744}
.btn-delay{background:rgba(255,152,0,.1);color:#ff9800;border-color:#ff9800}
.btn-export{background:rgba(108,99,255,.1);color:#6C63FF;border-color:#6C63FF}
.ai-box{background:#1A1A2E;padding:12px;border-radius:8px;border:1px solid #333344;margin:6px 0}
.paste-area{background:#0E1117;border:2px dashed #333344;border-radius:8px;padding:12px;min-height:80px}
.multi-comp{background:rgba(0,123,255,.06);border:1px solid rgba(0,123,255,.2);border-radius:6px;padding:8px;margin:4px 0}
section[data-testid="stSidebar"]{background:linear-gradient(180deg,#0E1117,#1A1A2E)}
#MainMenu,footer,header{visibility:hidden}
</style>"""


def stat_card(icon, label, value, color="#6C63FF"):
    return f'<div class="stat-card" style="border-top:3px solid {color}"><div style="font-size:1.3rem">{icon}</div><div class="num" style="color:{color}">{value}</div><div class="lbl">{label}</div></div>'


def vs_card(our_name, our_price, comp_name, comp_price, diff, comp_source=""):
    dc = "#FF1744" if diff > 0 else "#00C853" if diff < 0 else "#FFD600"
    src = f'<div style="font-size:.65rem;color:#666">{comp_source}</div>' if comp_source else ""
    return f'''<div class="vs-row">
<div class="our-s"><div style="font-size:.7rem;color:#8B8B8B">منتجنا</div><div style="font-weight:700;color:#B8B4FF;font-size:.9rem">{our_name}</div><div style="font-size:1.1rem;font-weight:900;color:#6C63FF;margin-top:2px">{our_price:.0f} ر.س</div></div>
<div class="vs-badge">VS</div>
<div class="comp-s"><div style="font-size:.7rem;color:#8B8B8B">المنافس</div><div style="font-weight:700;color:#FFD180;font-size:.9rem">{comp_name}</div><div style="font-size:1.1rem;font-weight:900;color:#ff9800;margin-top:2px">{comp_price:.0f} ر.س</div>{src}</div>
</div><div style="text-align:center;margin:2px 0"><span style="color:{dc};font-weight:700;font-size:.9rem">الفرق: {diff:+.0f} ر.س</span></div>'''
