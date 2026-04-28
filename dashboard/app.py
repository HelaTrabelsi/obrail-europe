import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os, json, requests
from datetime import datetime
from sqlalchemy import create_engine

st.set_page_config(
    page_title="ObRail Europe",
    page_icon="🚆",
    layout="wide",
    initial_sidebar_state="collapsed"
)

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

html, body, [class*="css"], .stApp {
    font-family: 'Inter', system-ui, sans-serif !important;
    background: #0b1017 !important;
    color: #7a8fa6 !important;
    -webkit-font-smoothing: antialiased !important;
}

#MainMenu, footer, header, .stDeployButton,
[data-testid="collapsedControl"],
[data-testid="stSidebar"],
section[data-testid="stSidebar"] { display: none !important; }

.main .block-container {
    max-width: 1280px !important;
    margin: 0 auto !important;
    padding: 0 3rem 5rem !important;
}

/* ── Spacing reset ── */
div[data-testid="stVerticalBlock"] { gap: 0 !important; }
.element-container { margin: 0 0 1rem !important; padding: 0 !important; }
.stVerticalBlock > [data-testid="stVerticalBlockBorderWrapper"] { padding: 0 !important; }
div[data-testid="column"] { padding: 0 8px !important; }
[data-testid="stHorizontalBlock"] { gap: 0 !important; align-items: stretch !important; }

/* ═══ NAV ════════════════════════════════════════ */
.nav {
    height: 56px;
    display: flex; align-items: center; justify-content: space-between;
    padding: 0 3rem;
    background: #0b1017;
    border-bottom: 1px solid rgba(255,255,255,0.06);
    position: sticky; top: 0; z-index: 999;
    margin-left: -3rem; margin-right: -3rem;
}
.nav-brand { display: flex; align-items: center; gap: 11px; }
.nav-mark {
    width: 30px; height: 30px; border-radius: 7px;
    background: linear-gradient(135deg, #00c98d 0%, #0096d6 100%);
    display: flex; align-items: center; justify-content: center;
    font-size: 0.78rem; font-weight: 700; color: #fff; flex-shrink: 0;
    letter-spacing: 0.02em;
}
.nav-name { font-size: 0.88rem; font-weight: 600; color: #dde6f0; letter-spacing: -0.01em; }
.nav-sub  { font-size: 0.57rem; color: #00c98d; text-transform: uppercase; letter-spacing: 0.13em; font-weight: 600; margin-top: 1px; }
.nav-pill {
    display: flex; align-items: center; gap: 5px;
    font-size: 0.67rem; color: #354d62;
    background: #111c28; border: 1px solid rgba(255,255,255,0.06);
    border-radius: 5px; padding: 3px 10px;
}
.live-dot { width: 5px; height: 5px; border-radius: 50%; background: #00c98d; animation: bl 2.5s infinite; }
@keyframes bl { 0%,100%{opacity:1} 50%{opacity:.2} }

/* ═══ TABS ════════════════════════════════════════ */
[data-testid="stRadio"] {
    padding: 0 3rem !important;
    margin: 0 -3rem 0 !important;
    background: #0b1017;
    border-bottom: 1px solid rgba(255,255,255,0.06);
}
[data-testid="stRadio"] > label { display: none !important; }
[data-testid="stRadio"] [role="radiogroup"] {
    display: flex !important; gap: 0 !important;
    overflow-x: auto !important; scrollbar-width: none !important;
}
[data-testid="stRadio"] [role="radiogroup"]::-webkit-scrollbar { display: none !important; }
[data-testid="stRadio"] [role="radiogroup"] label {
    display: flex !important; align-items: center !important;
    padding: 13px 20px !important;
    font-size: 0.77rem !important; font-weight: 500 !important;
    color: #354d62 !important;
    border: none !important; border-bottom: 2px solid transparent !important;
    border-radius: 0 !important; background: transparent !important;
    white-space: nowrap !important; cursor: pointer !important;
    transition: color .15s, border-color .15s !important;
}
[data-testid="stRadio"] [role="radiogroup"] label > span:first-child,
[data-testid="stRadio"] [role="radiogroup"] label > span:first-child *,
[data-testid="stRadio"] input[type="radio"] { display: none !important; }
[data-testid="stRadio"] [role="radiogroup"] label > div,
[data-testid="stRadio"] [role="radiogroup"] label p {
    font-size: 0.77rem !important; font-weight: 500 !important;
    color: inherit !important; margin: 0 !important; padding: 0 !important;
}
[data-testid="stRadio"] [role="radiogroup"] label:has(input:checked) {
    color: #00c98d !important; border-bottom-color: #00c98d !important; font-weight: 600 !important;
}
[data-testid="stRadio"] [role="radiogroup"] label:hover:not(:has(input:checked)) { color: #5a7a94 !important; }

/* ═══ PAGE HEADER ═════════════════════════════════ */
.ph { padding: 32px 0 24px; }
.ph-eye { font-size: 0.59rem; font-weight: 700; letter-spacing: 0.18em; text-transform: uppercase; color: #00c98d; margin-bottom: 6px; }
.ph-title { font-size: 1.5rem; font-weight: 700; color: #dde6f0; letter-spacing: -0.025em; line-height: 1.2; margin-bottom: 5px; }
.ph-title em { font-style: normal; color: #00c98d; }
.ph-sub { font-size: 0.8rem; color: #2d4255; font-weight: 400; line-height: 1.55; }

/* ═══ KPI CARDS ══════════════════════════════════ */
.krow { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px,1fr)); gap: 12px; margin-bottom: 28px; }

.kcard {
    background: #111c28;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px;
    padding: 20px 22px 18px;
    position: relative; overflow: hidden;
}
.kcard::before {
    content: ''; position: absolute; top: 0; left: 0; right: 0; height: 2px;
    background: var(--a, linear-gradient(90deg,#00c98d,#0096d6));
}
.kcard-val {
    font-size: 1.6rem; font-weight: 700; color: #dde6f0;
    letter-spacing: -0.04em; line-height: 1; margin-bottom: 5px; margin-top: 2px;
}
.kcard-lbl {
    font-size: 0.6rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.1em; color: #1e3247;
}
.kcard-delta { font-size: 0.68rem; color: #00c98d; margin-top: 4px; font-weight: 600; }

/* ═══ SECTION LABEL ══════════════════════════════ */
.sec {
    font-size: 0.6rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.13em; color: #1e3247;
    margin: 24px 0 10px;
    display: flex; align-items: center; gap: 10px;
}
.sec::after { content: ''; flex: 1; height: 1px; background: rgba(255,255,255,0.05); }

/* ═══ CHART CARD ═════════════════════════════════ */
.ccard {
    background: #111c28;
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px;
    padding: 20px 18px 12px;
    margin-bottom: 12px;
}
.ccard-title {
    font-size: 0.72rem; font-weight: 600; color: #5a7a94;
    text-transform: uppercase; letter-spacing: 0.08em; margin-bottom: 14px;
}

/* ═══ STAT ROW (inline metrics) ══════════════════ */
.srow {
    display: grid; grid-template-columns: repeat(4,1fr);
    gap: 1px; background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px; overflow: hidden; margin-bottom: 20px;
}
.srow-item {
    background: #111c28; padding: 16px 18px;
}
.srow-val { font-size: 1.15rem; font-weight: 700; color: #dde6f0; letter-spacing: -0.03em; }
.srow-lbl { font-size: 0.58rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #1e3247; margin-top: 3px; }

/* ═══ FORM WIDGETS ═══════════════════════════════ */
label, [data-testid="stWidgetLabel"] {
    color: #1e3247 !important; font-size: 0.6rem !important; font-weight: 700 !important;
    text-transform: uppercase !important; letter-spacing: 0.1em !important; margin-bottom: 5px !important;
}
.stSelectbox > div > div {
    background: #111c28 !important; border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 7px !important; color: #9eb8cc !important; font-size: 0.81rem !important;
    min-height: 36px !important;
}
.stSelectbox > div > div:focus-within {
    border-color: rgba(0,201,141,0.4) !important; box-shadow: 0 0 0 2px rgba(0,201,141,0.07) !important;
}
.stSelectbox svg { fill: #2d4255 !important; }
.stTextInput > div > div > input {
    background: #111c28 !important; border: 1px solid rgba(255,255,255,0.07) !important;
    border-radius: 7px !important; color: #9eb8cc !important;
    font-size: 0.81rem !important; height: 36px !important; padding: 0 12px !important;
}
.stTextInput > div > div > input::placeholder { color: #1e3247 !important; }
.stTextInput > div > div > input:focus {
    border-color: rgba(0,201,141,0.4) !important; box-shadow: 0 0 0 2px rgba(0,201,141,0.07) !important; outline: none !important;
}
.stSlider { padding: 0 !important; }
[data-baseweb="slider"] [data-testid="stSliderTrack"] { height: 3px !important; background: rgba(255,255,255,0.07) !important; }
[data-baseweb="slider"] [data-testid="stSliderTrack"] > div { height: 3px !important; }
[data-baseweb="slider"] [data-testid="stSliderTrack"] > div:first-child { background: rgba(255,255,255,0.07) !important; }
[data-baseweb="slider"] [data-testid="stSliderTrack"] > div:last-child { background: #00c98d !important; }
[data-baseweb="slider"] div[role="slider"] {
    background: #00c98d !important; border: 2px solid #0b1017 !important;
    width: 14px !important; height: 14px !important; box-shadow: 0 0 0 2px rgba(0,201,141,0.3) !important;
}
[data-testid="stTickBar"] { display: none !important; }

/* Metrics */
[data-testid="stMetric"] { padding: 0 !important; }
[data-testid="stMetricValue"] {
    font-size: 1.3rem !important; font-weight: 700 !important;
    color: #dde6f0 !important; letter-spacing: -0.03em !important; line-height: 1.1 !important;
}
[data-testid="stMetricLabel"] {
    color: #1e3247 !important; font-size: 0.59rem !important;
    text-transform: uppercase !important; letter-spacing: 0.1em !important; font-weight: 700 !important;
}

/* DataFrame */
.stDataFrame { border-radius: 8px !important; border: 1px solid rgba(255,255,255,0.06) !important; overflow: hidden !important; }

/* Download */
.stDownloadButton > button {
    background: rgba(0,201,141,0.07) !important; border: 1px solid rgba(0,201,141,0.2) !important;
    color: #00c98d !important; border-radius: 6px !important;
    font-size: 0.73rem !important; font-weight: 600 !important; padding: 5px 14px !important; height: 30px !important;
}
.stDownloadButton > button:hover { background: rgba(0,201,141,0.13) !important; }

/* Source cards */
.src {
    background: #111c28; border: 1px solid rgba(255,255,255,0.06);
    border-radius: 10px; padding: 18px 10px; text-align: center;
    height: 100%;
}

/* Info / RGPD */
.ibox {
    background: rgba(0,201,141,0.05); border-left: 2px solid #00c98d;
    border-radius: 6px; padding: 12px 16px;
    font-size: 0.79rem; color: #3a9e74; line-height: 1.6;
}
.rgpd {
    background: #111c28; border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px; padding: 16px 18px; margin-top: 12px;
    font-size: 0.74rem; color: #354d62; line-height: 1.85;
}
.rgpd-h { font-size: 0.6rem; font-weight: 700; color: #00c98d; text-transform: uppercase; letter-spacing: 0.1em; margin-bottom: 8px; }

hr { border: none !important; border-top: 1px solid rgba(255,255,255,0.05) !important; margin: 20px 0 !important; }
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.08); border-radius: 4px; }
</style>
""", unsafe_allow_html=True)

# Config 
DB_URL = (
    f"postgresql://{os.getenv('DB_USER','postgres')}:{os.getenv('DB_PASSWORD','postgres')}"
    f"@{os.getenv('DB_HOST','localhost')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME','obrail_db')}"
)
PAGES  = ["Accueil","Horaires","Statistiques","Liaisons","CO2","Qualite"]
COLORS = ['#00c98d','#0096d6','#f59e0b','#8b5cf6','#ef4444','#34d399','#60a5fa']
CFG    = {"displayModeBar": False, "responsive": True}

_BL = dict(
    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family='Inter, system-ui', color='#354d62', size=11),
    margin=dict(l=4, r=4, t=10, b=4),
    legend=dict(bgcolor='rgba(0,0,0,0)', font=dict(color='#4a6275', size=10)),
)
_AX  = dict(
    gridcolor='rgba(255,255,255,0.04)', linecolor='rgba(255,255,255,0.06)',
    tickcolor='rgba(255,255,255,0.06)', zerolinecolor='rgba(255,255,255,0.03)',
    tickfont=dict(color='#354d62', size=10),
)
_CAT = dict(**_AX, categoryorder='total ascending')

def L(h=None, **extra):
    out = {**_BL, **extra}
    if h: out['height'] = h
    return out

def chart(fig, h=None, cat_y=False, no_cs=False, no_legend=False):
    kw = {'xaxis': _AX, 'yaxis': _CAT if cat_y else _AX}
    if no_cs:     kw['coloraxis_showscale'] = False
    if no_legend: kw['showlegend'] = False
    fig.update_layout(**L(h, **kw))
    return fig

# Data 
@st.cache_data(ttl=300, show_spinner=False)
def load_data():
    try:
        df = pd.read_sql("SELECT * FROM dessertes", create_engine(DB_URL))
    except Exception:
        df = None
        for root, _, files in os.walk("../data/processed"):
            for f in files:
                if f.endswith('.parquet'):
                    df = pd.read_parquet(os.path.join(root, f)); break
    if df is None: return None
    if 'emissions_co2_gkm' not in df.columns: df['emissions_co2_gkm'] = 3.8
    df['co2_emission_kg'] = df['emissions_co2_gkm'] * df['distance_km'] / 1000
    df = df.rename(columns={
        'operateur_nom':'operator','gare_depart_nom':'origin_station',
        'gare_arrivee_nom':'destination_station'}, errors='ignore')
    return df

@st.cache_data(ttl=300)
def load_stats():
    p = "../data/transformed/stats.json"
    return json.load(open(p)) if os.path.exists(p) else None

# Components
def top_nav(api_ok):
    status = ('<span class="live-dot"></span> En ligne'
              if api_ok else '<span style="width:5px;height:5px;border-radius:50%;background:#ef4444;display:inline-block"></span> Hors ligne')
    st.markdown(f"""
    <div class="nav">
      <div class="nav-brand">
        <div class="nav-mark">OR</div>
        <div><div class="nav-name">ObRail Europe</div>
             <div class="nav-sub">Observatoire Ferroviaire</div></div>
      </div>
      <div class="nav-pill">{status}</div>
    </div>""", unsafe_allow_html=True)

    sel = st.radio("nav", PAGES,
                   index=PAGES.index(st.session_state.get('page', PAGES[0])),
                   horizontal=True, label_visibility="collapsed", key="nr")
    if sel != st.session_state.get('page'):
        st.session_state['page'] = sel; st.rerun()
    return st.session_state['page']

def ph(eye, title, sub):
    st.markdown(f'<div class="ph"><div class="ph-eye">{eye}</div>'
                f'<div class="ph-title">{title}</div>'
                f'<div class="ph-sub">{sub}</div></div>', unsafe_allow_html=True)

def sec(txt):
    st.markdown(f'<div class="sec">{txt}</div>', unsafe_allow_html=True)

def kpis(cards):
    html = '<div class="krow">'
    for val, label, ac, delta in cards:
        d = f'<div class="kcard-delta">{delta}</div>' if delta else ''
        html += (f'<div class="kcard" style="--a:{ac}">'
                 f'<div class="kcard-lbl">{label}</div>'
                 f'<div class="kcard-val">{val}</div>{d}</div>')
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

def srow(items):
    html = '<div class="srow">'
    for val, label in items:
        html += f'<div class="srow-item"><div class="srow-val">{val}</div><div class="srow-lbl">{label}</div></div>'
    html += '</div>'
    st.markdown(html, unsafe_allow_html=True)

def ccard(title, fig_fn):
    st.markdown(f'<div class="ccard"><div class="ccard-title">{title}</div>', unsafe_allow_html=True)
    fig_fn()
    st.markdown('</div>', unsafe_allow_html=True)

#  Pages
def page_accueil(df):
    ph("Tableau de bord", "ObRail <em>Europe</em>",
       "Donnees ferroviaires harmonisees — mobilite durable et bas-carbone")

    nb_n = int((df['type_service']=='Nuit').sum()) if 'type_service' in df.columns else 0
    nb_j = int((df['type_service']=='Jour').sum()) if 'type_service' in df.columns else 0
    co2e = df['distance_km'].sum()*(285-df['emissions_co2_gkm'].mean())/1_000_000

    kpis([
        (f"{len(df):,}",   "Trajets total",    "linear-gradient(90deg,#00c98d,#0096d6)", None),
        (f"{nb_n:,}",      "Trains de nuit",   "linear-gradient(90deg,#6366f1,#818cf8)", None),
        (f"{nb_j:,}",      "Trains de jour",   "linear-gradient(90deg,#f59e0b,#fbbf24)", None),
        (f"{df['operator'].nunique()}", "Operateurs", "linear-gradient(90deg,#0096d6,#38bdf8)", None),
        (f"{co2e:,.0f} t", "CO2 evite vs avion","linear-gradient(90deg,#00c98d,#34d399)", "- 85% vs avion"),
    ])

    c1, c2 = st.columns(2)
    with c1:
        sec("Repartition Jour / Nuit par operateur")
        if 'type_service' in df.columns:
            g = df.groupby(['operator','type_service']).size().reset_index(name='n')
            fig = px.bar(g, x='operator', y='n', color='type_service', barmode='group',
                         color_discrete_map={'Jour':'#f59e0b','Nuit':'#6366f1'},
                         labels={'n':'Trajets','operator':'','type_service':''})
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(chart(fig, h=280), use_container_width=True, config=CFG)
    with c2:
        sec("Parts de marche")
        op = df['operator'].value_counts().reset_index(); op.columns=['Op','Nb']
        fig = px.pie(op, names='Op', values='Nb', hole=.6, color_discrete_sequence=COLORS)
        fig.update_traces(textposition='inside', textinfo='percent+label', textfont_size=11)
        fig.update_layout(**L(h=280))
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    sec("Sources integrees")
    sources = [("FR","SNCF TER","#00c98d"),("FR","SNCF Intercites","#00c98d"),
               ("DE","Deutsche Bahn","#0096d6"),("DE","DB Regional","#0096d6"),("BE","SNCB","#f59e0b")]
    cols = st.columns(5)
    for i,(flag,name,color) in enumerate(sources):
        with cols[i]:
            st.markdown(
                f'<div class="src" style="border-top:2px solid {color}">'
                f'<div style="font-size:.62rem;font-weight:700;color:{color};letter-spacing:.1em;margin-bottom:6px">{flag}</div>'
                f'<div style="font-size:.82rem;font-weight:600;color:#9eb8cc;margin-bottom:3px">{name}</div>'
                f'<div style="font-size:.59rem;color:#1e3247;text-transform:uppercase;letter-spacing:.07em;font-weight:600">GTFS</div>'
                f'</div>', unsafe_allow_html=True)


def page_horaires(df):
    ph("Recherche", "<em>Horaires</em> & trajets",
       "Filtrez par gare, operateur ou distance")

    gares_dep = ["Toutes"] + sorted(df['origin_station'].dropna().unique().tolist())
    gares_arr = ["Toutes"] + sorted(df['destination_station'].dropna().unique().tolist())

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        op = st.selectbox("Operateur", ["Tous"]+sorted(df['operator'].dropna().unique()))
    with c2:
        ts = st.selectbox("Type service", ["Tous","Jour","Nuit"]) if 'type_service' in df.columns else "Tous"
    with c3:
        dep = st.selectbox("Gare depart", gares_dep)
    with c4:
        arr = st.selectbox("Gare arrivee", gares_arr)

    dist = st.slider("Distance (km)", 0, 3000, (0,3000), step=50)


    f = df.copy()
    if op  != "Tous": f = f[f['operator']==op]
    if ts  != "Tous" and 'type_service' in f.columns: f = f[f['type_service']==ts]
    if dep != "Toutes": f = f[f['origin_station']==dep]
    if arr != "Toutes": f = f[f['destination_station']==arr]
    if 'type_ligne' in f.columns: pass  # type_ligne filtre supprime
    f = f[(f['distance_km']>=dist[0]) & (f['distance_km']<=dist[1])]

    st.markdown("<br>", unsafe_allow_html=True)

    srow([
        (f"{len(f):,}", "Resultats"),
        (f"{f['distance_km'].sum():,.0f} km", "Distance totale"),
        (f"{f['distance_km'].mean():.0f} km", "Distance moyenne"),
        (f"{f['co2_emission_kg'].sum():,.0f} kg", "CO2 total"),
    ])

    if f.empty:
        st.markdown('<div class="ibox">Aucun trajet trouve.</div>', unsafe_allow_html=True)
        return

    dcols = ['operator','origin_station','destination_station']
    if 'heure_depart' in f.columns: dcols += ['heure_depart','heure_arrivee']
    dcols += ['distance_km','co2_emission_kg']
    if 'type_service' in f.columns: dcols.append('type_service')
    ds = f[dcols].head(500).copy()
    ds['distance_km']     = ds['distance_km'].round(1)
    ds['co2_emission_kg'] = ds['co2_emission_kg'].round(3)
    ds = ds.rename(columns={
        'operator':'Operateur','origin_station':'Depart','destination_station':'Arrivee',
        'heure_depart':'H. depart','heure_arrivee':'H. arrivee',
        'distance_km':'Dist. km','co2_emission_kg':'CO2 kg','type_service':'Type'})
    st.dataframe(ds, use_container_width=True, height=380)

    cl,_ = st.columns([1,6])
    with cl: st.download_button("Export CSV", ds.to_csv(index=False).encode(), "trajets.csv","text/csv")

    if 'heure_depart' in f.columns:
        h = f['heure_depart'].dropna().astype(str); h = h[h.str.match(r'^\d{2}:\d{2}')]
        if not h.empty:
            hc = h.str[:2].astype(int).value_counts().sort_index().reset_index(); hc.columns=['Heure','Nb']
            sec("Departs par heure")
            fig = px.bar(hc, x='Heure', y='Nb', color='Nb',
                         color_continuous_scale=[[0,'#0a2818'],[1,'#00c98d']],
                         labels={'Nb':'Trajets','Heure':'Heure'})
            fig.update_traces(marker_line_width=0)
            st.plotly_chart(chart(fig, h=200, no_cs=True), use_container_width=True, config=CFG)


def page_statistiques(df):
    ph("Analyse", "<em>Statistiques</em> reseau",
       "Couverture ferroviaire integree par operateur et type de service")

    nb_n = int((df['type_service']=='Nuit').sum()) if 'type_service' in df.columns else 0
    nb_j = int((df['type_service']=='Jour').sum()) if 'type_service' in df.columns else 0
    dist_moy = df['distance_km'].mean()
    dist_max = df['distance_km'].max()

    kpis([
        (f"{len(df):,}",    "Trajets total",   "linear-gradient(90deg,#00c98d,#0096d6)", None),
        (f"{nb_n:,}",       "Trains de nuit",  "linear-gradient(90deg,#6366f1,#818cf8)", None),
        (f"{nb_j:,}",       "Trains de jour",  "linear-gradient(90deg,#f59e0b,#fbbf24)", None),
        (f"{dist_moy:.0f} km","Distance moy.", "linear-gradient(90deg,#0096d6,#38bdf8)", None),
        (f"{dist_max:.0f} km","Distance max.", "linear-gradient(90deg,#354d62,#4a6275)", None),
    ])

    c1, c2 = st.columns(2)
    with c1:
        sec("Top 15 gares de depart")
        top = df['origin_station'].value_counts().head(15).reset_index(); top.columns=['Gare','Nb']
        fig = px.bar(top, x='Nb', y='Gare', orientation='h', color='Nb',
                     color_continuous_scale=[[0,'#0a2818'],[1,'#00c98d']])
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(chart(fig, h=360, cat_y=True, no_cs=True), use_container_width=True, config=CFG)
    with c2:
        sec("Top 15 gares d'arrivee")
        top = df['destination_station'].value_counts().head(15).reset_index(); top.columns=['Gare','Nb']
        fig = px.bar(top, x='Nb', y='Gare', orientation='h', color='Nb',
                     color_continuous_scale=[[0,'#0a1e30'],[1,'#0096d6']])
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(chart(fig, h=360, cat_y=True, no_cs=True), use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    c3, c4 = st.columns(2)
    with c3:
        if 'type_service' in df.columns:
            sec("Jour vs Nuit")
            ts = df['type_service'].value_counts().reset_index(); ts.columns=['Type','Nb']
            fig = px.pie(ts, names='Type', values='Nb', hole=.6, color='Type',
                         color_discrete_map={'Jour':'#f59e0b','Nuit':'#6366f1'})
            fig.update_traces(textinfo='percent+label')
            fig.update_layout(**L(h=240))
            st.plotly_chart(fig, use_container_width=True, config=CFG)
    with c4:
        sec("Distribution des distances")
        fig = px.histogram(df, x='distance_km', nbins=40, color_discrete_sequence=['#00c98d'],
                           labels={'distance_km':'Distance (km)'})
        fig.update_traces(marker_line_width=0, opacity=.85)
        st.plotly_chart(chart(fig, h=240), use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    sec("Tableau comparatif operateurs")
    grp = df.groupby('operator').agg(
        Trajets=('operator','count'), Dist_moy=('distance_km','mean'),
        CO2_moy=('co2_emission_kg','mean'), Dist_max=('distance_km','max')
    ).round(2).reset_index()
    st.dataframe(grp, use_container_width=True)


def page_liaisons(df):
    ph("Flux", "Directions & <em>Liaisons</em>",
       "Analyse des flux et connexions ferroviaires europeennes")

    c1,c2,c3 = st.columns([2,2,1])
    with c1: sel_op = st.selectbox("Operateur", ["Tous"]+sorted(df['operator'].dropna().unique()), key="l_op")
    with c2: sel_ts = st.selectbox("Type service", ["Tous","Jour","Nuit"], key="l_ts") if 'type_service' in df.columns else "Tous"
    with c3: top_n  = st.slider("Top N", 5, 30, 20)

    f = df.copy()
    if sel_op != "Tous": f = f[f['operator']==sel_op]
    if sel_ts != "Tous" and 'type_service' in f.columns: f = f[f['type_service']==sel_ts]
    f['liaison'] = f['origin_station'] + " > " + f['destination_station']

    st.markdown("<br>", unsafe_allow_html=True)

    cl, cr = st.columns(2)
    with cl:
        sec(f"Top {top_n} liaisons")
        top = f['liaison'].value_counts().head(top_n).reset_index(); top.columns=['Liaison','Nb']
        fig = px.bar(top, x='Nb', y='Liaison', orientation='h', color='Nb',
                     color_continuous_scale=[[0,'#0a2818'],[1,'#00c98d']])
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(chart(fig, h=460, cat_y=True, no_cs=True), use_container_width=True, config=CFG)
    with cr:
        if 'heure_depart' in f.columns and 'type_service' in f.columns:
            sec("Distance vs heure de depart")
            hd = f.dropna(subset=['heure_depart','distance_km']).copy()
            hd['heure'] = hd['heure_depart'].astype(str).str[:2]
            hd = hd[hd['heure'].str.match(r'^\d{2}$')]; hd['heure'] = hd['heure'].astype(int)
            if not hd.empty:
                fig = px.scatter(hd.sample(min(len(hd),5000)),
                                 x='heure', y='distance_km', color='type_service',
                                 color_discrete_map={'Jour':'#f59e0b','Nuit':'#6366f1'},
                                 opacity=.28,
                                 labels={'heure':'Heure','distance_km':'Distance (km)','type_service':''})
                st.plotly_chart(chart(fig, h=460), use_container_width=True, config=CFG)

    if 'type_service' in f.columns:
        st.markdown("<br>", unsafe_allow_html=True)
        sec("Heatmap operateur x type de service")
        heat  = f.groupby(['operator','type_service']).size().reset_index(name='n')
        pivot = heat.pivot(index='operator', columns='type_service', values='n').fillna(0)
        fig = go.Figure(go.Heatmap(
            z=pivot.values, x=pivot.columns.tolist(), y=pivot.index.tolist(),
            colorscale=[[0,'#0b1017'],[.5,'#00c98d'],[1,'#34d399']],
            text=pivot.values.astype(int), texttemplate='%{text}',
            textfont=dict(size=12, color='white'), showscale=False))
        fig.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
                          font=dict(family='Inter', color='#354d62', size=11),
                          margin=dict(l=4,r=4,t=10,b=4), height=200)
        fig.update_xaxes(**_AX); fig.update_yaxes(**_AX)
        st.plotly_chart(fig, use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    sec("Resume liaisons principales")
    s = f.groupby(['origin_station','destination_station']).agg(
        Frequence=('distance_km','count'), Dist_moy=('distance_km','mean'),
        CO2_moy=('co2_emission_kg','mean')
    ).round(2).sort_values('Frequence',ascending=False).head(50).reset_index()
    s.columns = ['Depart','Arrivee','Frequence','Dist. moy (km)','CO2 moy (kg)']
    st.dataframe(s, use_container_width=True)


def page_co2(df):
    ph("Environnement", "Emissions <em>CO2</em>",
       "Impact carbone compare — rail vs aviation intra-europeenne")

    td = df['distance_km'].sum()
    nb = len(df)


    co2_train_pkm = 14    # g CO2 / passager-km  (ADEME 2023)
    co2_avion_pkm = 258   # g CO2 / passager-km  (ADEME 2023 court-courrier)

    # Base de comparaison : distance totale x intensite carbone
    tc  = td * co2_train_pkm / 1000   # kg CO2 total si ce trafic en train
    ca  = td * co2_avion_pkm / 1000   # kg CO2 total si ce meme trafic en avion
    ev  = ca - tc
    pct = ev / ca * 100 if ca > 0 else 0
    ratio = round(co2_avion_pkm / co2_train_pkm)

    kpis([
        (f"{tc:,.0f} kg",          "CO2 si tout en train",        "linear-gradient(90deg,#00c98d,#34d399)", f"{co2_train_pkm:.1f} g/passager-km"),
        (f"{ca:,.0f} kg",          "CO2 si tout en avion",        "linear-gradient(90deg,#ef4444,#f87171)", f"{co2_avion_pkm} g/passager-km"),
        (f"{ev:,.0f} kg",          "CO2 evite",                   "linear-gradient(90deg,#0096d6,#38bdf8)", f"- {pct:.0f}% vs avion"),
        (f"x {ratio}",             "Avion plus emetteur",         "linear-gradient(90deg,#f59e0b,#fbbf24)", f"{co2_avion_pkm} / {co2_train_pkm:.1f} g/km"),
    ])

    sec("Comparatif modal (g CO2 / passager-km)")
    comp = pd.DataFrame({
        'Mode':['Train electrique','Train moy. UE','Avion long-courrier','Avion court-courrier'],
        'gkm':[6,14,195,255],'color':['#00c98d','#0096d6','#f87171','#ef4444']})
    fig = px.bar(comp, x='Mode', y='gkm', color='Mode',
                 color_discrete_sequence=comp['color'].tolist(), text='gkm')
    fig.update_traces(texttemplate='%{text}g', textposition='outside', marker_line_width=0)
    fig.update_layout(**L(h=240), showlegend=False)
    fig.update_xaxes(**_AX); fig.update_yaxes(**_AX)
    st.plotly_chart(fig, use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        sec("Distribution des emissions")
        fig = px.histogram(df, x='co2_emission_kg', nbins=50,
                           color_discrete_sequence=['#00c98d'],
                           labels={'co2_emission_kg':'CO2 (kg)'})
        m = df['co2_emission_kg'].mean()
        fig.add_vline(x=m, line_dash='dash', line_color='#f59e0b',
                      annotation_text=f'Moy:{m:.2f}kg', annotation_font_color='#f59e0b')
        fig.update_traces(marker_line_width=0, opacity=.85)
        st.plotly_chart(chart(fig, h=240), use_container_width=True, config=CFG)
    with c2:
        sec("Top 10 trajets emetteurs")
        top10 = df.nlargest(10,'co2_emission_kg').copy()
        top10['t'] = top10['origin_station'].str[:15]+" > "+top10['destination_station'].str[:15]
        fig = px.bar(top10, x='co2_emission_kg', y='t', orientation='h', color='co2_emission_kg',
                     color_continuous_scale=[[0,'#4a0000'],[1,'#ef4444']],
                     labels={'co2_emission_kg':'CO2 (kg)','t':''})
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(chart(fig, h=240, cat_y=True, no_cs=True), use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    c3, c4 = st.columns(2)
    with c3:
        sec("Intensite carbone par operateur")
        oc = df.groupby('operator').agg(co2=('co2_emission_kg','sum'),dist=('distance_km','sum')).reset_index()
        oc['gkm'] = (oc['co2']/oc['dist']*1000).round(2)
        fig = px.bar(oc.sort_values('gkm'), x='operator', y='gkm', color='gkm',
                     color_continuous_scale=[[0,'#0a2818'],[.5,'#00c98d'],[1,'#fbbf24']],
                     labels={'operator':'','gkm':'g CO2/km'})
        fig.update_traces(marker_line_width=0)
        st.plotly_chart(chart(fig, h=240, no_cs=True), use_container_width=True, config=CFG)
    with c4:
        sec("Distance vs CO2")
        cc = 'type_service' if 'type_service' in df.columns else None
        cdm = {'Jour':'#f59e0b','Nuit':'#6366f1'} if cc else {}
        fig = px.scatter(df.sample(min(len(df),8000)),
                         x='distance_km', y='co2_emission_kg',
                         color=cc, color_discrete_map=cdm or None,
                         color_discrete_sequence=COLORS if not cdm else None,
                         opacity=.28,
                         labels={'distance_km':'Distance (km)','co2_emission_kg':'CO2 (kg)','type_service':''})
        st.plotly_chart(chart(fig, h=240), use_container_width=True, config=CFG)


def page_qualite(df, stats):
    ph("Controle", "Qualite des <em>Donnees</em>",
       "Completude, tracabilite ETL et conformite RGPD")

    if stats:
        kpis([
            (f"{stats.get('avant_doublons',0):,}",         "Enregistrements bruts", "linear-gradient(90deg,#0096d6,#38bdf8)", None),
            (f"{stats.get('apres_doublons',0):,}",          "Apres nettoyage",       "linear-gradient(90deg,#00c98d,#34d399)", None),
            (f"{stats.get('doublons_supprimes',0):,}",      "Doublons supprimes",    "linear-gradient(90deg,#f59e0b,#fbbf24)", None),
            (f"{stats.get('sans_horaires_supprimes',0):,}", "Sans horaires exclus",  "linear-gradient(90deg,#ef4444,#f87171)", None),
        ])

    st.markdown("<br>", unsafe_allow_html=True)
    cl, cr = st.columns([3,2])
    with cl:
        sec("Completude par champ (%)")
        comp = pd.DataFrame({
            'Champ': df.columns,
            'Pct':   ((1-df.isnull().sum()/len(df))*100).round(1)
        }).sort_values('Pct', ascending=True)
        fig = px.bar(comp, x='Pct', y='Champ', orientation='h', color='Pct', text='Pct',
                     color_continuous_scale=[[0,'#4a0000'],[.6,'#f59e0b'],[1,'#00c98d']],
                     range_color=[0,100])
        fig.add_vline(x=95, line_dash='dash', line_color='#00c98d',
                      annotation_text='95%', annotation_font_color='#00c98d')
        fig.update_traces(texttemplate='%{text}%', textposition='outside', marker_line_width=0)
        fig.update_layout(**L(h=400, coloraxis_showscale=False))
        fig.update_xaxes(**_AX, range=[0,112]); fig.update_yaxes(**_AX)
        st.plotly_chart(fig, use_container_width=True, config=CFG)
    with cr:
        st.markdown(
            f'<div style="background:#111c28;border:1px solid rgba(255,255,255,.06);border-radius:8px;'
            f'padding:16px 18px;margin-bottom:12px">'
            f'<div style="font-size:.58rem;font-weight:700;text-transform:uppercase;letter-spacing:.1em;color:#1e3247;margin-bottom:4px">Mise a jour</div>'
            f'<div style="font-size:1.1rem;font-weight:700;color:#dde6f0">{datetime.now().strftime("%d/%m/%Y %H:%M")}</div></div>',
            unsafe_allow_html=True)
        miss = df.isnull().sum(); miss = miss[miss>0]
        if miss.empty:
            st.markdown('<div class="ibox">Aucune valeur manquante detectee.</div>', unsafe_allow_html=True)
        else:
            st.dataframe(miss.to_frame("Nb manquants"), use_container_width=True)
        st.markdown(
            '<div class="rgpd"><div class="rgpd-h">Conformite RGPD</div>'
            'Aucune donnee personnelle traitee<br>'
            'Sources open data publiques (ODbL)<br>'
            'Table etl_logs — tracabilite complete<br>'
            'Sources documentees et auditables</div>',
            unsafe_allow_html=True)

    sc = 'source_donnee' if 'source_donnee' in df.columns else ('source' if 'source' in df.columns else None)
    if sc:
        st.markdown("<br>", unsafe_allow_html=True)
        sec("Volume par source")
        src = df[sc].value_counts().reset_index(); src.columns=['Source','Nb']
        fig = px.bar(src, x='Source', y='Nb', color='Nb', text='Nb',
                     color_continuous_scale=[[0,'#0a1e30'],[1,'#0096d6']])
        fig.update_traces(textposition='outside', marker_line_width=0)
        st.plotly_chart(chart(fig, h=220, no_cs=True), use_container_width=True, config=CFG)

    st.markdown("<br>", unsafe_allow_html=True)
    c3, c4 = st.columns(2)
    with c3:
        sec("Top 10 departs")
        st.dataframe(df['origin_station'].value_counts().head(10).reset_index()
                     .rename(columns={'origin_station':'Gare','count':'Nb'}), use_container_width=True)
    with c4:
        sec("Top 10 arrivees")
        st.dataframe(df['destination_station'].value_counts().head(10).reset_index()
                     .rename(columns={'destination_station':'Gare','count':'Nb'}), use_container_width=True)


# Main
def main():
    api_ok = False
    try:
        r = requests.get(f"{os.getenv('API_URL','http://localhost:8000')}/health", timeout=1.5)
        api_ok = r.status_code == 200
    except Exception:
        pass

    page = top_nav(api_ok)
    df   = load_data()

    if df is None or df.empty:
        st.markdown(
            '<div style="text-align:center;padding:5rem 2rem">'
            '<div style="font-size:1.3rem;font-weight:700;color:#dde6f0;margin-bottom:.5rem">Aucune donnee disponible</div>'
            '<div style="color:#1e3247;font-size:.8rem">Executez le pipeline ETL pour charger les donnees</div>'
            '</div>', unsafe_allow_html=True)
        st.code("docker compose run etl", language="bash")
        return

    stats = load_stats()

    if   page == "Accueil":      page_accueil(df)
    elif page == "Horaires":     page_horaires(df)
    elif page == "Statistiques": page_statistiques(df)
    elif page == "Liaisons":     page_liaisons(df)
    elif page == "CO2":          page_co2(df)
    elif page == "Qualite":      page_qualite(df, stats)

    st.markdown(
        '<div style="text-align:center;padding:2rem 0 .5rem;color:#111c28;'
        'font-size:.6rem;border-top:1px solid rgba(255,255,255,.04);margin-top:2rem">'
        'ObRail Europe · SNCF · Deutsche Bahn · SNCB · GTFS Open Data · ODbL'
        '</div>', unsafe_allow_html=True)

if __name__ == "__main__":
    main()