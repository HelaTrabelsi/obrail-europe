# ============================================================
# ObRail Europe — EDA — Bloc E6.2 — RNCP36581
# Trabelsi Hela · Alpha Oumar Diallo · Vitoux Alexiane
# ============================================================
# Lancer : python notebooks/01_EDA.py
# Jupyter : python -m jupytext --to notebook notebooks/01_EDA.py

import matplotlib
matplotlib.use('Agg')  # Sans interface graphique — evite crash tkinter
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import seaborn as sns
from sqlalchemy import create_engine, text
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-darkgrid')

import os
os.makedirs('notebooks', exist_ok=True)

print("=" * 60)
print("ObRail Europe — EDA — Bloc E6.2")
print("=" * 60)

# ============================================================
# 1. CONNEXION POSTGRESQL
# ============================================================

DB_URL = "postgresql://postgres:postgres@localhost:5432/obrail_db"

# Detecte les vraies colonnes de la base
def detect_columns(engine):
    with engine.connect() as conn:
        cols_trajet = [r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name='trajet' ORDER BY ordinal_position"
        ))]
        cols_train = [r[0] for r in conn.execute(text(
            "SELECT column_name FROM information_schema.columns WHERE table_name='train' ORDER BY ordinal_position"
        ))]
    print(f"Colonnes trajet : {cols_trajet}")
    print(f"Colonnes train  : {cols_train}")
    return cols_trajet, cols_train

try:
    engine = create_engine(DB_URL)
    cols_trajet, cols_train = detect_columns(engine)

    # Adapte selon les vraies colonnes detectees
    col_distance = 'distance' if 'distance' in cols_trajet else 'distance_km'
    col_gare_fk  = 'id_gare'  if 'id_gare'  in cols_trajet else 'id_gare_depart'
    col_emission = 'emission_co2_gkm' if 'emission_co2_gkm' in cols_train else None

    emission_expr = f"COALESCE(t.{col_emission}, 14)" if col_emission else "14"

    query = """
    SELECT
        t.id_train,
        op.nom AS operateur,
        g.nom  AS gare,
        g.pays,
        t.heure_depart,
        tr.distance AS distance_km,
        t.type_service,
        t.type_ligne,
        COALESCE(t.emission_co2_gkm, 14) AS emission_co2_gkm,
        tr.distance * COALESCE(t.emission_co2_gkm, 14) / 1000.0 AS co2_emission_kg,
        EXTRACT(HOUR FROM t.heure_depart::time) AS heure_num
    FROM train t
    JOIN operateur op ON t.id_operateur = op.id_operateur
    JOIN trajet    tr ON t.id_trajet    = tr.id_trajet
    JOIN gare      g  ON tr.id_gare     = g.id_gare
    """

    df = pd.read_sql(query, engine)
    SOURCE = "PostgreSQL — donnees reelles"
    print(f"\nDonnees chargees : {len(df):,} lignes x {len(df.columns)} colonnes")

except Exception as e:
    print(f"\nConnexion impossible : {e}")
    print("Mode demo — donnees simulees")
    np.random.seed(42)
    n = 10000
    ops  = np.random.choice(['SNCF','Deutsche Bahn','SNCB'], n, p=[0.20,0.59,0.21])
    dist = np.abs(np.random.normal(250, 180, n)).clip(5, 1200)
    heur = np.random.randint(0, 24, n)
    df = pd.DataFrame({
        'id_train':        range(n),
        'operateur':       ops,
        'gare':            [f'Gare_{i}' for i in np.random.randint(0, 500, n)],
        'pays':            np.where(ops=='SNCF','FR', np.where(ops=='Deutsche Bahn','DE','BE')),
        'distance_km':     dist,
        'type_service':    np.where((heur>=20)|(heur<6),'Nuit','Jour'),
        'type_ligne':      np.random.choice(['national','regional'], n, p=[0.3,0.7]),
        'emission_co2_gkm':14.0,
        'co2_emission_kg': dist * 14 / 1000,
        'heure_num':       heur,
    })
    SOURCE = "Mode demo — donnees simulees"

print(f"Source : {SOURCE}")

# ============================================================
# 2. VUE D'ENSEMBLE
# ============================================================
print("\n" + "=" * 60)
print("VUE D'ENSEMBLE")
print("=" * 60)
print(f"Dimensions    : {df.shape[0]:,} lignes x {df.shape[1]} colonnes")
print(f"Valeurs nulles: {df.isnull().sum().sum()}")
print(f"\nTypes :\n{df.dtypes}")
print(f"\nStatistiques :\n{df.describe().round(2)}")

print("\n--- STATISTIQUES CLES ---")
print(f"Nb trains     : {len(df):,}")
print(f"Operateurs    : {sorted(df['operateur'].unique())}")
print(f"Pays          : {sorted(df['pays'].unique())}")
print(f"Gares uniques : {df['gare'].nunique():,}")
print(f"Distance      : min={df['distance_km'].min():.0f}km  max={df['distance_km'].max():.0f}km  moy={df['distance_km'].mean():.0f}km")
print(f"CO2 moyen     : {df['co2_emission_kg'].mean():.2f} kg/train")
print(f"Trains Jour   : {(df['type_service']=='Jour').sum():,}  ({(df['type_service']=='Jour').mean()*100:.1f}%)")
print(f"Trains Nuit   : {(df['type_service']=='Nuit').sum():,}  ({(df['type_service']=='Nuit').mean()*100:.1f}%)")

# ============================================================
# 3. DISTRIBUTIONS
# ============================================================
fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle(f'ObRail Europe — Distributions ({SOURCE})', fontsize=13, fontweight='bold')

# Distance
axes[0,0].hist(df['distance_km'], bins=60, color='#00C98D', edgecolor='white', alpha=0.85)
axes[0,0].axvline(df['distance_km'].mean(),   color='red',    linestyle='--', lw=2, label=f'Moy: {df["distance_km"].mean():.0f}km')
axes[0,0].axvline(df['distance_km'].median(), color='orange', linestyle='--', lw=2, label=f'Med: {df["distance_km"].median():.0f}km')
axes[0,0].set_title('Distribution des distances (km)')
axes[0,0].set_xlabel('Distance (km)')
axes[0,0].legend()

# CO2
axes[0,1].hist(df['co2_emission_kg'], bins=60, color='#0096D6', edgecolor='white', alpha=0.85)
axes[0,1].axvline(df['co2_emission_kg'].mean(), color='red', linestyle='--', lw=2,
                   label=f'Moy: {df["co2_emission_kg"].mean():.2f}kg')
axes[0,1].set_title('Distribution CO2 (kg/train)')
axes[0,1].set_xlabel('CO2 emis (kg)')
axes[0,1].legend()

# Heures
hc = df['heure_num'].value_counts().sort_index()
axes[1,0].bar(hc.index, hc.values, color='#F59E0B', edgecolor='white', alpha=0.85)
axes[1,0].axvspan(20, 24, alpha=0.12, color='navy', label='Nuit (20h-24h)')
axes[1,0].axvspan(0,   6, alpha=0.12, color='navy', label='Nuit (0h-6h)')
axes[1,0].set_title('Distribution heures de depart')
axes[1,0].set_xlabel('Heure')
axes[1,0].set_xticks(range(0, 24, 2))
axes[1,0].legend()

# Boxplot par operateur
df.boxplot(column='distance_km', by='operateur', ax=axes[1,1])
axes[1,1].set_title('Distance par operateur')
axes[1,1].set_xlabel('Operateur')

plt.tight_layout()
plt.savefig('notebooks/eda_01_distributions.png', dpi=150, bbox_inches='tight')
plt.close()
print("\nFigure sauvegardee : notebooks/eda_01_distributions.png")

# ============================================================
# 4. ANALYSE PAR OPERATEUR
# ============================================================
print("\n--- STATS PAR OPERATEUR ---")
stats_op = df.groupby('operateur').agg(
    nb        = ('id_train',    'count'),
    dist_moy  = ('distance_km', 'mean'),
    dist_max  = ('distance_km', 'max'),
    co2_moy   = ('co2_emission_kg', 'mean'),
    pct_nuit  = ('type_service', lambda x: (x=='Nuit').mean()*100)
).round(2)
stats_op.columns = ['Nb trains','Dist moy','Dist max','CO2 moy (kg)','% Nuit']
print(stats_op.to_string())

fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle('ObRail Europe — Analyse par operateur', fontsize=13, fontweight='bold')

ms = df['operateur'].value_counts()
axes[0].pie(ms.values, labels=ms.index, autopct='%1.1f%%',
            colors=['#00C98D','#0096D6','#F59E0B'], startangle=90)
axes[0].set_title('Parts de marche')

pivot = df.groupby(['operateur','type_service']).size().unstack(fill_value=0)
pivot.plot(kind='bar', ax=axes[1], color=['#F59E0B','#6366F1'], edgecolor='white')
axes[1].set_title('Repartition Jour/Nuit par operateur')
axes[1].tick_params(axis='x', rotation=30)

dm = df.groupby('operateur')['distance_km'].mean().sort_values(ascending=False)
axes[2].barh(dm.index, dm.values, color=['#00C98D','#0096D6','#F59E0B'])
axes[2].set_title('Distance moyenne par operateur')
for i, v in enumerate(dm.values):
    axes[2].text(v+2, i, f'{v:.0f}km', va='center')

plt.tight_layout()
plt.savefig('notebooks/eda_02_operateurs.png', dpi=150, bbox_inches='tight')
plt.close()
print("Figure sauvegardee : notebooks/eda_02_operateurs.png")

# ============================================================
# 5. ANALYSE CO2
# ============================================================
total_dist  = df['distance_km'].sum()
co2_train   = total_dist * 14  / 1000
co2_avion   = total_dist * 258 / 1000
economie    = co2_avion - co2_train

print("\n--- IMPACT CARBONE (ADEME 2023) ---")
print(f"Distance totale   : {total_dist:,.0f} km")
print(f"CO2 train (14g)   : {co2_train:,.0f} kg")
print(f"CO2 avion (258g)  : {co2_avion:,.0f} kg")
print(f"CO2 economise     : {economie:,.0f} kg")
print(f"Economie          : {economie/co2_avion*100:.1f}%")
print(f"Ratio avion/train : {258/14:.1f}x")

fig, axes = plt.subplots(1, 3, figsize=(16, 5))
fig.suptitle('ObRail Europe — Analyse CO2', fontsize=13, fontweight='bold')

comp = pd.DataFrame({
    'Mode':['Train elec FR','Train moy UE','Voiture','Avion long','Avion court'],
    'CO2': [6, 14, 193, 195, 258]
})
colors_comp = ['#00C98D','#00B87A','#F59E0B','#EF4444','#DC2626']
bars = axes[0].barh(comp['Mode'], comp['CO2'], color=colors_comp, edgecolor='white')
axes[0].axvline(14, color='green', linestyle='--', alpha=0.7, label='Notre ref: 14g/km')
axes[0].set_title('Comparatif modal CO2 (g/km)')
axes[0].legend(fontsize=8)
for bar, val in zip(bars, comp['CO2']):
    axes[0].text(val+1, bar.get_y()+bar.get_height()/2, f'{val}g', va='center', fontsize=9)

sample = df.sample(min(3000, len(df)), random_state=42)
for ts, c in [('Jour','#F59E0B'), ('Nuit','#6366F1')]:
    g = sample[sample['type_service']==ts]
    axes[1].scatter(g['distance_km'], g['co2_emission_kg'], c=c, alpha=0.3, s=6, label=ts)
axes[1].set_title('Distance vs CO2')
axes[1].set_xlabel('Distance (km)')
axes[1].set_ylabel('CO2 (kg)')
axes[1].legend()

df.groupby('type_service')['co2_emission_kg'].mean().plot(
    kind='bar', ax=axes[2], color=['#F59E0B','#6366F1'], edgecolor='white')
axes[2].set_title('CO2 moyen Jour vs Nuit')
axes[2].tick_params(axis='x', rotation=0)
for p in axes[2].patches:
    axes[2].text(p.get_x()+p.get_width()/2, p.get_height()+0.02,
                  f'{p.get_height():.2f}kg', ha='center', fontsize=9)

plt.tight_layout()
plt.savefig('notebooks/eda_03_co2.png', dpi=150, bbox_inches='tight')
plt.close()
print("Figure sauvegardee : notebooks/eda_03_co2.png")

# ============================================================
# 6. MATRICE DE CORRELATION
# ============================================================
le = LabelEncoder()
df_num = df.copy()
df_num['operateur_enc']    = le.fit_transform(df['operateur'])
df_num['type_service_enc'] = (df['type_service']=='Nuit').astype(int)
df_num['type_ligne_enc']   = (df['type_ligne']=='national').astype(int)
df_num['pays_enc']         = le.fit_transform(df['pays'])
df_num['heure_sin']        = np.sin(2 * np.pi * df_num['heure_num'] / 24)
df_num['heure_cos']        = np.cos(2 * np.pi * df_num['heure_num'] / 24)

cols   = ['distance_km','co2_emission_kg','heure_num','operateur_enc','type_service_enc','type_ligne_enc','pays_enc']
labels = ['Distance','CO2','Heure','Operateur','Nuit','National','Pays']
corr   = df_num[cols].corr()

plt.figure(figsize=(9, 7))
sns.heatmap(corr, annot=True, fmt='.2f', cmap='RdYlGn',
            xticklabels=labels, yticklabels=labels,
            vmin=-1, vmax=1, center=0, square=True, linewidths=0.5)
plt.title('Matrice de correlation — ObRail Europe', fontweight='bold')
plt.tight_layout()
plt.savefig('notebooks/eda_04_correlation.png', dpi=150, bbox_inches='tight')
plt.close()
print("Figure sauvegardee : notebooks/eda_04_correlation.png")

print("\n--- CORRELATIONS AVEC CO2 ---")
c2 = corr['co2_emission_kg'].drop('co2_emission_kg').sort_values(key=abs, ascending=False)
for f, v in c2.items():
    idx = cols.index(f)
    star = '***' if abs(v)>0.7 else '**' if abs(v)>0.3 else '*'
    print(f"  {labels[idx]:<15} : {v:+.4f} {star}")

# ============================================================
# 7. DETECTION OUTLIERS
# ============================================================
print("\n--- VALEURS ABERRANTES ---")
Q1    = df['distance_km'].quantile(0.25)
Q3    = df['distance_km'].quantile(0.75)
IQR   = Q3 - Q1
upper = Q3 + 1.5 * IQR
lower = Q1 - 1.5 * IQR
out   = df[(df['distance_km']>upper)|(df['distance_km']<lower)]
print(f"Q1={Q1:.0f}km  Q3={Q3:.0f}km  IQR={IQR:.0f}km")
print(f"Seuil sup : {upper:.0f}km  Seuil inf : {lower:.0f}km")
print(f"Outliers  : {len(out):,} trains ({len(out)/len(df)*100:.1f}%)")
print("Decision  : CONSERVER — vraies longues distances europeennes")

# ============================================================
# 8. TABLEAU FEATURES RETENUES
# ============================================================
print("\n--- FEATURES RETENUES POUR LE MODELE ML ---")
features_df = pd.DataFrame({
    'Variable':             ['distance_km','heure_sin','heure_cos','operateur_enc','type_ligne_enc','pays_enc'],
    'Type':                 ['Continue',   'Cyclique', 'Cyclique', 'Categorielle', 'Binaire',       'Categorielle'],
    'Transformation':       ['StandardScaler','sin(2pi*h/24)','cos(2pi*h/24)','LabelEncoder','national=1','LabelEncoder'],
    'Importance estimee':   ['Tres haute (r=0.99+)','Faible','Faible','Moyenne','Moyenne','Faible'],
    'Justification': [
        'Correlation quasi-parfaite avec CO2 (r>0.99)',
        'Encode heure cyclique — 23h proche de 0h',
        'Complement sin pour encodage cyclique complet',
        'Differences de perf entre SNCF/DB/SNCB',
        'Trains nationaux plus longs en moyenne',
        'Differences de reseau par pays (FR/DE/BE)',
    ]
})
print(features_df.to_string(index=False))

# ============================================================
# 9. EXPORT DATASET ML
# ============================================================
df_ml = df_num[['distance_km','heure_num','operateur_enc','type_service_enc',
                 'type_ligne_enc','pays_enc','co2_emission_kg']].copy()
df_ml['heure_sin'] = np.sin(2 * np.pi * df_ml['heure_num'] / 24)
df_ml['heure_cos'] = np.cos(2 * np.pi * df_ml['heure_num'] / 24)
df_ml = df_ml.drop('heure_num', axis=1)
df_ml.to_csv('notebooks/data_ml_ready.csv', index=False)
print(f"\nDataset ML exporte : notebooks/data_ml_ready.csv")
print(f"  {len(df_ml):,} lignes x {len(df_ml.columns)} colonnes")
print(f"  Colonnes : {list(df_ml.columns)}")

# ============================================================
# 10. RESUME FINAL
# ============================================================
print("\n" + "=" * 60)
print("RESUME EDA")
print("=" * 60)
print(f"Source          : {SOURCE}")
print(f"Nb trains       : {len(df):,}")
print(f"Operateurs      : SNCF {(df['operateur']=='SNCF').mean()*100:.0f}% · DB {(df['operateur']=='Deutsche Bahn').mean()*100:.0f}% · SNCB {(df['operateur']=='SNCB').mean()*100:.0f}%")
print(f"Distances       : {df['distance_km'].min():.0f}km a {df['distance_km'].max():.0f}km · moy {df['distance_km'].mean():.0f}km")
print(f"CO2             : moy {df['co2_emission_kg'].mean():.2f}kg · ratio avion/train 18.4x · economie 94.6%")
print(f"Jour/Nuit       : {(df['type_service']=='Jour').mean()*100:.0f}% jour · {(df['type_service']=='Nuit').mean()*100:.0f}% nuit")
print(f"Outliers        : {len(out):,} trains distances extremes → conserves")
print(f"Feature choisie : distance_km (r=0.99+ avec CO2) — prédicteur dominant")
print(f"\nFigures generees : eda_01_distributions.png · eda_02_operateurs.png · eda_03_co2.png · eda_04_correlation.png")
print(f"Dataset ML      : notebooks/data_ml_ready.csv")
print("\nProchaine etape : python notebooks/02_preprocessing.py")