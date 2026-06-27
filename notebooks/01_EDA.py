# ============================================================
# ObRail Europe — Notebook EDA — Bloc E6.2 — RNCP36581
# ============================================================
# Ouvrir dans Jupyter : jupyter notebook
# Ou convertir : jupytext --to notebook 01_EDA.py

# %% [markdown]
# # ObRail Europe — Analyse Exploratoire des Données (EDA)
# **Bloc E6.2 — RNCP36581**
# Trabelsi Hela · Alpha Oumar Diallo · Vitoux Alexiane

# %% [markdown]
# ## 0. Imports

# %%
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-darkgrid')
print("Imports OK")

# %% [markdown]
# ## 1. Connexion PostgreSQL et chargement

# %%
DB_URL = "postgresql://postgres:postgres@localhost:5432/obrail_db"

try:
    engine = create_engine(DB_URL)
    query = """
    SELECT 
        t.id_train,
        op.nom AS operateur,
        g.nom AS gare,
        g.pays,
        tr.heure_depart,
        tr.distance_km,
        t.type_service,
        t.type_ligne,
        COALESCE(t.emission_co2_gkm, 14) AS emission_co2_gkm,
        tr.distance_km * COALESCE(t.emission_co2_gkm, 14) / 1000.0 AS co2_emission_kg,
        EXTRACT(HOUR FROM tr.heure_depart::time) AS heure_num
    FROM train t
    JOIN operateur op ON t.id_operateur = op.id_operateur
    JOIN trajet tr ON t.id_trajet = tr.id_trajet
    JOIN gare g ON tr.id_gare_depart = g.id_gare
    """
    df = pd.read_sql(query, engine)
    print(f"Donnees chargees : {len(df):,} lignes x {len(df.columns)} colonnes")
except Exception as e:
    print(f"Connexion impossible: {e}")
    print("Mode demo : generation donnees simulees")
    np.random.seed(42)
    n = 10000
    operateurs = np.random.choice(['SNCF','Deutsche Bahn','SNCB'], n, p=[0.55,0.35,0.10])
    distances = np.abs(np.random.normal(250,180,n)).clip(5,1200)
    heures = np.random.randint(0,24,n)
    df = pd.DataFrame({
        'id_train': range(n),
        'operateur': operateurs,
        'gare': [f'Gare_{i}' for i in np.random.randint(0,200,n)],
        'pays': np.where(operateurs=='SNCF','FR',np.where(operateurs=='Deutsche Bahn','DE','BE')),
        'distance_km': distances,
        'type_service': np.where((heures>=20)|(heures<6),'Nuit','Jour'),
        'type_ligne': np.random.choice(['national','regional'],n,p=[0.3,0.7]),
        'emission_co2_gkm': 14.0,
        'co2_emission_kg': distances * 14 / 1000,
        'heure_num': heures,
    })
    print(f"Donnees simulees : {len(df):,} lignes")

# %% [markdown]
# ## 2. Vue d'ensemble

# %%
print(f"Dimensions : {df.shape}")
print(f"\nTypes :\n{df.dtypes}")
print(f"\nValeurs manquantes :\n{df.isnull().sum()}")
print(f"\nStatistiques :\n{df.describe().round(2)}")

# %%
print("STATISTIQUES CLES")
print(f"Nb trains : {len(df):,}")
print(f"Operateurs : {df['operateur'].unique()}")
print(f"Pays : {df['pays'].unique()}")
print(f"Gares uniques : {df['gare'].nunique():,}")
print(f"Distance min/max/moy : {df['distance_km'].min():.0f} / {df['distance_km'].max():.0f} / {df['distance_km'].mean():.0f} km")
print(f"CO2 moyen : {df['co2_emission_kg'].mean():.2f} kg")
print(f"Trains Jour : {(df['type_service']=='Jour').sum():,} ({(df['type_service']=='Jour').mean()*100:.1f}%)")
print(f"Trains Nuit : {(df['type_service']=='Nuit').sum():,} ({(df['type_service']=='Nuit').mean()*100:.1f}%)")

# %% [markdown]
# ## 3. Distributions des variables

# %%
fig, axes = plt.subplots(2,2,figsize=(14,10))
fig.suptitle('ObRail Europe — Distributions', fontsize=14, fontweight='bold')

axes[0,0].hist(df['distance_km'], bins=50, color='#00C98D', edgecolor='white', alpha=0.8)
axes[0,0].axvline(df['distance_km'].mean(), color='red', linestyle='--', label=f'Moy: {df["distance_km"].mean():.0f}km')
axes[0,0].axvline(df['distance_km'].median(), color='orange', linestyle='--', label=f'Med: {df["distance_km"].median():.0f}km')
axes[0,0].set_title('Distribution distances (km)')
axes[0,0].legend()

axes[0,1].hist(df['co2_emission_kg'], bins=50, color='#0096D6', edgecolor='white', alpha=0.8)
axes[0,1].axvline(df['co2_emission_kg'].mean(), color='red', linestyle='--', label=f'Moy: {df["co2_emission_kg"].mean():.2f}kg')
axes[0,1].set_title('Distribution CO2 (kg)')
axes[0,1].legend()

heures_c = df['heure_num'].value_counts().sort_index()
axes[1,0].bar(heures_c.index, heures_c.values, color='#F59E0B', edgecolor='white', alpha=0.8)
axes[1,0].axvspan(20,24,alpha=0.1,color='navy',label='Nuit')
axes[1,0].axvspan(0,6,alpha=0.1,color='navy')
axes[1,0].set_title('Distribution heures de depart')
axes[1,0].legend()
axes[1,0].set_xticks(range(0,24,2))

df.boxplot(column='distance_km', by='operateur', ax=axes[1,1])
axes[1,1].set_title('Distance par operateur')

plt.tight_layout()
plt.savefig('eda_01_distributions.png', dpi=150, bbox_inches='tight')
plt.show()
print("Figure sauvegardee : eda_01_distributions.png")

# %% [markdown]
# ## 4. Analyse par operateur

# %%
fig, axes = plt.subplots(1,3,figsize=(16,5))
fig.suptitle('ObRail Europe — Analyse par operateur', fontsize=14, fontweight='bold')

ms = df['operateur'].value_counts()
axes[0].pie(ms.values, labels=ms.index, autopct='%1.1f%%',
            colors=['#00C98D','#0096D6','#F59E0B'])
axes[0].set_title('Parts de marche')

pivot = df.groupby(['operateur','type_service']).size().unstack(fill_value=0)
pivot.plot(kind='bar', ax=axes[1], color=['#F59E0B','#6366F1'], edgecolor='white')
axes[1].set_title('Repartition Jour/Nuit par operateur')
axes[1].tick_params(axis='x', rotation=30)

dm = df.groupby('operateur')['distance_km'].mean().sort_values(ascending=False)
axes[2].barh(dm.index, dm.values, color=['#00C98D','#0096D6','#F59E0B'])
axes[2].set_title('Distance moyenne par operateur')
for i,v in enumerate(dm.values):
    axes[2].text(v+2, i, f'{v:.0f}km', va='center')

plt.tight_layout()
plt.savefig('eda_02_operateurs.png', dpi=150, bbox_inches='tight')
plt.show()

# %%
print("STATS PAR OPERATEUR")
print(df.groupby('operateur').agg(
    nb=('id_train','count'),
    dist_moy=('distance_km','mean'),
    dist_max=('distance_km','max'),
    co2_moy=('co2_emission_kg','mean'),
    pct_nuit=('type_service',lambda x:(x=='Nuit').mean()*100)
).round(2).to_string())

# %% [markdown]
# ## 5. Analyse CO2

# %%
fig, axes = plt.subplots(1,3,figsize=(16,5))
fig.suptitle('ObRail Europe — Analyse CO2', fontsize=14, fontweight='bold')

comp = pd.DataFrame({'Mode':['Train elec','Train moy UE','Avion long','Avion court'],'CO2':[6,14,195,258]})
axes[0].barh(comp['Mode'], comp['CO2'], color=['#00C98D','#00C98D','#EF4444','#EF4444'])
axes[0].set_title('Comparatif modal (g/km)')

sample = df.sample(min(2000,len(df)), random_state=42)
for ts,c in [('Jour','#F59E0B'),('Nuit','#6366F1')]:
    g = sample[sample['type_service']==ts]
    axes[1].scatter(g['distance_km'], g['co2_emission_kg'], c=c, alpha=0.3, s=8, label=ts)
axes[1].set_title('Distance vs CO2')
axes[1].set_xlabel('Distance (km)')
axes[1].set_ylabel('CO2 (kg)')
axes[1].legend()

df.groupby('type_service')['co2_emission_kg'].mean().plot(
    kind='bar', ax=axes[2], color=['#F59E0B','#6366F1'], edgecolor='white')
axes[2].set_title('CO2 moyen Jour vs Nuit')
axes[2].tick_params(axis='x', rotation=0)

plt.tight_layout()
plt.savefig('eda_03_co2.png', dpi=150, bbox_inches='tight')
plt.show()

total_dist = df['distance_km'].sum()
co2_train = total_dist * 14 / 1000
co2_avion = total_dist * 258 / 1000
print(f"CO2 train total : {co2_train:,.0f} kg")
print(f"CO2 avion total : {co2_avion:,.0f} kg")
print(f"Economie : {(co2_avion-co2_train)/co2_avion*100:.1f}%")
print(f"Ratio avion/train : {258/14:.1f}x")

# %% [markdown]
# ## 6. Matrice de correlation

# %%
le = LabelEncoder()
df_num = df.copy()
df_num['operateur_enc'] = le.fit_transform(df['operateur'])
df_num['type_service_enc'] = (df['type_service']=='Nuit').astype(int)
df_num['type_ligne_enc'] = (df['type_ligne']=='national').astype(int)
df_num['pays_enc'] = le.fit_transform(df['pays'])

cols = ['distance_km','co2_emission_kg','heure_num','operateur_enc','type_service_enc','type_ligne_enc','pays_enc']
labels = ['Distance','CO2','Heure','Operateur','Nuit','National','Pays']

corr = df_num[cols].corr()
plt.figure(figsize=(9,7))
sns.heatmap(corr, annot=True, fmt='.2f', cmap='RdYlGn',
            xticklabels=labels, yticklabels=labels,
            vmin=-1, vmax=1, center=0, square=True)
plt.title('Matrice de correlation', fontweight='bold')
plt.tight_layout()
plt.savefig('eda_04_correlation.png', dpi=150, bbox_inches='tight')
plt.show()

print("\nCORRELATIONS AVEC CO2 (cible) :")
c2 = corr['co2_emission_kg'].drop('co2_emission_kg').sort_values(key=abs, ascending=False)
for f,v in c2.items():
    print(f"  {f:<20} : {v:+.4f}")

# %% [markdown]
# ## 7. Detection des valeurs aberrantes

# %%
Q1 = df['distance_km'].quantile(0.25)
Q3 = df['distance_km'].quantile(0.75)
IQR = Q3 - Q1
lower = Q1 - 1.5 * IQR
upper = Q3 + 1.5 * IQR
outliers = df[(df['distance_km'] < lower) | (df['distance_km'] > upper)]
print(f"Q1={Q1:.0f}km  Q3={Q3:.0f}km  IQR={IQR:.0f}km")
print(f"Seuil sup : {upper:.0f}km")
print(f"Outliers : {len(outliers):,} ({len(outliers)/len(df)*100:.1f}%)")
print("Decision : CONSERVER — representent de vraies longues distances europeennes")

# %% [markdown]
# ## 8. Tableau des variables retenues

# %%
features = pd.DataFrame({
    'Variable': ['distance_km','heure_sin','heure_cos','operateur_enc','type_ligne_enc','pays_enc'],
    'Type': ['Continue','Cyclique','Cyclique','Categorielle','Binaire','Categorielle'],
    'Transformation': ['StandardScaler','sin(2pi*h/24)','cos(2pi*h/24)','LabelEncoder','national=1','LabelEncoder'],
    'Importance estimee': ['Tres haute (r=0.99+)','Faible','Faible','Moyenne','Moyenne','Faible'],
    'Justification': [
        'Correlation quasi-parfaite avec CO2',
        'Encode heure cyclique — 23h proche de 0h',
        'Complement sin pour encodage cyclique',
        'Differences de performance entre operateurs',
        'Trains nationaux plus longs en moyenne',
        'Differences de reseau par pays',
    ]
})
print(features.to_string(index=False))

# %%
# Export dataset ML
df_ml = df_num[['distance_km','heure_num','operateur_enc','type_service_enc','type_ligne_enc','pays_enc','co2_emission_kg']].copy()
df_ml['heure_sin'] = np.sin(2 * np.pi * df_ml['heure_num'] / 24)
df_ml['heure_cos'] = np.cos(2 * np.pi * df_ml['heure_num'] / 24)
df_ml = df_ml.drop('heure_num', axis=1)
df_ml.to_csv('data_ml_ready.csv', index=False)
print(f"\nDataset ML exporte : data_ml_ready.csv ({len(df_ml):,} lignes x {len(df_ml.columns)} colonnes)")
print("Prochaine etape : 02_preprocessing.ipynb")