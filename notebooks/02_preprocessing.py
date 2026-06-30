
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.utils import resample
import joblib
import warnings, os
warnings.filterwarnings("ignore")
np.random.seed(42)

os.makedirs("notebooks/ml_splits", exist_ok=True)

# =============================================================================
# 1. CHARGEMENT
# =============================================================================
DATA_PATH = "notebooks/data_ml_ready.csv"
df = pd.read_csv(DATA_PATH)

print("=" * 60)
print("DONNÉES CHARGÉES DEPUIS L'EDA")
print("=" * 60)
print(f"  Dimensions       : {df.shape[0]:,} lignes × {df.shape[1]} colonnes")
print(f"  Colonnes         : {list(df.columns)}")
print(f"  Valeurs manquantes : {df.isnull().sum().sum()}")
print(f"\nAperçu :")
print(df.head(3).to_string())

# =============================================================================
# 2. ENJEUX MÉTIER RETENUS (2 enjeux)
# =============================================================================
print("\n" + "=" * 60)
print("ENJEUX MÉTIER RETENUS")
print("=" * 60)
print("  1. Régression CO₂      → prédire co2_emission_kg")
print("  2. Détection sous-desserte → prédire sous_desserte (créée)")
print()
print("  Enjeu 'substitution avion-train' (Jour/Nuit) écarté :")
print("  → Sans heure_depart, F1=0,59 non améliorable avec les features")
print("    disponibles dans les flux GTFS statiques.")
print("  → La détection de sous-desserte couvre mieux la mission TEN-T.")

# =============================================================================
# 3. FEATURE ENGINEERING
# =============================================================================

# Distance bucket
bins   = [0, 100, 300, 600, 1100]
labels = ["<100km", "100-300km", "300-600km", ">600km"]
df["distance_bucket"] = pd.cut(df["distance_km"], bins=bins, labels=labels)
df["distance_bucket_enc"] = df["distance_bucket"].cat.codes
print(f"\n✓ distance_bucket : {df['distance_bucket'].value_counts().sort_index().to_dict()}")

# CO2 par km
df["co2_par_km"] = (
    df["co2_emission_kg"] / df["distance_km"].replace(0, np.nan)
).fillna(0).round(6)
print(f"✓ co2_par_km (moy={df['co2_par_km'].mean():.6f} kg/km)")

# Variable cible sous-desserte
# Règle métier : gare fragile si distance < P30 ET ligne régionale
seuil = df["distance_km"].quantile(0.30)
df["sous_desserte"] = (
    (df["distance_km"] < seuil) & (df["type_ligne_enc"] == 0)
).astype(int)
print(f"✓ sous_desserte (seuil={seuil:.1f}km) : {df['sous_desserte'].value_counts().to_dict()}")

# =============================================================================
# 4. NORMALISATION — StandardScaler sur distance_km ET co2_par_km
# =============================================================================
scaler = StandardScaler()
cols_scale = ["distance_km", "co2_par_km"]

df["distance_km_raw"] = df["distance_km"].copy()
df[cols_scale] = scaler.fit_transform(df[cols_scale])

print(f"\n✓ StandardScaler appliqué sur : {cols_scale}")
print(f"  Moyennes ≈ 0 : {df[cols_scale].mean().round(4).to_dict()}")
print(f"  Std ≈ 1       : {df[cols_scale].std().round(4).to_dict()}")

joblib.dump(scaler, "notebooks/ml_splits/scaler.joblib")
print(f"✓ Scaler sauvegardé : notebooks/ml_splits/scaler.joblib")
print(f"  mean_  = {scaler.mean_}")
print(f"  scale_ = {scaler.scale_}")

# =============================================================================
# 5. FEATURES PAR ENJEU (2 enjeux)
# =============================================================================

# ENJEU 1 — CO2 : sans distance_km directe (anti-leakage)
# co2_emission_kg = distance_km × 14 / 1000 → relation déterministe
# On utilise distance_bucket (catégorie) à la place
FEATURES_CO2 = [
    "operateur_enc", "type_service_enc", "type_ligne_enc",
    "pays_enc", "heure_sin", "heure_cos", "distance_bucket_enc"
]
TARGET_CO2 = "co2_emission_kg"

# ENJEU 2 — SOUS-DESSERTE : sans distance_km (anti-leakage)
# sous_desserte est définie par distance_km < seuil → ne pas la donner en feature
FEATURES_DES = [
    "operateur_enc", "pays_enc", "type_ligne_enc",
    "heure_sin", "heure_cos", "co2_par_km"
]
TARGET_DES = "sous_desserte"

print(f"\n✓ Features CO₂          ({len(FEATURES_CO2)}) : {FEATURES_CO2}")
print(f"✓ Features Sous-desserte ({len(FEATURES_DES)}) : {FEATURES_DES}")

# =============================================================================
# 6. SPLIT 70 / 15 / 15
# =============================================================================
def split_70_15_15(df, features, target, label=""):
    X = df[features]
    y = df[target]
    stratify = y if y.nunique() <= 10 else None
    X_tr, X_tmp, y_tr, y_tmp = train_test_split(
        X, y, test_size=0.30, random_state=42, stratify=stratify)
    stratify2 = y_tmp if y.nunique() <= 10 else None
    X_v, X_te, y_v, y_te = train_test_split(
        X_tmp, y_tmp, test_size=0.50, random_state=42, stratify=stratify2)
    print(f"\n  {label} : train={len(X_tr):,}  val={len(X_v):,}  test={len(X_te):,}")
    return X_tr, X_v, X_te, y_tr, y_v, y_te

print("\n" + "=" * 60)
print("SPLIT 70 / 15 / 15")
print("=" * 60)

X_tr_co2, X_v_co2, X_te_co2, y_tr_co2, y_v_co2, y_te_co2 = \
    split_70_15_15(df, FEATURES_CO2, TARGET_CO2, "CO₂")

X_tr_des, X_v_des, X_te_des, y_tr_des, y_v_des, y_te_des = \
    split_70_15_15(df, FEATURES_DES, TARGET_DES, "Sous-desserte")

# =============================================================================
# 7. ÉQUILIBRAGE — sous-desserte uniquement (classes déséquilibrées)
# =============================================================================
def equilibrer(X_tr, y_tr, label):
    counts = y_tr.value_counts()
    ratio  = counts.min() / counts.max()
    print(f"\n  {label} : {counts.to_dict()} | ratio={ratio:.2f}", end=" ")
    if ratio < 0.4:
        print("→ Oversampling appliqué")
        df_tmp = pd.concat([X_tr, y_tr], axis=1)
        col  = y_tr.name
        maj  = counts.idxmax()
        min_ = counts.idxmin()
        df_min_up = resample(
            df_tmp[df_tmp[col] == min_], replace=True,
            n_samples=len(df_tmp[df_tmp[col] == maj]), random_state=42)
        df_bal = pd.concat([df_tmp[df_tmp[col] == maj], df_min_up])
        return df_bal.drop(columns=col), df_bal[col]
    else:
        print("→ Équilibre OK — pas d'oversampling nécessaire")
        return X_tr, y_tr

print("\n" + "=" * 60)
print("ÉQUILIBRAGE DES CLASSES")
print("=" * 60)
X_tr_des, y_tr_des = equilibrer(X_tr_des, y_tr_des, "Sous-desserte")

# =============================================================================
# 8. SAUVEGARDE DES SPLITS (2 enjeux)
# =============================================================================
for name, Xtr, Xv, Xte, ytr, yv, yte in [
    ("co2",      X_tr_co2, X_v_co2, X_te_co2, y_tr_co2, y_v_co2, y_te_co2),
    ("desserte", X_tr_des, X_v_des, X_te_des, y_tr_des, y_v_des, y_te_des),
]:
    pd.concat([Xtr, ytr], axis=1).to_csv(
        f"notebooks/ml_splits/{name}_train.csv", index=False)
    pd.concat([Xv, yv], axis=1).to_csv(
        f"notebooks/ml_splits/{name}_val.csv", index=False)
    pd.concat([Xte, yte], axis=1).to_csv(
        f"notebooks/ml_splits/{name}_test.csv", index=False)

print("\n✓ Splits sauvegardés dans notebooks/ml_splits/")

# =============================================================================
# 9. TABLEAU DES VARIABLES RETENUES (livrable cahier des charges III.1)
# =============================================================================
tableau = pd.DataFrame([
    {"Enjeu":"CO₂","Variable":"distance_bucket_enc","Type":"Catégoriel",
     "Rôle":"Feature","Traitement":"Buckets [0,100,300,600,1100] — PAS distance directe (anti-leakage)"},
    {"Enjeu":"CO₂","Variable":"operateur_enc","Type":"Catégoriel",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"CO₂","Variable":"type_service_enc","Type":"Binaire",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"CO₂","Variable":"type_ligne_enc","Type":"Binaire",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"CO₂","Variable":"pays_enc","Type":"Catégoriel",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"CO₂","Variable":"heure_sin + heure_cos","Type":"Cyclique",
     "Rôle":"Feature","Traitement":"sin/cos(2π×heure/24) — évite ambiguïté 23h≈0h"},
    {"Enjeu":"CO₂","Variable":"co2_emission_kg","Type":"Continue",
     "Rôle":"Cible","Traitement":"distance × 14 g/km / 1000"},
    {"Enjeu":"Sous-desserte","Variable":"operateur_enc","Type":"Catégoriel",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"Sous-desserte","Variable":"pays_enc","Type":"Catégoriel",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"Sous-desserte","Variable":"type_ligne_enc","Type":"Binaire",
     "Rôle":"Feature","Traitement":"LabelEncoder (EDA)"},
    {"Enjeu":"Sous-desserte","Variable":"heure_sin + heure_cos","Type":"Cyclique",
     "Rôle":"Feature","Traitement":"sin/cos(2π×heure/24)"},
    {"Enjeu":"Sous-desserte","Variable":"co2_par_km","Type":"Numérique",
     "Rôle":"Feature","Traitement":"StandardScaler"},
    {"Enjeu":"Sous-desserte","Variable":"sous_desserte","Type":"Binaire",
     "Rôle":"Cible","Traitement":"1 si dist<P30 ET régionale, 0 sinon"},
])
print("\n" + "=" * 60)
print("TABLEAU DES VARIABLES RETENUES")
print("=" * 60)
print(tableau.to_string(index=False))
tableau.to_csv("notebooks/ml_splits/tableau_variables.csv", index=False)
print("\n✓ tableau_variables.csv sauvegardé")

# =============================================================================
# 10. VISUALISATION — distribution des 2 cibles
# =============================================================================
df_orig = pd.read_csv(DATA_PATH)
fig, axes = plt.subplots(1, 2, figsize=(12, 5))
fig.suptitle("ObRail — Distribution des variables cibles (2 enjeux retenus)",
             fontsize=13, fontweight='bold')

axes[0].hist(df_orig["co2_emission_kg"], bins=50, color="#2196F3",
             edgecolor="white", alpha=0.85)
axes[0].set_title("CO₂ par trajet (kg) — Enjeu 1 Régression")
axes[0].axvline(df_orig["co2_emission_kg"].mean(), color="red", linestyle="--",
                label=f"Moy: {df_orig['co2_emission_kg'].mean():.3f} kg")
axes[0].set_xlabel("CO₂ émis (kg)"); axes[0].legend()

vals_des = df["sous_desserte"].value_counts().sort_index().values
axes[1].bar(["Normal (0)", "Sous-desservi (1)"], vals_des,
            color=["#4CAF50", "#F44336"], edgecolor="white")
axes[1].set_title("Sous-desserte — Enjeu 2 Classification")
for i, v in enumerate(vals_des):
    axes[1].text(i, v + 50, f"{v:,}", ha="center", fontweight="bold")

plt.tight_layout()
plt.savefig("notebooks/ml_splits/targets_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print("✓ targets_distribution.png sauvegardée")

# =============================================================================
# 11. RÉCAPITULATIF FINAL
# =============================================================================
print("\n" + "=" * 60)
print("RÉCAPITULATIF")
print("=" * 60)
print(f"  Source : {DATA_PATH} — {len(df):,} lignes")
print(f"  Scaler : mean_dist={scaler.mean_[0]:.4f}  scale_dist={scaler.scale_[0]:.4f}")
print(f"  CO₂      : {len(X_tr_co2):,} train / {len(X_v_co2):,} val / {len(X_te_co2):,} test")
print(f"  Desserte : {len(X_tr_des):,} train / {len(X_v_des):,} val / {len(X_te_des):,} test")
print("\n→ Prochaine étape : python notebooks/03_models.py")