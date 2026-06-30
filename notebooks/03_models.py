
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import warnings, os, joblib
warnings.filterwarnings("ignore")
np.random.seed(42)

from sklearn.linear_model    import LinearRegression, LogisticRegression, Ridge
from sklearn.ensemble        import RandomForestRegressor, RandomForestClassifier, GradientBoostingClassifier
from sklearn.neural_network  import MLPRegressor, MLPClassifier
from sklearn.model_selection import cross_val_score
from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score,
    accuracy_score, f1_score, roc_auc_score,
    confusion_matrix, classification_report
)
from xgboost import XGBRegressor, XGBClassifier

os.makedirs("notebooks/outputs_models", exist_ok=True)
SPLITS = "notebooks/ml_splits"

print("=" * 65)
print("ObRail Europe — Modèles ML — 2 enjeux (sans data leakage)")
print("=" * 65)
print("""
ENJEUX RETENUS :
  1. Régression CO2        → prédire co2_emission_kg
     Features : operateur, type_service, type_ligne, pays, heure_sin/cos,
                distance_bucket (PAS distance directe → anti-leakage)

  2. Détection sous-desserte → prédire sous_desserte (binaire)
     Features : operateur, pays, type_ligne, heure_sin/cos, co2_par_km
                (PAS distance directe → anti-leakage)

""")

# ============================================================
# 1. FEATURES (identiques à 02_preprocessing.py)
# ============================================================
FEAT_CO2 = [
    "operateur_enc", "type_service_enc", "type_ligne_enc",
    "pays_enc", "heure_sin", "heure_cos", "distance_bucket_enc"
]
TARGET_CO2 = "co2_emission_kg"

FEAT_DES = [
    "operateur_enc", "pays_enc", "type_ligne_enc",
    "heure_sin", "heure_cos", "co2_par_km"
]
TARGET_DES = "sous_desserte"

# ============================================================
# 2. CHARGEMENT DES SPLITS
# ============================================================
def charger(enjeu, target, features):
    tr = pd.read_csv(f"{SPLITS}/{enjeu}_train.csv")
    v  = pd.read_csv(f"{SPLITS}/{enjeu}_val.csv")
    te = pd.read_csv(f"{SPLITS}/{enjeu}_test.csv")
    feat_dispo = [f for f in features if f in tr.columns]
    print(f"  {enjeu}: {len(feat_dispo)} features chargées : {feat_dispo}")
    return (tr[feat_dispo], v[feat_dispo], te[feat_dispo],
            tr[target], v[target], te[target])

print("\n--- Chargement des splits ---")
X_tr_co2, X_v_co2, X_te_co2, y_tr_co2, y_v_co2, y_te_co2 = \
    charger("co2", TARGET_CO2, FEAT_CO2)
X_tr_des, X_v_des, X_te_des, y_tr_des, y_v_des, y_te_des = \
    charger("desserte", TARGET_DES, FEAT_DES)

# ============================================================
# 3. DÉFINITION DES 4 ALGORITHMES PAR TYPE DE TÂCHE
# ============================================================
MODELES_REG = {
    "Ridge (alpha=1.0)": Ridge(alpha=1.0),
    "RandomForest":      RandomForestRegressor(n_estimators=100, max_depth=8,
                                               random_state=42, n_jobs=-1),
    "XGBoost":           XGBRegressor(n_estimators=100, learning_rate=0.05,
                                      max_depth=4, random_state=42, verbosity=0),
    "MLP":               MLPRegressor(hidden_layer_sizes=(64, 32), max_iter=300,
                                      random_state=42, early_stopping=True),
}

MODELES_CLF = {
    "LogisticRegression": LogisticRegression(max_iter=500, random_state=42, C=0.1),
    "RandomForest":       RandomForestClassifier(n_estimators=100, max_depth=8,
                                                 random_state=42, n_jobs=-1),
    "XGBoost":            XGBClassifier(n_estimators=100, learning_rate=0.05,
                                        max_depth=4, random_state=42, verbosity=0),
    "MLP":                MLPClassifier(hidden_layer_sizes=(64, 32), max_iter=300,
                                        random_state=42, early_stopping=True),
}

# ============================================================
# 4. FONCTIONS D'ENTRAÎNEMENT + CROSS-VALIDATION
# ============================================================
def run_reg(modeles, X_tr, y_tr, X_v, y_v):
    resultats, fits = [], {}
    for nom, mod in modeles.items():
        cv_r2 = cross_val_score(mod, X_tr, y_tr, cv=5, scoring='r2', n_jobs=-1)
        mod.fit(X_tr, y_tr)
        yp   = mod.predict(X_v)
        rmse = np.sqrt(mean_squared_error(y_v, yp))
        mae  = mean_absolute_error(y_v, yp)
        r2   = r2_score(y_v, yp)
        print(f"  {nom:<25} | R²={r2:.4f}  RMSE={rmse:.4f}  MAE={mae:.4f}"
              f"  CV-R²={cv_r2.mean():.4f}±{cv_r2.std():.4f}")
        resultats.append({
            "Modèle":    nom,
            "R²":        round(r2, 4),
            "RMSE":      round(rmse, 4),
            "MAE":       round(mae, 4),
            "CV-R² moy": round(cv_r2.mean(), 4),
            "CV-R² std": round(cv_r2.std(), 4),
        })
        fits[nom] = mod
    df_res = pd.DataFrame(resultats).sort_values("R²", ascending=False).reset_index(drop=True)
    return df_res, fits

def run_clf(modeles, X_tr, y_tr, X_v, y_v):
    resultats, fits = [], {}
    for nom, mod in modeles.items():
        cv_f1 = cross_val_score(mod, X_tr, y_tr, cv=5,
                                scoring='f1_weighted', n_jobs=-1)
        mod.fit(X_tr, y_tr)
        yp  = mod.predict(X_v)
        ypr = mod.predict_proba(X_v)[:, 1] if hasattr(mod, "predict_proba") else yp
        acc = accuracy_score(y_v, yp)
        f1  = f1_score(y_v, yp, average='weighted')
        auc = roc_auc_score(y_v, ypr)
        print(f"  {nom:<25} | Acc={acc:.4f}  F1={f1:.4f}  AUC={auc:.4f}"
              f"  CV-F1={cv_f1.mean():.4f}±{cv_f1.std():.4f}")
        resultats.append({
            "Modèle":    nom,
            "Accuracy":  round(acc, 4),
            "F1-score":  round(f1, 4),
            "AUC-ROC":   round(auc, 4),
            "CV-F1 moy": round(cv_f1.mean(), 4),
            "CV-F1 std": round(cv_f1.std(), 4),
        })
        fits[nom] = mod
    df_res = pd.DataFrame(resultats).sort_values("F1-score", ascending=False).reset_index(drop=True)
    return df_res, fits

# ============================================================
# 5. ENTRAÎNEMENT
# ============================================================
print("\n" + "=" * 65)
print("ENJEU 1 — RÉGRESSION CO2 (sans distance_km — anti-leakage)")
print("=" * 65)
df_co2, fits_co2 = run_reg(MODELES_REG, X_tr_co2, y_tr_co2, X_v_co2, y_v_co2)
best_co2 = df_co2.iloc[0]["Modèle"]
print(f"\n  → Meilleur : {best_co2}  R²={df_co2.iloc[0]['R²']}")

print("\n" + "=" * 65)
print("ENJEU 2 — DÉTECTION SOUS-DESSERTE (sans distance_km — anti-leakage)")
print("=" * 65)
df_des, fits_des = run_clf(MODELES_CLF,
                            X_tr_des, y_tr_des, X_v_des, y_v_des)
best_des = df_des.iloc[0]["Modèle"]
print(f"\n  → Meilleur : {best_des}  F1={df_des.iloc[0]['F1-score']}")

# ============================================================
# 6. ÉVALUATION SUR LE JEU DE TEST
# ============================================================
print("\n" + "=" * 65)
print("ÉVALUATION FINALE — JEU DE TEST (utilisé une seule fois)")
print("=" * 65)

yp_co2 = fits_co2[best_co2].predict(X_te_co2)
r2_test  = r2_score(y_te_co2, yp_co2)
rmse_test = np.sqrt(mean_squared_error(y_te_co2, yp_co2))
print(f"\n  CO2 → {best_co2}")
print(f"    R²={r2_test:.4f}  RMSE={rmse_test:.4f} kg")

yp_des = fits_des[best_des].predict(X_te_des)
print(f"\n  Sous-desserte → {best_des}")
print(classification_report(y_te_des, yp_des,
      target_names=["Normal", "Sous-desservi"], digits=4))

# ============================================================
# 7. TABLEAUX COMPARATIFS
# ============================================================
print("\n" + "=" * 65)
print("TABLEAUX COMPARATIFS — TOUS LES MODÈLES")
print("=" * 65)
print("\n  [CO2 — Régression]")
print(df_co2.to_string(index=False))
print("\n  [Sous-desserte — Classification]")
print(df_des.to_string(index=False))

df_co2.to_csv("notebooks/outputs_models/comparatif_co2.csv", index=False)
df_des.to_csv("notebooks/outputs_models/comparatif_desserte.csv", index=False)

# ============================================================
# 8. VISUALISATION — comparaison des 4 algorithmes
# ============================================================
fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle("ObRail Europe — Comparaison des 4 algorithmes ML\n(2 enjeux — sans data leakage)",
             fontsize=13, fontweight='bold')

coul = ["#00C98D", "#0096D6", "#F59E0B", "#6366F1"]

# CO2 — R²
axes[0].barh(df_co2["Modèle"], df_co2["R²"], color=coul, edgecolor='white')
axes[0].set_title("Enjeu 1 — CO2 : R² (↑ mieux)", fontweight='bold')
axes[0].set_xlim(0, 1.05)
for i, v in enumerate(df_co2["R²"]):
    axes[0].text(max(v + 0.01, 0.02), i, f"{v:.4f}", va='center', fontsize=9)

# Sous-desserte — F1
axes[1].barh(df_des["Modèle"], df_des["F1-score"], color=coul, edgecolor='white')
axes[1].set_title("Enjeu 2 — Sous-desserte : F1 (↑ mieux)", fontweight='bold')
axes[1].set_xlim(0, 1.05)
for i, v in enumerate(df_des["F1-score"]):
    axes[1].text(max(v + 0.01, 0.02), i, f"{v:.4f}", va='center', fontsize=9)

plt.tight_layout()
plt.savefig("notebooks/outputs_models/comparaison_modeles.png", dpi=150, bbox_inches='tight')
plt.close()
print("\n✅ comparaison_modeles.png sauvegardée")

# Feature importance
mod_rf_co2 = fits_co2.get("RandomForest")
mod_rf_des = fits_des.get("RandomForest")
if mod_rf_co2 and hasattr(mod_rf_co2, "feature_importances_"):
    fig2, axes2 = plt.subplots(1, 2, figsize=(14, 5))
    fig2.suptitle("ObRail — Feature Importance (Random Forest)", fontsize=13, fontweight='bold')
    for ax, label, mod, cols in [
        (axes2[0], "CO2", mod_rf_co2, X_tr_co2.columns),
        (axes2[1], "Sous-desserte (RF)", mod_rf_des, X_tr_des.columns),
    ]:
        if mod and hasattr(mod, "feature_importances_"):
            imp = pd.Series(mod.feature_importances_, index=cols).sort_values(ascending=True)
            ax.barh(imp.index, imp.values, color="#00C98D", edgecolor='white')
            ax.set_title(label, fontweight='bold')
            ax.set_xlabel("Importance")
    plt.tight_layout()
    plt.savefig("notebooks/outputs_models/feature_importance.png", dpi=150, bbox_inches='tight')
    plt.close()
    print("feature_importance.png sauvegardée")

# ============================================================
# 9. SAUVEGARDE DES MODÈLES RETENUS
# ============================================================
joblib.dump(fits_co2[best_co2], "notebooks/outputs_models/best_model_co2.joblib")
joblib.dump(fits_des[best_des], "notebooks/outputs_models/best_model_desserte.joblib")

print(f"\nModèles sauvegardés :")
print(f"   CO2        → {best_co2}")
print(f"   Sous-dess. → {best_des}")
print("\n→ Prochaine étape : python notebooks/04_evaluation.py")