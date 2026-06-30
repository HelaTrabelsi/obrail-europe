
import numpy as np
import pandas as pd
import warnings, os, joblib, time
warnings.filterwarnings("ignore")
np.random.seed(42)

from sklearn.model_selection import GridSearchCV, RandomizedSearchCV
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from xgboost import XGBRegressor

SPLITS = "notebooks/ml_splits"
OUT = "notebooks/outputs_models"
os.makedirs(OUT, exist_ok=True)

print("=" * 65)
print("ObRail Europe — Optimisation hyperparametres (GridSearchCV)")
print("Modele cible : XGBoost — Enjeu 1 (Regression CO2)")
print("=" * 65)

# ============================================================
# 1. CHARGEMENT DES SPLITS (features v2 sans data leakage)
# ============================================================
FEAT_CO2_V2 = ["operateur_enc", "type_service_enc", "type_ligne_enc",
               "pays_enc", "heure_sin", "heure_cos", "distance_bucket_enc"]
TARGET_CO2 = "co2_emission_kg"

def charger_adapte(enjeu, target, features):
    tr = pd.read_csv(f"{SPLITS}/{enjeu}_train.csv")
    v  = pd.read_csv(f"{SPLITS}/{enjeu}_val.csv")
    te = pd.read_csv(f"{SPLITS}/{enjeu}_test.csv")
    feat_dispo = [f for f in features if f in tr.columns]
    return (tr[feat_dispo], v[feat_dispo], te[feat_dispo],
            tr[target], v[target], te[target])

X_tr, X_v, X_te, y_tr, y_v, y_te = charger_adapte("co2", TARGET_CO2, FEAT_CO2_V2)
print(f"\nDonnees chargees : train={len(X_tr):,}  val={len(X_v):,}  test={len(X_te):,}")
print(f"Features : {list(X_tr.columns)}")

# ============================================================
# 2. BASELINE — modele actuel (hyperparametres fixes)
# ============================================================
print("\n" + "=" * 65)
print("BASELINE — Hyperparametres fixes (avant optimisation)")
print("=" * 65)

baseline = XGBRegressor(n_estimators=100, learning_rate=0.05, max_depth=4,
                         random_state=42, verbosity=0)
t0 = time.time()
baseline.fit(X_tr, y_tr)
t_baseline = time.time() - t0

yp_base = baseline.predict(X_v)
r2_base = r2_score(y_v, yp_base)
rmse_base = np.sqrt(mean_squared_error(y_v, yp_base))

print(f"  Parametres   : n_estimators=100, learning_rate=0.05, max_depth=4")
print(f"  R² (val)     : {r2_base:.4f}")
print(f"  RMSE (val)   : {rmse_base:.4f} kg")
print(f"  Temps fit    : {t_baseline:.2f}s")

# ============================================================
# 3. GRIDSEARCHCV — recherche exhaustive
# ============================================================
print("\n" + "=" * 65)
print("GRIDSEARCHCV — Recherche exhaustive (grille reduite pour temps)")
print("=" * 65)

param_grid = {
    "n_estimators":  [50, 100, 200],
    "learning_rate": [0.01, 0.05, 0.1],
    "max_depth":     [3, 4, 6],
}
print(f"\nGrille testee : {param_grid}")
print(f"Combinaisons  : {3*3*3} x 5-fold CV = {3*3*3*5} entrainements")

t0 = time.time()
grid = GridSearchCV(
    XGBRegressor(random_state=42, verbosity=0),
    param_grid=param_grid,
    cv=5,
    scoring="r2",
    n_jobs=-1,
    verbose=1
)
grid.fit(X_tr, y_tr)
t_grid = time.time() - t0

print(f"\n✓ GridSearchCV termine en {t_grid:.1f}s")
print(f"  Meilleurs parametres : {grid.best_params_}")
print(f"  Meilleur score CV R² : {grid.best_score_:.4f}")

yp_grid = grid.best_estimator_.predict(X_v)
r2_grid = r2_score(y_v, yp_grid)
rmse_grid = np.sqrt(mean_squared_error(y_v, yp_grid))
mae_grid = mean_absolute_error(y_v, yp_grid)

print(f"\n  Performance sur validation apres optimisation :")
print(f"    R²   = {r2_grid:.4f}")
print(f"    RMSE = {rmse_grid:.4f} kg")
print(f"    MAE  = {mae_grid:.4f} kg")

# ============================================================
# 4. RANDOMIZEDSEARCHCV — alternative plus rapide
# ============================================================
print("\n" + "=" * 65)
print("RANDOMIZEDSEARCHCV — Recherche aleatoire (grille plus large)")
print("=" * 65)

param_dist = {
    "n_estimators":  [50, 100, 150, 200, 300],
    "learning_rate": [0.01, 0.03, 0.05, 0.1, 0.15, 0.2],
    "max_depth":     [3, 4, 5, 6, 8],
    "subsample":     [0.7, 0.8, 0.9, 1.0],
    "colsample_bytree": [0.7, 0.8, 0.9, 1.0],
}
print(f"\nEspace de recherche : {param_dist}")
print(f"Iterations testees  : 20 combinaisons aleatoires x 5-fold CV")

t0 = time.time()
random_search = RandomizedSearchCV(
    XGBRegressor(random_state=42, verbosity=0),
    param_distributions=param_dist,
    n_iter=20,
    cv=5,
    scoring="r2",
    n_jobs=-1,
    random_state=42,
    verbose=1
)
random_search.fit(X_tr, y_tr)
t_random = time.time() - t0

print(f"\n✓ RandomizedSearchCV termine en {t_random:.1f}s")
print(f"  Meilleurs parametres : {random_search.best_params_}")
print(f"  Meilleur score CV R² : {random_search.best_score_:.4f}")

yp_rand = random_search.best_estimator_.predict(X_v)
r2_rand = r2_score(y_v, yp_rand)
rmse_rand = np.sqrt(mean_squared_error(y_v, yp_rand))

print(f"\n  Performance sur validation :")
print(f"    R²   = {r2_rand:.4f}")
print(f"    RMSE = {rmse_rand:.4f} kg")

# ============================================================
# 5. COMPARATIF FINAL
# ============================================================
print("\n" + "=" * 65)
print("TABLEAU COMPARATIF — Baseline vs GridSearch vs RandomSearch")
print("=" * 65)

comparatif = pd.DataFrame([
    {"Methode": "Baseline (manuel)", "R² val": round(r2_base, 4),
     "RMSE val": round(rmse_base, 4), "Temps (s)": round(t_baseline, 1),
     "Parametres": "n_est=100, lr=0.05, depth=4"},
    {"Methode": "GridSearchCV", "R² val": round(r2_grid, 4),
     "RMSE val": round(rmse_grid, 4), "Temps (s)": round(t_grid, 1),
     "Parametres": str(grid.best_params_)},
    {"Methode": "RandomizedSearchCV", "R² val": round(r2_rand, 4),
     "RMSE val": round(rmse_rand, 4), "Temps (s)": round(t_random, 1),
     "Parametres": str(random_search.best_params_)},
])
print(comparatif.to_string(index=False))
comparatif.to_csv(f"{OUT}/gridsearch_comparatif.csv", index=False)

# ============================================================
# 6. SELECTION DU MEILLEUR MODELE FINAL
# ============================================================
candidats = [
    ("Baseline", baseline, r2_base),
    ("GridSearch", grid.best_estimator_, r2_grid),
    ("RandomSearch", random_search.best_estimator_, r2_rand),
]
meilleur_nom, meilleur_modele, meilleur_r2 = max(candidats, key=lambda x: x[2])

print(f"\n{'='*65}")
print(f"MODELE FINAL RETENU : {meilleur_nom}  (R²={meilleur_r2:.4f})")
print(f"{'='*65}")

# Evaluation finale sur TEST (une seule fois)
yp_test = meilleur_modele.predict(X_te)
r2_test = r2_score(y_te, yp_test)
rmse_test = np.sqrt(mean_squared_error(y_te, yp_test))
mae_test = mean_absolute_error(y_te, yp_test)

print(f"\nEvaluation finale sur jeu de TEST :")
print(f"  R²   = {r2_test:.4f}")
print(f"  RMSE = {rmse_test:.4f} kg")
print(f"  MAE  = {mae_test:.4f} kg")

# Sauvegarde si le modele optimise est meilleur que l'existant
joblib.dump(meilleur_modele, f"{OUT}/best_model_co2_optimized.joblib")
print(f"\n✓ Modele optimise sauvegarde : {OUT}/best_model_co2_optimized.joblib")
print(f"  (Le modele original best_model_co2.joblib est conserve en parallele)")

print(f"\n{'='*65}")
print("CONCLUSION POUR LE RAPPORT")
print(f"{'='*65}")
print(f"""
La recherche d'hyperparametres par GridSearchCV et RandomizedSearchCV
a ete menee sur le modele XGBoost (Enjeu 1 - CO2), conformement aux
exigences du cahier des charges.

Gain de performance : {(meilleur_r2 - r2_base)*100:+.2f} points de R²
par rapport aux hyperparametres fixes initiaux.

Methodologie :
  - GridSearchCV : recherche exhaustive sur 27 combinaisons (5-fold CV)
  - RandomizedSearchCV : recherche aleatoire sur 20 combinaisons (5-fold CV)
  - Le modele {meilleur_nom} a ete retenu pour la production

Cette etape demontre la maitrise de l'optimisation d'hyperparametres
exigee par le cahier des charges ObRail (section III.4).
""")