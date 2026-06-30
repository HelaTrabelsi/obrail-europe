
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import pandas as pd
import numpy as np
import seaborn as sns
import joblib
import warnings
warnings.filterwarnings('ignore')
np.random.seed(42)

from sklearn.metrics import (
    mean_squared_error, mean_absolute_error, r2_score,
    accuracy_score, f1_score, roc_auc_score,
    confusion_matrix, classification_report, roc_curve
)
import os
os.makedirs('notebooks/outputs_eval', exist_ok=True)

SPLITS = 'notebooks/ml_splits'
MODELS = 'notebooks/outputs_models'

print("=" * 65)
print("ObRail Europe — Évaluation finale des modèles ML (2 enjeux)")
print("=" * 65)

# ============================================================
# 1. FEATURES (identiques à 02 et 03)
# ============================================================
FEAT_CO2 = [
    "operateur_enc", "type_service_enc", "type_ligne_enc",
    "pays_enc", "heure_sin", "heure_cos", "distance_bucket_enc"
]
FEAT_DES = [
    "operateur_enc", "pays_enc", "type_ligne_enc",
    "heure_sin", "heure_cos", "co2_par_km"
]

# ============================================================
# 2. CHARGEMENT DES SPLITS ET MODÈLES
# ============================================================
def charger(enjeu, target, features):
    te = pd.read_csv(f"{SPLITS}/{enjeu}_test.csv")
    feat_dispo = [f for f in features if f in te.columns]
    return te[feat_dispo], te[target]

X_te_co2, y_te_co2 = charger("co2",      "co2_emission_kg", FEAT_CO2)
X_te_des, y_te_des = charger("desserte", "sous_desserte",   FEAT_DES)

mod_co2 = joblib.load(f"{MODELS}/best_model_co2.joblib")
mod_des = joblib.load(f"{MODELS}/best_model_desserte.joblib")
scaler  = joblib.load(f"{SPLITS}/scaler.joblib")

print(f"\n✅ Modèles chargés :")
print(f"   CO2        → {type(mod_co2).__name__}")
print(f"   Sous-dess. → {type(mod_des).__name__}")
print(f"   Scaler     → mean_dist={scaler.mean_[0]:.4f}  scale={scaler.scale_[0]:.4f}")
print(f"\nFeatures CO2      : {list(X_te_co2.columns)}")
print(f"Features Desserte : {list(X_te_des.columns)}")

# ============================================================
# 3. PRÉDICTIONS ET MÉTRIQUES
# ============================================================
yp_co2 = mod_co2.predict(X_te_co2)
yp_des = mod_des.predict(X_te_des)

# CO2
rmse = np.sqrt(mean_squared_error(y_te_co2, yp_co2))
mae  = mean_absolute_error(y_te_co2, yp_co2)
r2   = r2_score(y_te_co2, yp_co2)

# Sous-desserte
acc_des = accuracy_score(y_te_des, yp_des)
f1_des  = f1_score(y_te_des, yp_des, average='weighted')
auc_des = roc_auc_score(y_te_des, mod_des.predict_proba(X_te_des)[:, 1])

print("\n" + "=" * 65)
print("MÉTRIQUES FINALES — JEU DE TEST")
print("=" * 65)
print(f"\n  ENJEU 1 — CO2 ({type(mod_co2).__name__})")
print(f"    R²={r2:.4f}  RMSE={rmse:.4f} kg  MAE={mae:.4f} kg")
print(f"\n  ENJEU 2 — Sous-desserte ({type(mod_des).__name__})")
print(f"    Accuracy={acc_des:.4f}  F1={f1_des:.4f}  AUC={auc_des:.4f}")

COLORS = {
    'green': '#00C98D', 'blue': '#0096D6', 'amber': '#F59E0B',
    'red': '#EF4444', 'purple': '#6366F1', 'gray': '#718096'
}

# ============================================================
# FIGURE 1 — ÉVALUATION COMPLÈTE
# ============================================================
fig = plt.figure(figsize=(16, 12))
fig.suptitle('ObRail Europe — Évaluation des Modèles ML\n2 enjeux — Bloc E6.2 — RNCP36581',
             fontsize=15, fontweight='bold', y=0.98)
gs = gridspec.GridSpec(2, 3, figure=fig, hspace=0.55, wspace=0.4)

# ── CO2 : Réel vs Prédit ──
ax1 = fig.add_subplot(gs[0, 0])
idx = np.random.choice(len(y_te_co2), min(2000, len(y_te_co2)), replace=False)
ax1.scatter(np.array(y_te_co2)[idx], yp_co2[idx],
            alpha=0.3, s=8, color=COLORS['blue'])
lim = [float(y_te_co2.min()), float(y_te_co2.max())]
ax1.plot(lim, lim, 'r--', lw=2, label='Idéal')
ax1.set_title(f'CO2 — Réel vs Prédit\nR²={r2:.4f}  RMSE={rmse:.4f} kg',
              fontweight='bold', fontsize=10)
ax1.set_xlabel('CO2 réel (kg)'); ax1.set_ylabel('CO2 prédit (kg)')
ax1.legend(fontsize=8)

# ── CO2 : Résidus ──
ax2 = fig.add_subplot(gs[0, 1])
residus = np.array(y_te_co2) - yp_co2
ax2.scatter(yp_co2[idx], residus[idx], alpha=0.3, s=8, color=COLORS['amber'])
ax2.axhline(0, color='red', linestyle='--', lw=2)
ax2.set_title('CO2 — Résidus\n(centré sur 0 = bon modèle)', fontweight='bold', fontsize=10)
ax2.set_xlabel('CO2 prédit (kg)'); ax2.set_ylabel('Résidu')

# ── CO2 : Distribution résidus ──
ax3 = fig.add_subplot(gs[0, 2])
ax3.hist(residus, bins=60, color=COLORS['green'], edgecolor='white', alpha=0.85)
ax3.axvline(0, color='red', linestyle='--', lw=2)
ax3.axvline(residus.mean(), color='orange', linestyle='--', lw=2,
            label=f'Moy={residus.mean():.4f}')
ax3.set_title('CO2 — Distribution résidus', fontweight='bold', fontsize=10)
ax3.legend(fontsize=8)

# ── Sous-desserte : Matrice de confusion ──
ax4 = fig.add_subplot(gs[1, 0])
cm_des = confusion_matrix(y_te_des, yp_des)
sns.heatmap(cm_des, annot=True, fmt='d', cmap='Oranges', ax=ax4,
            xticklabels=['Normal', 'Sous-des.'],
            yticklabels=['Normal', 'Sous-des.'],
            annot_kws={'size': 12})
ax4.set_title(f'Sous-desserte — Confusion\nF1={f1_des:.4f}  AUC={auc_des:.4f}',
              fontweight='bold', fontsize=10)
ax4.set_ylabel('Réel'); ax4.set_xlabel('Prédit')

# ── Sous-desserte : Courbe ROC ──
ax5 = fig.add_subplot(gs[1, 1])
fpr, tpr, _ = roc_curve(y_te_des, mod_des.predict_proba(X_te_des)[:, 1])
ax5.plot(fpr, tpr, color=COLORS['amber'], lw=2, label=f'AUC={auc_des:.4f}')
ax5.plot([0, 1], [0, 1], 'r--', lw=1, label='Aléatoire')
ax5.fill_between(fpr, tpr, alpha=0.1, color=COLORS['amber'])
ax5.set_title('Sous-desserte — Courbe ROC', fontweight='bold', fontsize=10)
ax5.set_xlabel('Taux faux positifs'); ax5.set_ylabel('Taux vrais positifs')
ax5.legend(fontsize=9)

# ── KPIs résumé ──
ax6 = fig.add_subplot(gs[1, 2])
ax6.axis('off')
kpis = [
    ('CO2 — R²',         f'{r2:.4f}',    '#00C98D'),
    ('CO2 — RMSE (kg)',  f'{rmse:.4f}',  '#0096D6'),
    ('CO2 — MAE (kg)',   f'{mae:.4f}',   '#6366F1'),
    ('Desserte — F1',    f'{f1_des:.4f}', '#F59E0B'),
    ('Desserte — AUC',   f'{auc_des:.4f}','#F59E0B'),
    ('Desserte — Acc.',  f'{acc_des:.4f}','#EF4444'),
]
ax6.set_title('Récapitulatif métriques', fontweight='bold', fontsize=11, pad=15)
for i, (label, val, color) in enumerate(kpis):
    y_pos = 0.92 - i * 0.14
    ax6.add_patch(plt.Rectangle(
        (0.02, y_pos - 0.05), 0.96, 0.11,
        facecolor=color + '22', edgecolor=color, linewidth=1.5,
        transform=ax6.transAxes))
    ax6.text(0.08, y_pos + 0.01, label, transform=ax6.transAxes,
             fontsize=9, color='#2D3748', va='center')
    ax6.text(0.88, y_pos + 0.01, val, transform=ax6.transAxes,
             fontsize=10, fontweight='bold', color=color, va='center', ha='right')

plt.savefig('notebooks/outputs_eval/01_evaluation_complete.png', dpi=150, bbox_inches='tight')
plt.close()
print("\n01_evaluation_complete.png sauvegardée")

# ============================================================
# FIGURE 2 — FEATURE IMPORTANCE
# ============================================================
fig2, axes2 = plt.subplots(1, 2, figsize=(14, 6))
fig2.suptitle('ObRail Europe — Importance des features (2 enjeux)',
              fontsize=13, fontweight='bold')

# CO2 — Feature importance Random Forest
if hasattr(mod_co2, 'feature_importances_'):
    imp_co2 = pd.Series(mod_co2.feature_importances_,
                        index=X_te_co2.columns).sort_values(ascending=True)
    colors_bar = [COLORS['green'] if v > imp_co2.mean() else COLORS['gray']
                  for v in imp_co2.values]
    axes2[0].barh(imp_co2.index, imp_co2.values, color=colors_bar, edgecolor='white')
    axes2[0].axvline(imp_co2.mean(), color='red', linestyle='--', lw=1.5,
                     label=f'Moy={imp_co2.mean():.3f}')
    axes2[0].set_title(f'Enjeu 1 — CO2 ({type(mod_co2).__name__})',
                       fontweight='bold')
    axes2[0].set_xlabel('Importance'); axes2[0].legend()
    for i, v in enumerate(imp_co2.values):
        axes2[0].text(v + 0.001, i, f'{v:.3f}', va='center', fontsize=9)
else:
    # Logistic Regression ou autre — coefficients
    coefs = pd.Series(mod_co2.coef_.flatten(),
                      index=X_te_co2.columns).sort_values(ascending=True)
    colors_c = [COLORS['green'] if v > 0 else COLORS['red'] for v in coefs.values]
    axes2[0].barh(coefs.index, coefs.values, color=colors_c, edgecolor='white')
    axes2[0].set_title(f'Enjeu 1 — CO2 ({type(mod_co2).__name__}) — Coefficients',
                       fontweight='bold')

# Sous-desserte — coefficients ou feature importance
if hasattr(mod_des, 'feature_importances_'):
    imp_des = pd.Series(mod_des.feature_importances_,
                        index=X_te_des.columns).sort_values(ascending=True)
    axes2[1].barh(imp_des.index, imp_des.values,
                  color=COLORS['amber'], edgecolor='white')
    axes2[1].set_title(f'Enjeu 2 — Sous-desserte ({type(mod_des).__name__})',
                       fontweight='bold')
else:
    coefs_des = pd.Series(mod_des.coef_.flatten(),
                          index=X_te_des.columns).sort_values(ascending=True)
    colors_d = [COLORS['amber'] if v > 0 else COLORS['red'] for v in coefs_des.values]
    axes2[1].barh(coefs_des.index, coefs_des.values, color=colors_d, edgecolor='white')
    axes2[1].axvline(0, color='black', lw=1)
    axes2[1].set_title(f'Enjeu 2 — Sous-desserte ({type(mod_des).__name__}) — Coefficients',
                       fontweight='bold')

plt.tight_layout()
plt.savefig('notebooks/outputs_eval/02_feature_importance.png', dpi=150, bbox_inches='tight')
plt.close()
print("✅ 02_feature_importance.png sauvegardée")

# ============================================================
# FIGURE 3 — COMPARAISON DES MODÈLES (depuis CSV)
# ============================================================
fig3, axes3 = plt.subplots(1, 2, figsize=(14, 6))
fig3.suptitle('ObRail — Comparaison 4 modèles (validation set)',
              fontsize=13, fontweight='bold')

df_co2_c = pd.read_csv(f"{MODELS}/comparatif_co2.csv")
df_des_c  = pd.read_csv(f"{MODELS}/comparatif_desserte.csv")
coul = ['#00C98D', '#0096D6', '#F59E0B', '#6366F1']

for ax, df_c, col, title in [
    (axes3[0], df_co2_c, 'R²',      'CO2 — R² sur validation'),
    (axes3[1], df_des_c, 'F1-score', 'Sous-desserte — F1 sur validation'),
]:
    bars = ax.bar(df_c['Modèle'], df_c[col], color=coul,
                  edgecolor='white', width=0.6)
    ax.set_title(title, fontweight='bold')
    ax.tick_params(axis='x', rotation=30)
    for bar, val in zip(bars, df_c[col]):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.002,
                f'{val:.4f}', ha='center', va='bottom', fontsize=9, fontweight='bold')

plt.tight_layout()
plt.savefig('notebooks/outputs_eval/03_comparaison_modeles.png', dpi=150, bbox_inches='tight')
plt.close()
print("✅ 03_comparaison_modeles.png sauvegardée")

# ============================================================
# FIGURE 4 — SIMULATION CO2 (avec scaler correct)
# ============================================================
fig4, axes4 = plt.subplots(1, 2, figsize=(14, 5))
fig4.suptitle('ObRail Europe — Simulation prédictions CO2\n(features v2 sans distance directe)',
              fontsize=13, fontweight='bold')

distances_brutes = np.linspace(1, 917, 200)
CO2_PAR_KM = 0.014

exemple_feat = pd.DataFrame({
    'operateur_enc':       np.zeros(200, dtype=int),
    'type_service_enc':    np.zeros(200, dtype=int),
    'type_ligne_enc':      np.ones(200, dtype=int),
    'pays_enc':            np.zeros(200, dtype=int),
    'heure_sin':           np.full(200, 0.5),
    'heure_cos':           np.full(200, 0.866),
    'distance_bucket_enc': [0 if d < 100 else 1 if d < 300 else 2 if d < 600 else 3
                            for d in distances_brutes],
})

co2_pred   = mod_co2.predict(exemple_feat)
co2_avion  = distances_brutes * 258 / 1000
co2_theoriq = distances_brutes * 14 / 1000

axes4[0].plot(distances_brutes, co2_pred,     color=COLORS['green'], lw=2.5,
              label=f'Train prédit ({type(mod_co2).__name__})')
axes4[0].plot(distances_brutes, co2_theoriq,  color=COLORS['blue'],  lw=1.5,
              linestyle='--', label='Train théorique (14 g/km)')
axes4[0].plot(distances_brutes, co2_avion,    color=COLORS['red'],   lw=2,
              label='Avion (258 g/km)')
axes4[0].fill_between(distances_brutes, co2_pred, co2_avion,
                       alpha=0.08, color='green', label='CO2 économisé')
axes4[0].set_title('CO2 Train vs Avion selon la catégorie de distance', fontweight='bold')
axes4[0].set_xlabel('Distance (km)'); axes4[0].set_ylabel('CO2 (kg)')
axes4[0].legend(fontsize=9)

# Exemples trajets réels
trajets = {
    'Paris→Lyon\n392km': 392,
    'Paris→Berlin\n878km': 878,
    'Brux.→Paris\n265km': 265,
    'Munich→Hambourg\n612km': 612
}
dist_vals = np.array(list(trajets.values()))
bucket_v  = [0 if d < 100 else 1 if d < 300 else 2 if d < 600 else 3
             for d in dist_vals]

ex_feat = pd.DataFrame({
    'operateur_enc':       [0, 0, 1, 0],
    'type_service_enc':    [0, 0, 0, 1],
    'type_ligne_enc':      [1, 1, 1, 1],
    'pays_enc':            [2, 0, 1, 0],
    'heure_sin':           [0.5] * 4,
    'heure_cos':           [0.866] * 4,
    'distance_bucket_enc': bucket_v,
})

co2_trains = mod_co2.predict(ex_feat)
co2_avions = dist_vals * 258 / 1000

x = np.arange(len(trajets)); w = 0.35
axes4[1].bar(x - w / 2, co2_trains, w, color=COLORS['green'],
             label='Train (prédit)', edgecolor='white')
axes4[1].bar(x + w / 2, co2_avions, w, color=COLORS['red'],
             label='Avion', edgecolor='white')
axes4[1].set_title('Exemples réels — CO2 Train vs Avion', fontweight='bold')
axes4[1].set_xticks(x)
axes4[1].set_xticklabels(list(trajets.keys()), fontsize=9)
axes4[1].set_ylabel('CO2 (kg)'); axes4[1].legend()

for i, (t, a) in enumerate(zip(co2_trains, co2_avions)):
    axes4[1].text(i - w / 2, t + 0.01, f'{t:.2f}kg',
                  ha='center', fontsize=8, fontweight='bold')
    axes4[1].text(i + w / 2, a + 0.01, f'{a:.1f}kg',
                  ha='center', fontsize=8, fontweight='bold')
    eco = (a - t) / a * 100 if a > 0 else 0
    axes4[1].text(i, max(t, a) + 0.5, f'-{eco:.0f}%',
                  ha='center', fontsize=9, color=COLORS['green'], fontweight='bold')

plt.tight_layout()
plt.savefig('notebooks/outputs_eval/04_simulation_co2.png', dpi=150, bbox_inches='tight')
plt.close()
print("04_simulation_co2.png sauvegardée")

# ============================================================
# RAPPORT FINAL
# ============================================================
print("\n" + "=" * 65)
print("RAPPORT D'ÉVALUATION FINAL — 2 enjeux (version corrigée)")
print("=" * 65)
print(f"""
ENJEU 1 — RÉGRESSION CO2 (sans data leakage)
  Modèle    : {type(mod_co2).__name__}
  Features  : {list(X_te_co2.columns)}
  R²        = {r2:.4f}  → explique {r2*100:.2f}% de la variance
  RMSE      = {rmse:.4f} kg
  MAE       = {mae:.4f} kg
  Référence : 14 g/km (ADEME) | Scaler: mean={scaler.mean_[0]:.4f} scale={scaler.scale_[0]:.4f}

ENJEU 2 — DÉTECTION SOUS-DESSERTE (sans data leakage)
  Modèle    : {type(mod_des).__name__}
  Features  : {list(X_te_des.columns)}
  Accuracy  = {acc_des:.4f}
  F1        = {f1_des:.4f}
  AUC-ROC   = {auc_des:.4f}

""")

print("Figures générées dans notebooks/outputs_eval/")
print("   01_evaluation_complete.png")
print("   02_feature_importance.png")
print("   03_comparaison_modeles.png")
print("   04_simulation_co2.png")
print("\n Prochaine étape : python notebooks/05_gridsearch.py")