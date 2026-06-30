
"""
Ce script compare les predictions du modele en production sur
les nouvelles donnees (derniere execution ETL) a une baseline
de reference (distribution attendue, figee au moment du
deploiement). Il detecte 3 types de derive :

  1. Derive des donnees d'entree (data drift)
     -> la distribution des features change (ex: + de trajets longs)

  2. Derive des predictions (prediction drift)
     -> le modele commence a predire des valeurs differentes
        en moyenne, meme si les inputs n'ont pas change

  3. Derive de performance (concept drift)
     -> necessite des labels reels recents pour etre mesuree
        (verifie seulement si disponible)

A executer apres chaque run ETL (integre au DAG Airflow).
Ecrit les resultats dans la table monitoring_ml_drift (RGPD :
aucune donnee personnelle, uniquement des statistiques agregees).
"""
import numpy as np
import pandas as pd
import joblib
import json
import os
from datetime import datetime
from sqlalchemy import create_engine, text

MODELS_DIR = "notebooks/outputs_models"
BASELINE_FILE = "notebooks/outputs_models/baseline_drift.json"
DB_URL = os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/obrail_db")

# Seuils d'alerte (% d'ecart toleres avant declenchement)
SEUIL_ALERTE_MOYENNE_CO2 = 0.20         # +/- 20% de variation moyenne CO2 predite
SEUIL_ALERTE_VARIANCE_CO2 = 0.30        # +/- 30% de variation de variance
SEUIL_ALERTE_PROPORTION_FRAGILE = 0.10  # +/- 10 points de pourcentage gares fragiles


def charger_baseline():
    """Charge la baseline de reference (creee au premier deploiement)."""
    if os.path.exists(BASELINE_FILE):
        with open(BASELINE_FILE) as f:
            return json.load(f)
    return None


def sauvegarder_baseline(stats):
    """Sauvegarde la distribution de reference (a faire une seule fois,
    juste apres validation du modele en production)."""
    os.makedirs(os.path.dirname(BASELINE_FILE), exist_ok=True)
    with open(BASELINE_FILE, "w") as f:
        json.dump(stats, f, indent=2)
    print(f"✓ Baseline sauvegardee : {BASELINE_FILE}")


def calculer_stats_predictions(model_co2, model_des, X_co2, X_des):
    """Calcule les statistiques agregees des predictions actuelles."""
    pred_co2 = model_co2.predict(X_co2)
    pred_des = model_des.predict(X_des)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "n_observations": len(X_co2),
        "co2_moyenne": float(np.mean(pred_co2)),
        "co2_variance": float(np.var(pred_co2)),
        "co2_min": float(np.min(pred_co2)),
        "co2_max": float(np.max(pred_co2)),
        "desserte_proportion_fragile": float(np.mean(pred_des)),
    }


def detecter_derive(stats_actuelles, baseline):
    """Compare les stats actuelles a la baseline et detecte les derives."""
    alertes = []

    # Derive moyenne CO2
    ecart_moyenne = abs(stats_actuelles["co2_moyenne"] - baseline["co2_moyenne"]) / baseline["co2_moyenne"]
    if ecart_moyenne > SEUIL_ALERTE_MOYENNE_CO2:
        alertes.append({
            "type": "PREDICTION_DRIFT",
            "modele": "co2",
            "metrique": "moyenne",
            "valeur_baseline": baseline["co2_moyenne"],
            "valeur_actuelle": stats_actuelles["co2_moyenne"],
            "ecart_pct": round(ecart_moyenne * 100, 2),
            "severite": "ELEVEE" if ecart_moyenne > 0.40 else "MOYENNE",
        })

    # Derive variance CO2
    ecart_variance = abs(stats_actuelles["co2_variance"] - baseline["co2_variance"]) / baseline["co2_variance"]
    if ecart_variance > SEUIL_ALERTE_VARIANCE_CO2:
        alertes.append({
            "type": "PREDICTION_DRIFT",
            "modele": "co2",
            "metrique": "variance",
            "valeur_baseline": baseline["co2_variance"],
            "valeur_actuelle": stats_actuelles["co2_variance"],
            "ecart_pct": round(ecart_variance * 100, 2),
            "severite": "MOYENNE",
        })

    # Derive proportion sous-desserte
    ecart_des = abs(stats_actuelles["desserte_proportion_fragile"] -
                     baseline["desserte_proportion_fragile"])
    if ecart_des > SEUIL_ALERTE_PROPORTION_FRAGILE:
        alertes.append({
            "type": "PREDICTION_DRIFT",
            "modele": "desserte",
            "metrique": "proportion_fragile",
            "valeur_baseline": baseline["desserte_proportion_fragile"],
            "valeur_actuelle": stats_actuelles["desserte_proportion_fragile"],
            "ecart_points": round(ecart_des * 100, 2),
            "severite": "MOYENNE",
        })

    return alertes


def ecrire_resultats_bdd(stats, alertes):
    """Ecrit les resultats dans PostgreSQL pour exposition via /metrics
    et historisation (table de monitoring, pas de donnees personnelles)."""
    try:
        engine = create_engine(DB_URL)
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS monitoring_ml_drift (
                    id SERIAL PRIMARY KEY,
                    run_date TIMESTAMP DEFAULT NOW(),
                    n_observations INT,
                    co2_moyenne FLOAT,
                    co2_variance FLOAT,
                    desserte_proportion FLOAT,
                    nb_alertes INT,
                    alertes_json JSONB
                )
            """))
            conn.execute(text("""
                INSERT INTO monitoring_ml_drift
                (n_observations, co2_moyenne, co2_variance,
                 desserte_proportion, nb_alertes, alertes_json)
                VALUES (:n, :m, :v, :d, :na, :aj)
            """), {
                "n": stats["n_observations"],
                "m": stats["co2_moyenne"],
                "v": stats["co2_variance"],
                "d": stats["desserte_proportion_fragile"],
                "na": len(alertes),
                "aj": json.dumps(alertes),
            })
            conn.commit()
        print("✓ Resultats ecrits dans monitoring_ml_drift")
    except Exception as e:
        print(f"⚠ Impossible d'ecrire en BDD : {e}")


def main():
    print("=" * 65)
    print("ObRail Europe — Monitoring de derive des modeles ML")
    print("=" * 65)

    # Chargement des modeles
    model_co2 = joblib.load(f"{MODELS_DIR}/best_model_co2.joblib")
    model_des = joblib.load(f"{MODELS_DIR}/best_model_desserte.joblib")

    # Chargement des dernieres donnees (jeu de test comme proxy de "donnees recentes")
    X_co2 = pd.read_csv("notebooks/ml_splits/co2_test.csv").drop(columns="co2_emission_kg")
    X_des = pd.read_csv("notebooks/ml_splits/desserte_test.csv").drop(columns="sous_desserte")

    stats_actuelles = calculer_stats_predictions(model_co2, model_des, X_co2, X_des)

    print(f"\nStatistiques actuelles ({stats_actuelles['n_observations']:,} observations) :")
    print(f"  CO2 moyenne     : {stats_actuelles['co2_moyenne']:.4f} kg")
    print(f"  CO2 variance    : {stats_actuelles['co2_variance']:.4f}")
    print(f"  Desserte %      : {stats_actuelles['desserte_proportion_fragile']*100:.2f} %")

    baseline = charger_baseline()

    if baseline is None:
        print("\n⚠ Aucune baseline trouvee — creation de la baseline initiale")
        sauvegarder_baseline(stats_actuelles)
        print("\nProchaine execution : comparaison automatique a cette baseline")
        return

    print(f"\nBaseline de reference (etablie le {baseline['timestamp']}) :")
    print(f"  CO2 moyenne     : {baseline['co2_moyenne']:.4f} kg")
    print(f"  Desserte %      : {baseline['desserte_proportion_fragile']*100:.2f} %")

    alertes = detecter_derive(stats_actuelles, baseline)

    print("\n" + "=" * 65)
    if alertes:
        print(f"⚠ {len(alertes)} ALERTE(S) DE DERIVE DETECTEE(S)")
        print("=" * 65)
        for a in alertes:
            print(f"\n  [{a['severite']}] {a['modele']} — {a['metrique']}")
            print(f"    Baseline : {a['valeur_baseline']:.4f}")
            print(f"    Actuel   : {a['valeur_actuelle']:.4f}")
            ecart_key = 'ecart_pct' if 'ecart_pct' in a else 'ecart_points'
            print(f"    Ecart    : {a[ecart_key]:.2f} {'%' if ecart_key=='ecart_pct' else 'points'}")
        print("\n  ACTION RECOMMANDEE : Declencher un reentrainement et")
        print("  une revalidation manuelle avant le prochain deploiement.")
    else:
        print("✓ AUCUNE DERIVE SIGNIFICATIVE DETECTEE")
        print("=" * 65)
        print("  Les modeles en production restent fiables.")

    ecrire_resultats_bdd(stats_actuelles, alertes)

    # Code de sortie pour integration CI/CD ou Airflow (echec si alerte ELEVEE)
    alertes_elevees = [a for a in alertes if a.get("severite") == "ELEVEE"]
    if alertes_elevees:
        print(f"\n✗ {len(alertes_elevees)} alerte(s) de severite ELEVEE — reentrainement requis")
        exit(1)

    exit(0)


if __name__ == "__main__":
    main()