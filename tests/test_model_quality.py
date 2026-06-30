
import pytest
import numpy as np
import pandas as pd
import joblib
import os

MODELS_DIR = "notebooks/outputs_models"
SPLITS_DIR = "notebooks/ml_splits"

# Seuils de qualite minimale acceptable en production
SEUIL_R2_CO2_MIN = 0.75        # en dessous : modele inacceptable
SEUIL_F1_SUBST_MIN = 0.45      # en dessous : pas mieux qu'un classifieur naif ameliore
SEUIL_AUC_DESSERTE_MIN = 0.65  # en dessous : proche de l'aleatoire (0.50)

CO2_MAX_PHYSIQUE = 50.0   # kg : aucun trajet ferroviaire ne devrait depasser ca
CO2_MIN_PHYSIQUE = 0.0    # kg : ne peut pas etre negatif


@pytest.fixture(scope="module")
def modeles():
    return {
        "co2": joblib.load(f"{MODELS_DIR}/best_model_co2.joblib"),
        "substitution": joblib.load(f"{MODELS_DIR}/best_model_nuit.joblib"),
        "desserte": joblib.load(f"{MODELS_DIR}/best_model_desserte.joblib"),
    }


@pytest.fixture(scope="module")
def test_sets():
    return {
        "co2": (
            pd.read_csv(f"{SPLITS_DIR}/co2_test.csv").drop(columns="co2_emission_kg"),
            pd.read_csv(f"{SPLITS_DIR}/co2_test.csv")["co2_emission_kg"],
        ),
        "substitution": (
            pd.read_csv(f"{SPLITS_DIR}/nuit_test.csv").drop(columns="type_service_enc"),
            pd.read_csv(f"{SPLITS_DIR}/nuit_test.csv")["type_service_enc"],
        ),
        "desserte": (
            pd.read_csv(f"{SPLITS_DIR}/desserte_test.csv").drop(columns="sous_desserte"),
            pd.read_csv(f"{SPLITS_DIR}/desserte_test.csv")["sous_desserte"],
        ),
    }


# ============================================================
# 1. INTEGRITE DES PREDICTIONS — pas de NaN/Inf
# ============================================================

class TestIntegritePredicitions:

    def test_co2_pas_de_nan(self, modeles, test_sets):
        X, _ = test_sets["co2"]
        preds = modeles["co2"].predict(X)
        assert not np.isnan(preds).any(), "Le modele CO2 produit des NaN"
        assert not np.isinf(preds).any(), "Le modele CO2 produit des Inf"

    def test_substitution_pas_de_nan(self, modeles, test_sets):
        X, _ = test_sets["substitution"]
        preds = modeles["substitution"].predict(X)
        assert not np.isnan(preds).any(), "Le modele substitution produit des NaN"

    def test_desserte_pas_de_nan(self, modeles, test_sets):
        X, _ = test_sets["desserte"]
        preds = modeles["desserte"].predict(X)
        assert not np.isnan(preds).any(), "Le modele desserte produit des NaN"


# ============================================================
# 2. BORNES PHYSIQUES REALISTES
# ============================================================

class TestBornesPhysiques:

    def test_co2_jamais_negatif(self, modeles, test_sets):
        X, _ = test_sets["co2"]
        preds = modeles["co2"].predict(X)
        assert (preds >= CO2_MIN_PHYSIQUE).all(), \
            f"Predictions CO2 negatives detectees (min={preds.min():.3f})"

    def test_co2_jamais_extreme(self, modeles, test_sets):
        X, _ = test_sets["co2"]
        preds = modeles["co2"].predict(X)
        assert (preds <= CO2_MAX_PHYSIQUE).all(), \
            f"Predictions CO2 irrealistes detectees (max={preds.max():.3f} kg)"

    def test_substitution_classes_valides(self, modeles, test_sets):
        X, _ = test_sets["substitution"]
        preds = modeles["substitution"].predict(X)
        assert set(np.unique(preds)).issubset({0, 1}), \
            "Le modele substitution predit des classes hors {0,1}"

    def test_desserte_classes_valides(self, modeles, test_sets):
        X, _ = test_sets["desserte"]
        preds = modeles["desserte"].predict(X)
        assert set(np.unique(preds)).issubset({0, 1}), \
            "Le modele desserte predit des classes hors {0,1}"


# ============================================================
# 3. DETERMINISME — memes inputs -> memes outputs
# ============================================================

class TestDeterminisme:

    def test_co2_deterministe(self, modeles, test_sets):
        X, _ = test_sets["co2"]
        sample = X.iloc[:50]
        p1 = modeles["co2"].predict(sample)
        p2 = modeles["co2"].predict(sample)
        np.testing.assert_array_almost_equal(p1, p2, decimal=8,
            err_msg="Le modele CO2 n'est pas deterministe")

    def test_substitution_deterministe(self, modeles, test_sets):
        X, _ = test_sets["substitution"]
        sample = X.iloc[:50]
        p1 = modeles["substitution"].predict(sample)
        p2 = modeles["substitution"].predict(sample)
        np.testing.assert_array_equal(p1, p2,
            err_msg="Le modele substitution n'est pas deterministe")


# ============================================================
# 4. PERFORMANCE MINIMALE — non-regression
# ============================================================

class TestPerformanceMinimale:

    def test_co2_r2_minimum_garanti(self, modeles, test_sets):
        from sklearn.metrics import r2_score
        X, y = test_sets["co2"]
        preds = modeles["co2"].predict(X)
        r2 = r2_score(y, preds)
        assert r2 >= SEUIL_R2_CO2_MIN, \
            f"R² CO2 = {r2:.4f} en dessous du seuil minimum {SEUIL_R2_CO2_MIN}"

    def test_substitution_f1_minimum_garanti(self, modeles, test_sets):
        from sklearn.metrics import f1_score
        X, y = test_sets["substitution"]
        preds = modeles["substitution"].predict(X)
        f1 = f1_score(y, preds, average="weighted")
        assert f1 >= SEUIL_F1_SUBST_MIN, \
            f"F1 substitution = {f1:.4f} en dessous du seuil minimum {SEUIL_F1_SUBST_MIN}"

    def test_desserte_auc_minimum_garanti(self, modeles, test_sets):
        from sklearn.metrics import roc_auc_score
        X, y = test_sets["desserte"]
        if hasattr(modeles["desserte"], "predict_proba"):
            proba = modeles["desserte"].predict_proba(X)[:, 1]
            auc = roc_auc_score(y, proba)
            assert auc >= SEUIL_AUC_DESSERTE_MIN, \
                f"AUC desserte = {auc:.4f} en dessous du seuil minimum {SEUIL_AUC_DESSERTE_MIN}"


# ============================================================
# 5. COHERENCE METIER — sanity checks
# ============================================================

class TestCoherenceMetier:

    def test_co2_correle_avec_distance_bucket(self, modeles, test_sets):
        """Un trajet longue distance (bucket eleve) doit predire plus de CO2
        qu'un trajet courte distance (bucket bas), toutes choses egales."""
        X, _ = test_sets["co2"]
        sample = X.iloc[:1].copy()

        sample_court = sample.copy()
        sample_court["distance_bucket_enc"] = 0  # <100km

        sample_long = sample.copy()
        sample_long["distance_bucket_enc"] = 3   # >600km

        co2_court = modeles["co2"].predict(sample_court)[0]
        co2_long = modeles["co2"].predict(sample_long)[0]

        assert co2_long > co2_court, \
            f"Incoherence : trajet long predit moins de CO2 ({co2_long:.3f}) " \
            f"qu'un trajet court ({co2_court:.3f})"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])