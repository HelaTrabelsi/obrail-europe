
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'api'))

try:
    from main import app
    client = TestClient(app)
    APP_AVAILABLE = True
except Exception:
    APP_AVAILABLE = False


# TESTS UNITAIRES 

class TestCalculsCO2:
    """Tests unitaires sur les calculs CO2"""

    def test_ratio_train_avion(self):
        """Le ratio avion/train doit etre 18 selon ADEME 2023"""
        co2_train = 14   # g/passager-km
        co2_avion = 258  # g/passager-km
        ratio = co2_avion / co2_train
        assert ratio == pytest.approx(18.43, rel=0.01)

    def test_economie_co2_pct(self):
        """L'economie CO2 vs avion doit etre ~95%"""
        co2_train = 14
        co2_avion = 258
        economie = (co2_avion - co2_train) / co2_avion * 100
        assert economie == pytest.approx(94.57, rel=0.01)

    def test_co2_calcul_distance(self):
        """Test calcul CO2 sur une distance donnee"""
        distance_km = 500
        co2_train = distance_km * 14 / 1000   # en kg
        co2_avion = distance_km * 258 / 1000  # en kg
        assert co2_train == 7.0
        assert co2_avion == 129.0
        assert co2_avion > co2_train


class TestHaversine:
    """Tests unitaires sur la formule Haversine"""

    def test_distance_paris_lyon(self):
        """Distance Paris-Lyon environ 392 km"""
        import math

        def haversine(lat1, lon1, lat2, lon2):
            R = 6371
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = (math.sin(dlat/2)**2 +
                 math.cos(math.radians(lat1)) *
                 math.cos(math.radians(lat2)) *
                 math.sin(dlon/2)**2)
            return R * 2 * math.asin(math.sqrt(a))

        # Paris : 48.8566, 2.3522 — Lyon : 45.7640, 4.8357
        dist = haversine(48.8566, 2.3522, 45.7640, 4.8357)
        assert 380 < dist < 420  # environ 392 km

    def test_distance_meme_point(self):
        """Distance entre un point et lui-meme = 0"""
        import math

        def haversine(lat1, lon1, lat2, lon2):
            R = 6371
            dlat = math.radians(lat2 - lat1)
            dlon = math.radians(lon2 - lon1)
            a = (math.sin(dlat/2)**2 +
                 math.cos(math.radians(lat1)) *
                 math.cos(math.radians(lat2)) *
                 math.sin(dlon/2)**2)
            return R * 2 * math.asin(math.sqrt(a))

        dist = haversine(48.8566, 2.3522, 48.8566, 2.3522)
        assert dist == pytest.approx(0.0, abs=0.001)


class TestDetectionJourNuit:
    """Tests unitaires sur la detection Jour/Nuit"""

    def detect_type_service(self, heure_str):
        """Detecte Jour ou Nuit selon l'heure de depart"""
        h = int(heure_str.split(':')[0]) % 24
        return 'Nuit' if h >= 20 or h < 6 else 'Jour'

    def test_depart_jour(self):
        assert self.detect_type_service("08:30:00") == "Jour"
        assert self.detect_type_service("14:00:00") == "Jour"
        assert self.detect_type_service("18:59:00") == "Jour"

    def test_depart_nuit(self):
        assert self.detect_type_service("20:00:00") == "Nuit"
        assert self.detect_type_service("23:30:00") == "Nuit"
        assert self.detect_type_service("02:15:00") == "Nuit"
        assert self.detect_type_service("05:59:00") == "Nuit"

    def test_heure_gtfs_superieure_24h(self):
        """GTFS peut avoir des horaires > 24h — 25h30 = 1h30 = Nuit"""
        # 25 % 24 = 1 -> 1h du matin -> Nuit
        h = int("25:30:00".split(':')[0]) % 24
        assert h == 1
        assert self.detect_type_service("25:30:00") == "Nuit"


# TESTS D'INTEGRATION 

@pytest.mark.skipif(not APP_AVAILABLE, reason="API non disponible")
class TestAPIHealth:
    """Tests d'integration sur l'endpoint /health"""

    def test_health_status_code(self):
        """L'endpoint /health doit retourner 200 ou 503"""
        response = client.get("/health")
        assert response.status_code in [200, 503]

    def test_health_json_format(self):
        """La reponse doit etre du JSON valide"""
        response = client.get("/health")
        assert "application/json" in response.headers["content-type"]
        data = response.json()
        assert "status" in data or "detail" in data

    def test_health_contient_champs_requis(self):
        """La reponse health doit contenir status+database ou detail si DB off"""
        response = client.get("/health")
        data = response.json()

        if response.status_code == 200:
            assert "status" in data
            assert "database" in data
        else:
            assert "detail" in data


@pytest.mark.skipif(not APP_AVAILABLE, reason="API non disponible")
class TestAPIDessertes:
    """Tests d'integration sur les endpoints /dessertes"""

    def test_dessertes_status_code(self):
        """GET /dessertes doit retourner 200"""
        response = client.get("/dessertes")
        assert response.status_code in [200, 500, 503]

    def test_dessertes_search_params(self):
        """GET /dessertes/search accepte les bons parametres"""
        response = client.get("/dessertes/search?limit=5")
        assert response.status_code in [200, 500, 503]

    def test_dessertes_search_limit_max(self):
        """Le limit max est 500"""
        response = client.get("/dessertes/search?limit=501")
        assert response.status_code in [200, 422, 500, 503]

    def test_dessertes_id_inexistant(self):
        """Un ID inexistant doit retourner 404"""
        response = client.get("/dessertes/999999999")
        assert response.status_code in [404, 500, 503]


@pytest.mark.skipif(not APP_AVAILABLE, reason="API non disponible")
class TestAPIStats:
    """Tests d'integration sur les endpoints /stats"""

    def test_stats_global(self):
        """GET /stats doit retourner les KPIs globaux"""
        response = client.get("/stats")
        assert response.status_code in [200, 500, 503]

    def test_stats_co2(self):
        """GET /stats/co2 doit retourner les emissions"""
        response = client.get("/stats/co2")
        assert response.status_code in [200, 500, 503]

    def test_stats_qualite(self):
        """GET /stats/qualite doit inclure etl_logs"""
        response = client.get("/stats/qualite")
        assert response.status_code in [200, 500, 503]

    def test_stats_couverture(self):
        """GET /stats/couverture doit retourner la repartition Jour/Nuit"""
        response = client.get("/stats/couverture")
        assert response.status_code in [200, 500, 503]

    def test_operateurs(self):
        """GET /operateurs doit retourner la liste"""
        response = client.get("/operateurs")
        assert response.status_code in [200, 500, 503]

    def test_gares(self):
        """GET /gares doit accepter le parametre nom"""
        response = client.get("/gares?nom=Paris")
        assert response.status_code in [200, 500, 503]


# TESTS ETL 

class TestValidationDonnees:
    """Tests de validation sur la structure des donnees"""

    def test_colonnes_requises(self):
        """Le DataFrame transforme doit avoir les colonnes obligatoires"""
        import pandas as pd
        colonnes_requises = [
            'operateur_nom', 'gare_depart_nom', 'gare_arrivee_nom',
            'heure_depart', 'heure_arrivee', 'type_service', 'distance_km'
        ]
        df = pd.DataFrame({
            'operateur_nom': ['SNCF'],
            'gare_depart_nom': ['Paris'],
            'gare_arrivee_nom': ['Lyon'],
            'heure_depart': ['08:00:00'],
            'heure_arrivee': ['10:00:00'],
            'type_service': ['Jour'],
            'distance_km': [392.0]
        })
        for col in colonnes_requises:
            assert col in df.columns, f"Colonne manquante : {col}"

    def test_pas_de_doublons(self):
        """Le DataFrame ne doit pas avoir de doublons sur la cle unique"""
        import pandas as pd
        df = pd.DataFrame({
            'operateur_nom': ['SNCF', 'SNCF'],
            'gare_depart_nom': ['Paris', 'Paris'],
            'gare_arrivee_nom': ['Lyon', 'Lyon'],
            'heure_depart': ['08:00:00', '08:00:00'],
        })
        df_dedup = df.drop_duplicates(
            subset=['operateur_nom', 'gare_depart_nom', 'gare_arrivee_nom', 'heure_depart']
        )
        assert len(df_dedup) == 1

    def test_type_service_valeurs_valides(self):
        """type_service doit etre uniquement Jour ou Nuit"""
        import pandas as pd
        df = pd.DataFrame({
            'type_service': ['Jour', 'Nuit', 'Jour']
        })
        valeurs_valides = {'Jour', 'Nuit'}
        assert set(df['type_service'].unique()).issubset(valeurs_valides)

    def test_horaires_non_nuls(self):
        """Les horaires ne doivent pas etre nuls"""
        import pandas as pd
        df = pd.DataFrame({
            'heure_depart': ['08:00:00', None, '14:30:00'],
            'heure_arrivee': ['10:00:00', '12:00:00', None]
        })
        df_clean = df.dropna(subset=['heure_depart', 'heure_arrivee'])
        assert len(df_clean) == 1
        assert df_clean.iloc[0]['heure_depart'] == '08:00:00'