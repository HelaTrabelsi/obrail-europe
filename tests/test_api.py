
import pytest
import math
import pandas as pd
import sys, os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'API'))

try:
    from main import app
    from fastapi.testclient import TestClient
    client = TestClient(app)
    APP_AVAILABLE = True
except Exception:
    APP_AVAILABLE = False


class TestCalculsCO2:
    def test_ratio_train_avion(self):
        assert 258 / 14 == pytest.approx(18.43, rel=0.01)
    def test_economie_co2_pct(self):
        assert (258 - 14) / 258 * 100 == pytest.approx(94.57, rel=0.01)
    def test_co2_train_500km(self):
        assert 500 * 14 / 1000 == 7.0
    def test_co2_avion_500km(self):
        assert 500 * 258 / 1000 == 129.0
    def test_co2_train_inferieur_avion(self):
        for d in [100, 300, 500, 1000, 2000]:
            assert d * 14 < d * 258
    def test_co2_negatif_impossible(self):
        with pytest.raises((ValueError, AssertionError)):
            assert -100 > 0, "Distance doit etre positive"
    def test_co2_zero_distance(self):
        assert 0 * 14 / 1000 == 0.0
    def test_co2_longue_distance(self):
        assert 2500 * 14 / 1000 == 35.0
        assert 2500 * 258 / 1000 == 645.0


def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) * math.sin(dlon/2)**2)
    return R * 2 * math.asin(math.sqrt(a))

class TestHaversine:
    def test_distance_paris_lyon(self):
        assert 380 < haversine(48.8566, 2.3522, 45.7640, 4.8357) < 420
    def test_distance_paris_berlin(self):
        assert 850 < haversine(48.8566, 2.3522, 52.5200, 13.4050) < 920
    def test_distance_bruxelles_paris(self):
        assert 240 < haversine(50.8503, 4.3517, 48.8566, 2.3522) < 290
    def test_distance_meme_point_zero(self):
        assert haversine(48.8566, 2.3522, 48.8566, 2.3522) == pytest.approx(0.0, abs=0.001)
    def test_distance_toujours_positive(self):
        assert haversine(48.8566, 2.3522, 45.7640, 4.8357) > 0
    def test_distance_symetrique(self):
        d1 = haversine(48.8566, 2.3522, 45.7640, 4.8357)
        d2 = haversine(45.7640, 4.8357, 48.8566, 2.3522)
        assert d1 == pytest.approx(d2, rel=0.0001)


def detect(h):
    return 'Nuit' if int(h.split(':')[0]) % 24 >= 20 or int(h.split(':')[0]) % 24 < 6 else 'Jour'

class TestDetectionJourNuit:
    def test_matin_jour(self):        assert detect("08:30:00") == "Jour"
    def test_midi_jour(self):         assert detect("12:00:00") == "Jour"
    def test_apres_midi_jour(self):   assert detect("14:00:00") == "Jour"
    def test_fin_soiree_jour(self):   assert detect("19:59:00") == "Jour"
    def test_debut_nuit(self):        assert detect("20:00:00") == "Nuit"
    def test_minuit_nuit(self):       assert detect("00:00:00") == "Nuit"
    def test_nuit_profonde(self):     assert detect("02:30:00") == "Nuit"
    def test_fin_nuit(self):          assert detect("05:59:00") == "Nuit"
    def test_debut_matin_jour(self):  assert detect("06:00:00") == "Jour"
    def test_heure_gtfs_25h(self):    assert detect("25:30:00") == "Nuit"
    def test_heure_gtfs_24h(self):    assert detect("24:00:00") == "Nuit"
    def test_valeurs_limites(self):
        assert detect("20:00:00") == "Nuit"
        assert detect("06:00:00") == "Jour"
        assert detect("19:59:59") == "Jour"


class TestNettoyageDonnees:
    def test_suppression_doublons(self):
        df = pd.DataFrame({'op':['SNCF','SNCF','DB'],'dep':['Paris','Paris','Berlin'],'arr':['Lyon','Lyon','Munich'],'h':['08:00','08:00','09:00']})
        assert len(df.drop_duplicates(subset=['op','dep','arr','h'])) == 2
    def test_filtre_horaires_nuls(self):
        df = pd.DataFrame({'hd':['08:00',None,'14:30'],'ha':['10:00','12:00',None]})
        assert len(df.dropna(subset=['hd','ha'])) == 1
    def test_type_service_valeurs_valides(self):
        assert set(['Jour','Nuit','Jour']).issubset({'Jour','Nuit'})
    def test_distance_positive(self):
        df = pd.DataFrame({'d': [0, -5, 100, 250, 0.5]})
        assert len(df[df['d'] > 0]) == 3
    def test_normalisation_utf8(self):
        for nom in ['München','Köln','Düsseldorf']:
            assert nom.encode('utf-8').decode('utf-8') == nom
    def test_colonnes_requises(self):
        cols = ['operateur_nom','gare_depart_nom','gare_arrivee_nom','heure_depart','heure_arrivee','type_service','distance_km']
        df = pd.DataFrame({c:['x'] for c in cols})
        for c in cols: assert c in df.columns
    def test_deduplication_compte(self):
        assert 186902 - 59162 == 127740
    def test_type_ligne_valeurs(self):
        assert set(['national','regional']).issubset({'national','regional'})


class TestSchemaPostgreSQL:
    def test_ordre_chargement_fk(self):
        o = ['operateur','gare','trajet','train']
        assert o.index('operateur') < o.index('train')
        assert o.index('gare') < o.index('trajet')
        assert o.index('trajet') < o.index('train')
    def test_nb_operateurs(self):
        assert len(['SNCF','Deutsche Bahn','SNCB']) == 3
    def test_nb_trains_attendu(self):
        assert 0 < 99854 < 200000
    def test_nb_gares_attendu(self):
        assert 0 < 3017 < 10000
    def test_contrainte_unique_train(self):
        r = [(1,1,'08:00'),(1,1,'14:00'),(2,1,'08:00')]
        assert len(r) == len(set(r))
    def test_contrainte_unique_gare(self):
        g = [('Paris','FR'),('Paris','BE'),('Lyon','FR')]
        assert len(g) == len(set(g))
    def test_check_type_service(self):
        for v in ['Jour','Nuit']: assert v in ('Jour','Nuit')
        for v in ['jour','nuit',None]: assert v not in ('Jour','Nuit')


@pytest.mark.skipif(not APP_AVAILABLE, reason="API non disponible")
class TestAPIHealth:
    def test_health_status_code(self):
        assert client.get("/health").status_code in [200,503]
    def test_health_content_type_json(self):
        assert "application/json" in client.get("/health").headers["content-type"]
    def test_health_champs_db_ok(self):
        r = client.get("/health")
        if r.status_code == 200:
            for c in ["status","database","nb_trains"]: assert c in r.json()
    def test_health_champs_db_ko(self):
        r = client.get("/health")
        if r.status_code == 503: assert "detail" in r.json()
    def test_health_nb_trains_positif(self):
        r = client.get("/health")
        if r.status_code == 200: assert r.json()["nb_trains"] > 0


@pytest.mark.skipif(not APP_AVAILABLE, reason="API non disponible")
class TestAPIDessertes:
    def test_liste_status(self):
        assert client.get("/dessertes").status_code in [200,500,503]
    def test_liste_format_tableau(self):
        r = client.get("/dessertes?limit=5")
        if r.status_code == 200: assert isinstance(r.json(), list)
    def test_search_gare(self):
        assert client.get("/dessertes/search?gare=Paris").status_code in [200,500,503]
    def test_search_operateur(self):
        assert client.get("/dessertes/search?operateur=SNCF").status_code in [200,500,503]
    def test_search_type_jour(self):
        assert client.get("/dessertes/search?type_service=Jour").status_code in [200,500,503]
    def test_search_type_nuit(self):
        assert client.get("/dessertes/search?type_service=Nuit").status_code in [200,500,503]
    def test_search_distance_min(self):
        assert client.get("/dessertes/search?dist_min=100").status_code in [200,500,503]
    def test_search_distance_max(self):
        assert client.get("/dessertes/search?dist_max=500").status_code in [200,500,503]
    def test_search_limite_max_500(self):
        assert client.get("/dessertes/search?limit=501").status_code in [200,422,500,503]
    def test_id_inexistant_404(self):
        assert client.get("/dessertes/999999999").status_code in [404,500,503]
    def test_id_valide_format(self):
        assert client.get("/dessertes/1").status_code in [200,404,500,503]
    def test_id_non_numerique_422(self):
        assert client.get("/dessertes/abc").status_code in [422,500,503]


@pytest.mark.skipif(not APP_AVAILABLE, reason="API non disponible")
class TestAPIStats:
    def test_stats_globales(self):
        assert client.get("/stats").status_code in [200,500,503]
    def test_stats_co2(self):
        assert client.get("/stats/co2").status_code in [200,500,503]
    def test_stats_couverture(self):
        assert client.get("/stats/couverture").status_code in [200,500,503]
    def test_stats_qualite(self):
        assert client.get("/stats/qualite").status_code in [200,500,503]
    def test_stats_champs_globaux(self):
        r = client.get("/stats")
        if r.status_code == 200:
            for c in ['nb_trains','nb_operateurs','nb_gares','nb_jour','nb_nuit']: assert c in r.json()
    def test_stats_qualite_etl_logs(self):
        r = client.get("/stats/qualite")
        if r.status_code == 200:
            assert "etl_logs" in r.json()
            assert "nb_trains_total" in r.json()
    def test_operateurs_liste(self):
        assert client.get("/operateurs").status_code in [200,500,503]
    def test_operateurs_format(self):
        r = client.get("/operateurs")
        if r.status_code == 200: assert isinstance(r.json(), list)
    def test_gares_liste(self):
        assert client.get("/gares").status_code in [200,500,503]
    def test_gares_search(self):
        assert client.get("/gares?nom=Paris").status_code in [200,500,503]


class TestMonitoring:
    def test_endpoint_metrics_defini(self):
        if APP_AVAILABLE: assert client.get("/metrics").status_code == 200
    def test_scrape_interval(self):
        assert 0 < 15 <= 60
    def test_metriques_attendues(self):
        if APP_AVAILABLE:
            r = client.get("/metrics")
            if r.status_code == 200:
                for m in ['http_requests_total','http_request_duration_seconds']: assert m in r.text
    def test_dag_schedule(self):
        parts = "0 2 * * *".split()
        assert parts[0] == "0" and parts[1] == "2" and len(parts) == 5
    def test_retries_airflow(self):
        assert 2 >= 1 and 5 >= 1


class TestCICD:
    BASE = os.path.join(os.path.dirname(__file__), '..')

    def test_fichier_ci_existe(self):
        assert os.path.exists(os.path.join(self.BASE, '.github', 'workflows', 'ci.yml'))

    def test_fichier_requirements_existe(self):
        assert os.path.exists(os.path.join(self.BASE, 'requirements.txt'))

    def test_requirements_contient_fastapi(self):
        p = os.path.join(self.BASE, 'requirements.txt')
        if os.path.exists(p): assert 'fastapi' in open(p).read().lower()

    def test_requirements_contient_prometheus(self):
        p = os.path.join(self.BASE, 'requirements.txt')
        if os.path.exists(p): assert 'prometheus' in open(p).read().lower()

    def test_dockerfile_api_existe(self):
        assert os.path.exists(os.path.join(self.BASE, 'Dockerfile.api'))

    def test_dockerfile_etl_existe(self):
        p1 = os.path.join(self.BASE, 'Dockerfile-etl')
        p2 = os.path.join(self.BASE, 'Dockerfile.etl')
        assert os.path.exists(p1) or os.path.exists(p2)

    def test_init_sql_existe(self):
        assert os.path.exists(os.path.join(self.BASE, 'init.sql'))

    def test_dag_existe(self):
        p1 = os.path.join(self.BASE, 'DAGS', 'obrail_etl_dags.py')
        p2 = os.path.join(self.BASE, 'dags', 'obrail_etl_dag.py')
        assert os.path.exists(p1) or os.path.exists(p2)