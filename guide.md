# ObRail Europe — Guide de démarrage complet

## Prérequis

- Docker Desktop installé et lancé
- Git
- 5 GB d'espace disque libre

---

## Structure du projet

```
obrail/
├── src/
│   ├── extract.py              # Téléchargement GTFS
│   ├── transform.py            # Nettoyage et transformation
│   ├── load.py                 # Chargement PostgreSQL normalisé
│   └── pipeline.py             # Orchestrateur ETL
├── api/
│   └── main.py                 # API REST FastAPI (10 endpoints + /metrics)
├── dashboard/
│   └── app.py                  # Dashboard Streamlit (6 pages)
├── dags/
│   └── obrail_etl_dag.py       # DAG Airflow — planification automatique
├── tests/
│   └── test_api.py             # 25 tests unitaires et d'intégration
├── monitoring/
│   ├── prometheus.yml          # Configuration Prometheus
│   └── grafana_dashboard.json  # Dashboard Grafana importable
├── .github/
│   └── workflows/
│       └── ci.yml              # Pipeline CI/CD GitHub Actions
├── data/
│   ├── raw/                    # Données brutes GTFS téléchargées
│   ├── transformed/            # CSV nettoyé
│   └── processed/              # Parquet fallback dashboard
├── docker-compose.yml          # Sans Airflow (manuel)
├── docker-compose-airflow.yml  # Avec Airflow (automatisé)
├── docker-compose-full.yml     # Complet : Airflow + Grafana + Prometheus
├── Dockerfile.etl
├── Dockerfile.api
├── Dockerfile.dashboard
├── requirements.txt
├── init.sql                    # Schéma PostgreSQL normalisé
└── .env                        # Variables d'environnement
```

---

## 1. Créer le fichier .env

```powershell
[System.IO.File]::WriteAllText("$PWD\.env",
"DB_HOST=db`nDB_PORT=5432`nDB_NAME=obrail_db`nDB_USER=postgres`nDB_PASSWORD=postgres`n",
[System.Text.Encoding]::UTF8)
```

---

## 2. VERSION MANUELLE — Sans Airflow

```powershell
# Démarrer les services
docker compose up -d --build

# Vérifier
docker compose ps

# Lancer le pipeline ETL (~52 secondes)
docker compose run etl

# Accès
# Dashboard  → http://localhost:8501
# API        → http://localhost:8000
# Swagger    → http://localhost:8000/docs
```

---

## 3. VERSION AUTOMATISÉE — Avec Airflow

Le pipeline se lance **automatiquement tous les jours à 2h du matin**.

```powershell
# Démarrer tous les services + Airflow
docker compose -f docker-compose-airflow.yml up -d

# Attendre 2-3 minutes puis vérifier
docker compose -f docker-compose-airflow.yml ps

# Accès
# Dashboard     → http://localhost:8501
# API           → http://localhost:8000
# Airflow UI    → http://localhost:8080  (admin / admin)
```

### Activer le DAG dans Airflow

1. Ouvrir http://localhost:8080
2. Login : **admin** / **admin**
3. Cliquer sur le toggle du DAG `obrail_etl_pipeline` pour l'activer
4. Le pipeline se lancera automatiquement à 2h00 chaque nuit
5. Pour lancer manuellement : cliquer sur **▶ Trigger DAG**

### Planification cron

| Expression | Signification |
|---|---|
| `0 2 * * *` | Tous les jours à 2h00 (notre config) |
| `0 * * * *` | Toutes les heures |
| `0 0 * * 1` | Tous les lundis à minuit |

---

## 4. VERSION COMPLÈTE — Avec Airflow + Grafana + Prometheus

```powershell
# Démarrer tous les services (11 conteneurs)
docker compose -f docker-compose-full.yml up -d

# Lancer l'ETL
docker compose -f docker-compose-full.yml run etl

# Accès
# Dashboard     → http://localhost:8501
# API           → http://localhost:8000
# Airflow UI    → http://localhost:8080  (admin / admin)
# Grafana       → http://localhost:3000  (admin / admin)
# Prometheus    → http://localhost:9090
```

### Configurer Grafana

1. Ouvrir http://localhost:3000 (admin / admin)
2. **Connections** → **Data sources** → **Add data source** → **Prometheus**
3. URL : `http://prometheus:9090` → **Save & test**
4. **Dashboards** → **New** → **Import**
5. Uploader `monitoring/grafana_dashboard.json`
6. Sélectionner **Prometheus** comme datasource → **Import**

Le dashboard affiche :
- 🟢 Statut API (UP/DOWN)
- 📊 Total requêtes
- ⚡ Requêtes/seconde
- ⏱ Latence moyenne (ms)
- 📈 Requêtes par endpoint
- 🟢 Disponibilité dans le temps
- 🔵 Répartition codes HTTP

---

## 5. Lancer les tests

```powershell
# Installer les dépendances de test
pip install pytest pytest-cov httpx fastapi

# Lancer les 25 tests
pytest tests/ -v

# Avec rapport de couverture
pytest tests/ -v --cov=api --cov-report=term-missing
```

Résultat attendu : **25/25 tests PASSED**

---

## 6. Accéder aux services

| Service | URL | Login |
|---|---|---|
| Dashboard Streamlit | http://localhost:8501 | — |
| API REST | http://localhost:8000 | — |
| Swagger / Documentation | http://localhost:8000/docs | — |
| Santé API | http://localhost:8000/health | — |
| Métriques Prometheus | http://localhost:8000/metrics | — |
| Airflow UI | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## 7. Exemples de requêtes API

```bash
# Santé et nombre de trains en base
curl http://localhost:8000/health

# Liste des opérateurs avec stats
curl http://localhost:8000/operateurs

# Recherche par gare
curl "http://localhost:8000/dessertes/search?gare=Paris&limit=10"

# Trains de nuit SNCF
curl "http://localhost:8000/dessertes/search?operateur=SNCF&type_service=Nuit"

# Trajets entre 200 et 800 km
curl "http://localhost:8000/dessertes/search?dist_min=200&dist_max=800"

# Statistiques globales
curl http://localhost:8000/stats

# CO2 par opérateur (base ADEME 2023)
curl http://localhost:8000/stats/co2

# Qualité des données + etl_logs
curl http://localhost:8000/stats/qualite

# Couverture Jour/Nuit par opérateur
curl http://localhost:8000/stats/couverture

# Métriques Prometheus
curl http://localhost:8000/metrics
```

---

## 8. Arrêter et relancer

```powershell
# Version manuelle
docker compose down
docker compose down -v                            # Reset complet avec données

# Version Airflow
docker compose -f docker-compose-airflow.yml down
docker compose -f docker-compose-airflow.yml down -v

# Version complète
docker compose -f docker-compose-full.yml down
docker compose -f docker-compose-full.yml down -v
```

---

## 9. Résolution des problèmes

| Problème | Solution |
|---|---|
| Dashboard "Aucune donnée" | `docker compose run etl` |
| API retourne 503 | `docker compose logs api --tail=30` |
| Airflow ne démarre pas | `docker compose -f docker-compose-airflow.yml logs airflow_init --tail=50` |
| Base vide après ETL | `docker exec -it obrail_db psql -U postgres -d obrail_db -c 'SELECT COUNT(*) FROM train'` |
| Grafana vide | Vérifier que Prometheus est connecté comme datasource |
| Prometheus targets DOWN | Rebuilder l'API : `docker compose build --no-cache api` |
| Rebuilder une image | `docker compose build --no-cache api` |

---

## 10. Sources de données

| Source | Format | Pays | URL |
|---|---|---|---|
| SNCF TER | GTFS | France | eu.ftp.opendatasoft.com/sncf/gtfs/ |
| SNCF Intercités | GTFS | France | eu.ftp.opendatasoft.com/sncf/gtfs/ |
| Deutsche Bahn (FV) | GTFS | Allemagne | download.gtfs.de/germany/fv_free/ |
| DB Régional (RV) | GTFS | Allemagne | download.gtfs.de/germany/rv_free/ |
| SNCB iRail | GTFS | Belgique | gtfs.irail.be/nmbs/gtfs/ |

Licence : Open Data Commons ODbL — données librement réutilisables.

---

## 11. Conformité RGPD

- Aucune donnée personnelle traitée
- Sources open data publiques (ODbL)
- Table `etl_logs` : traçabilité de chaque exécution
- Credentials dans `.env`, jamais sur Git
- API lecture seule (GET uniquement)

---

## 12. CI/CD GitHub Actions

Le pipeline CI/CD se déclenche automatiquement à chaque push sur GitHub.

3 jobs dans l'ordre :
1. **tests** — lance les 25 tests pytest
2. **build** — construit les images Docker
3. **integration** — vérifie que l'API répond

Voir `.github/workflows/ci.yml` pour la configuration complète.