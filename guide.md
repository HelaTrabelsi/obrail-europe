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
│   ├── extract.py        # Téléchargement GTFS
│   ├── transform.py      # Nettoyage et transformation
│   ├── load.py           # Chargement PostgreSQL normalisé
│   └── pipeline.py       # Orchestrateur ETL
├── api/
│   └── main.py           # API REST FastAPI (10 endpoints)
├── dashboard/
│   └── app.py            # Dashboard Streamlit (6 pages)
├── dags/
│   └── obrail_etl_dag.py # DAG Airflow — planification automatique
├── data/
│   ├── raw/              # Données brutes GTFS téléchargées
│   ├── transformed/      # CSV nettoyé
│   └── processed/        # Parquet fallback dashboard
├── docker-compose.yml            # Sans Airflow (manuel)
├── docker-compose-airflow.yml    # Avec Airflow (automatisé)
├── Dockerfile.etl
├── Dockerfile.api
├── Dockerfile.dashboard
├── requirements.txt
├── init.sql              # Schéma PostgreSQL normalisé
└── .env                  # Variables d'environnement
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

Lance le pipeline manuellement quand tu veux.

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

### Lancer le pipeline manuellement depuis Airflow

Dans l'interface Airflow, cliquer sur **▶ Trigger DAG**.

### Planification

```python
schedule_interval="0 2 * * *"   # Tous les jours à 2h du matin
```

| Expression cron | Signification |
|---|---|
| `0 2 * * *` | Tous les jours à 2h00 |
| `0 * * * *` | Toutes les heures |
| `0 0 * * 1` | Tous les lundis à minuit |

---

## 4. Accéder aux services

| Service | URL |
|---|---|
| Dashboard Streamlit | http://localhost:8501 |
| API REST | http://localhost:8000 |
| Swagger / Documentation | http://localhost:8000/docs |
| Santé API | http://localhost:8000/health |
| Airflow UI | http://localhost:8080 |

---

## 5. Exemples de requêtes API

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
```

---

## 6. Arrêter et relancer

```powershell
# Version manuelle
docker compose down
docker compose down -v         # Reset complet avec données

# Version Airflow
docker compose -f docker-compose-airflow.yml down
docker compose -f docker-compose-airflow.yml down -v   # Reset complet
```

---

## 7. Résolution des problèmes

| Problème | Solution |
|---|---|
| Dashboard "Aucune donnée" | `docker compose run etl` |
| API retourne 503 | `docker compose logs api --tail=30` |
| Airflow ne démarre pas | `docker compose -f docker-compose-airflow.yml logs airflow_init --tail=50` |
| Base vide après ETL | `docker exec -it obrail_db psql -U postgres -d obrail_db -c 'SELECT COUNT(*) FROM train'` |
| Rebuilder une image | `docker compose build --no-cache api` |

---

## 8. Sources de données

| Source | Format | Pays | URL |
|---|---|---|---|
| SNCF TER | GTFS | France | eu.ftp.opendatasoft.com/sncf/gtfs/ |
| SNCF Intercités | GTFS | France | eu.ftp.opendatasoft.com/sncf/gtfs/ |
| Deutsche Bahn (FV) | GTFS | Allemagne | download.gtfs.de/germany/fv_free/ |
| DB Régional (RV) | GTFS | Allemagne | download.gtfs.de/germany/rv_free/ |
| SNCB iRail | GTFS | Belgique | gtfs.irail.be/nmbs/gtfs/ |

Licence : Open Data Commons ODbL — données librement réutilisables.

---

## 9. Conformité RGPD

- Aucune donnée personnelle traitée
- Sources open data publiques (ODbL)
- Table `etl_logs` : traçabilité de chaque exécution
- Credentials dans `.env`, jamais sur Git
- API lecture seule (GET uniquement)