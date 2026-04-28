# ObRail Europe — Guide de démarrage complet

## Prérequis

- Docker Desktop installé et lancé
- Git
- 5 GB d'espace disque libre (données volumineuses)

---

## Structure du projet

```
obrail/
├── src/
│   ├── extract.py       # Téléchargement
│   ├── transform.py     # Nettoyage et transformation
│   ├── load.py          # Chargement PostgreSQL
│   └── pipeline.py      # Orchestrateur ETL
├── api/
│   └── main.py          # API REST FastAPI
├── dashboard/
│   └── app.py           # Dashboard Streamlit
├── data/
│   ├── raw/             # Données brutes (GTFS téléchargés)
│   ├── transformed/     # CSV nettoyé + stats.json
│   └── processed/       # Parquet pour le dashboard
├── docker-compose.yml
├── Dockerfile.etl
├── Dockerfile.api
├── Dockerfile.dashboard
├── requirements.txt     # Requirements
├── init.sql             # Schéma PostgreSQL
└── .env                 # Variables d'environnement
```

---

## 1. Créer le fichier .env

Dans le dossier du projet, créer un fichier `.env` :

**Windows PowerShell :**
```powershell
[System.IO.File]::WriteAllText("$PWD\.env",
"DB_HOST=db`nDB_PORT=5432`nDB_NAME=obrail_db`nDB_USER=postgres`nDB_PASSWORD=postgres`n",
[System.Text.Encoding]::UTF8)
```

**Mac / Linux :**
```bash
cat > .env << EOF
DB_HOST=db
DB_PORT=5432
DB_NAME=obrail_db
DB_USER=""
DB_PASSWORD=""
EOF
```

---

## 2. Lancer la stack Docker

```bash
# Construire et démarrer tous les services
docker compose up -d --build
```

Cela démarre 4 conteneurs :
| Conteneur       | Rôle                        | Port  |
|-----------------|-----------------------------|-------|
| obrail_db       | Base de données PostgreSQL  | 5432  |
| obrail_etl      | Pipeline ETL (one-shot)     | —     |
| obrail_api      | API REST FastAPI            | 8000  |
| obrail_dashboard| Dashboard Streamlit         | 8501  |

Vérifier que tout tourne :
```bash
docker compose ps
```

---

## 3. Lancer le pipeline ETL

**Pipeline complet** (Extract + Transform + Load) :
```bash
docker compose run etl
```

Durée estimée : **10 à 20 minutes** selon la connexion internet
(Deutsche Bahn Regional = fichier volumineux ~800 MB)

**Étapes individuelles si besoin :**
```bash
# Extraction uniquement (téléchargement GTFS)
docker compose run etl python src/pipeline.py --step extract

# Transformation uniquement (si données déjà téléchargées)
docker compose run etl python src/pipeline.py --step transform

# Chargement uniquement (si dessertes.csv déjà généré)
docker compose run etl python src/pipeline.py --step load
```

---

## 4. Accéder aux services

| Service       | URL                              |
|---------------|----------------------------------|
| Dashboard     | http://localhost:8501            |
| API REST      | http://localhost:8000            |
| Documentation | http://localhost:8000/docs       |
| Santé API     | http://localhost:8000/health     |

---

## 5. Exemples de requêtes API

```bash
# Santé et nombre de dessertes en base
curl http://localhost:8000/health

# Liste des opérateurs
curl http://localhost:8000/operateurs

# Recherche Paris → Lyon
curl "http://localhost:8000/dessertes/search?depart=Paris&arrivee=Lyon"

# Trains de nuit SNCF
curl "http://localhost:8000/dessertes/search?operateur=SNCF&type_service=Nuit"

# Trajets entre 200 et 800 km
curl "http://localhost:8000/dessertes/search?distance_min=200&distance_max=800"

# Statistiques globales
curl http://localhost:8000/stats

# CO2 par opérateur
curl http://localhost:8000/stats/co2

# Qualité des données
curl http://localhost:8000/stats/qualite

# Couverture jour/nuit
curl http://localhost:8000/stats/couverture
```

---

## 6. Arrêter et relancer

```bash
# Arrêter tous les conteneurs
docker compose down

# Arrêter et supprimer les données PostgreSQL (reset complet)
docker compose down -v

# Relancer sans rebuild
docker compose up -d

# Relancer avec rebuild (après modification du code)
docker compose up -d --build
```

---

## 7. Mettre à jour les données

Pour télécharger les dernières versions des GTFS :
```bash
# Relancer l'ETL complet
docker compose run etl
```

Les données sont automatiquement écrasées et rechargées.

---

## 8. Résolution des problèmes courants

**Le dashboard affiche "Aucune donnée disponible"**
```bash
# Vérifier que l'ETL a bien tourné
docker compose run etl python src/pipeline.py --step load
```

**Erreur de connexion à la base**
```bash
# Vérifier que PostgreSQL est healthy
docker compose ps
# Attendre que db soit "healthy" puis relancer
```

**Le fichier .env est mal encodé (Windows)**
```powershell
[System.IO.File]::WriteAllText("$PWD\.env",
"DB_HOST=db`nDB_PORT=5432`nDB_NAME=obrail_db`nDB_USER=postgres`nDB_PASSWORD=postgres`n",
[System.Text.Encoding]::UTF8)
```

**Rebuilder une image spécifique**
```bash
docker compose build --no-cache etl
docker compose build --no-cache dashboard
docker compose build --no-cache api
```

---

## 9. Sources de données intégrées

| Source            | Format | Pays       | URL                                              |
|-------------------|--------|------------|--------------------------------------------------|
| SNCF TER          | GTFS   | France     | eu.ftp.opendatasoft.com/sncf/gtfs/               |
| SNCF Intercités   | GTFS   | France     | eu.ftp.opendatasoft.com/sncf/gtfs/               |
| Deutsche Bahn     | GTFS   | Allemagne  | download.gtfs.de/germany/fv_free/                |
| DB Régional       | GTFS   | Allemagne  | download.gtfs.de/germany/rv_free/                |
| SNCB              | GTFS   | Belgique   | gtfs.irail.be/nmbs/gtfs/                         |

Licences : Open Data Commons ODbL — données librement réutilisables.

---

## 10. Conformité RGPD

Ce projet ne traite **aucune donnée personnelle**.
- Toutes les sources sont des données open data publiques
- La table `etl_logs` assure la traçabilité des chargements
- Les sources sont documentées et auditables
- Licence ODbL respectée