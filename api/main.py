from fastapi import FastAPI, Query, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker
from typing import Optional, List
from pydantic import BaseModel
from datetime import time
import os
from dotenv import load_dotenv

load_dotenv()

# App 
app = FastAPI(
    title="ObRail Europe API",
    description="""
##  API REST  Observatoire Ferroviaire Européen

Cette API expose les données ferroviaires européennes collectées et harmonisées par ObRail Europe.

### Fonctionnalités
- Recherche de dessertes par ville, opérateur, type de service
- Statistiques d'émissions CO₂
- Analyse de la couverture ferroviaire
- Données de qualité et traçabilité

### Sources
Données issues de flux GTFS (SNCF, Deutsche Bahn, SNCB) et d'APIs open data.
""",
    version="1.0.0",
    contact={"name": "ObRail Europe", "email": "data@obrail.eu"},
    license_info={"name": "Open Data Commons ODbL"},
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# DB 
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:"
    f"{os.getenv('DB_PASSWORD', 'postgres')}@"
    f"{os.getenv('DB_HOST', 'db')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'obrail_db')}"
)
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Schemas 
class Desserte(BaseModel):
    id: int
    operateur_nom: Optional[str]
    nom_ligne: Optional[str]
    type_ligne: Optional[str]
    type_service: Optional[str]
    gare_depart_nom: Optional[str]
    gare_arrivee_nom: Optional[str]
    heure_depart: Optional[str]
    heure_arrivee: Optional[str]
    distance_km: Optional[float]
    emissions_co2_gkm: Optional[float]
    source_donnee: Optional[str]

    class Config:
        from_attributes = True


class StatsGlobal(BaseModel):
    total_dessertes: int
    total_operateurs: int
    distance_moyenne_km: float
    co2_moyen_gkm: float
    nb_trains_jour: int
    nb_trains_nuit: int


# Endpoints 

@app.get("/", tags=["Info"])
def root():
    return {
        "service": "ObRail Europe API",
        "version": "1.0.0",
        "description": "Données ferroviaires européennes harmonisées",
        "endpoints": {
            "dessertes": "/dessertes",
            "search": "/dessertes/search",
            "stats": "/stats",
            "operateurs": "/operateurs",
            "health": "/health",
            "docs": "/docs",
        }
    }


@app.get("/health", tags=["Info"])
def health(db: Session = Depends(get_db)):
    try:
        db.execute(text("SELECT 1"))
        count = db.execute(text("SELECT COUNT(*) FROM dessertes")).scalar()
        return {"status": "ok", "db": "connected", "dessertes_count": count}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB error: {e}")


@app.get("/dessertes", response_model=List[Desserte], tags=["Dessertes"])
def list_dessertes(
        skip: int = Query(0, ge=0, description="Offset pagination"),
        limit: int = Query(50, ge=1, le=500, description="Nombre de résultats (max 500)"),
        db: Session = Depends(get_db)
):
    """Retourne la liste paginée de toutes les dessertes."""
    rows = db.execute(
        text("SELECT * FROM dessertes ORDER BY id LIMIT :limit OFFSET :skip"),
        {"limit": limit, "skip": skip}
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/dessertes/search", response_model=List[Desserte], tags=["Dessertes"])
def search_dessertes(
        depart: Optional[str] = Query(None, description="Ville / gare de départ (partiel accepté)"),
        arrivee: Optional[str] = Query(None, description="Ville / gare d'arrivée (partiel accepté)"),
        operateur: Optional[str] = Query(None, description="Nom de l'opérateur (ex: SNCF, Deutsche Bahn)"),
        type_service: Optional[str] = Query(None, description="Jour ou Nuit"),
        type_ligne: Optional[str] = Query(None, description="national ou regional"),
        distance_min: Optional[float] = Query(None, description="Distance minimale en km"),
        distance_max: Optional[float] = Query(None, description="Distance maximale en km"),
        heure_depart_apres: Optional[str] = Query(None, description="Heure départ après (HH:MM)"),
        heure_depart_avant: Optional[str] = Query(None, description="Heure départ avant (HH:MM)"),
        limit: int = Query(100, ge=1, le=500),
        db: Session = Depends(get_db)
):
    """
    Recherche multicritères des dessertes ferroviaires.

    Exemples :
    - `/dessertes/search?depart=Paris&arrivee=Lyon`
    - `/dessertes/search?operateur=SNCF&type_service=Nuit`
    - `/dessertes/search?distance_min=200&distance_max=800`
    """
    conditions = ["1=1"]
    params = {"limit": limit}

    if depart:
        conditions.append("LOWER(gare_depart_nom) LIKE LOWER(:depart)")
        params["depart"] = f"%{depart}%"
    if arrivee:
        conditions.append("LOWER(gare_arrivee_nom) LIKE LOWER(:arrivee)")
        params["arrivee"] = f"%{arrivee}%"
    if operateur:
        conditions.append("LOWER(operateur_nom) LIKE LOWER(:operateur)")
        params["operateur"] = f"%{operateur}%"
    if type_service:
        conditions.append("type_service = :type_service")
        params["type_service"] = type_service
    if type_ligne:
        conditions.append("type_ligne = :type_ligne")
        params["type_ligne"] = type_ligne
    if distance_min is not None:
        conditions.append("distance_km >= :distance_min")
        params["distance_min"] = distance_min
    if distance_max is not None:
        conditions.append("distance_km <= :distance_max")
        params["distance_max"] = distance_max
    if heure_depart_apres:
        conditions.append("heure_depart >= :hd_apres::time")
        params["hd_apres"] = heure_depart_apres
    if heure_depart_avant:
        conditions.append("heure_depart <= :hd_avant::time")
        params["hd_avant"] = heure_depart_avant

    query = f"SELECT * FROM dessertes WHERE {' AND '.join(conditions)} ORDER BY gare_depart_nom LIMIT :limit"
    rows = db.execute(text(query), params).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/dessertes/{desserte_id}", response_model=Desserte, tags=["Dessertes"])
def get_desserte(desserte_id: int, db: Session = Depends(get_db)):
    """Retourne une desserte par son identifiant."""
    row = db.execute(
        text("SELECT * FROM dessertes WHERE id = :id"), {"id": desserte_id}
    ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Desserte introuvable")
    return dict(row._mapping)


@app.get("/operateurs", tags=["Référentiels"])
def list_operateurs(db: Session = Depends(get_db)):
    """Retourne la liste des opérateurs disponibles avec leur nombre de dessertes."""
    rows = db.execute(text("""
        SELECT operateur_nom,
               COUNT(*) as nb_dessertes,
               ROUND(AVG(distance_km)::numeric, 1) as distance_moyenne_km,
               COUNT(*) FILTER (WHERE type_service = 'Nuit') as nb_nuit,
               COUNT(*) FILTER (WHERE type_service = 'Jour') as nb_jour
        FROM dessertes
        GROUP BY operateur_nom
        ORDER BY nb_dessertes DESC
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/gares", tags=["Référentiels"])
def list_gares(
        q: Optional[str] = Query(None, description="Recherche par nom (partiel)"),
        limit: int = Query(20, ge=1, le=100),
        db: Session = Depends(get_db)
):
    """Retourne la liste des gares avec leur fréquence."""
    if q:
        rows = db.execute(text("""
            SELECT stop_name, nb FROM (
                SELECT gare_depart_nom AS stop_name, COUNT(*) AS nb FROM dessertes
                WHERE LOWER(gare_depart_nom) LIKE LOWER(:q)
                GROUP BY gare_depart_nom
            ) t ORDER BY nb DESC LIMIT :limit
        """), {"q": f"%{q}%", "limit": limit}).fetchall()
    else:
        rows = db.execute(text("""
            SELECT gare_depart_nom AS stop_name, COUNT(*) AS nb
            FROM dessertes
            GROUP BY gare_depart_nom
            ORDER BY nb DESC
            LIMIT :limit
        """), {"limit": limit}).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/stats", response_model=StatsGlobal, tags=["Statistiques"])
def global_stats(db: Session = Depends(get_db)):
    """Statistiques globales de l'entrepôt de données."""
    row = db.execute(text("""
        SELECT
            COUNT(*)                                            AS total_dessertes,
            COUNT(DISTINCT operateur_nom)                      AS total_operateurs,
            ROUND(AVG(distance_km)::numeric, 2)                AS distance_moyenne_km,
            ROUND(AVG(emissions_co2_gkm)::numeric, 2)          AS co2_moyen_gkm,
            COUNT(*) FILTER (WHERE type_service = 'Jour')      AS nb_trains_jour,
            COUNT(*) FILTER (WHERE type_service = 'Nuit')      AS nb_trains_nuit
        FROM dessertes
    """)).fetchone()
    return dict(row._mapping)


@app.get("/stats/co2", tags=["Statistiques"])
def co2_stats(db: Session = Depends(get_db)):
    """Statistiques détaillées sur les émissions CO₂ par opérateur."""
    rows = db.execute(text("""
        SELECT
            operateur_nom,
            ROUND(AVG(emissions_co2_gkm)::numeric, 2)          AS co2_moyen_gkm,
            ROUND(MIN(emissions_co2_gkm)::numeric, 2)          AS co2_min_gkm,
            ROUND(MAX(emissions_co2_gkm)::numeric, 2)          AS co2_max_gkm,
            ROUND(SUM(distance_km * emissions_co2_gkm / 1000)::numeric, 0) AS co2_total_kg
        FROM dessertes
        GROUP BY operateur_nom
        ORDER BY co2_moyen_gkm ASC
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/stats/couverture", tags=["Statistiques"])
def couverture_stats(db: Session = Depends(get_db)):
    """Analyse de la couverture ferroviaire : répartition jour/nuit, distances, fréquences."""
    rows = db.execute(text("""
        SELECT
            type_service,
            type_ligne,
            COUNT(*)                                AS nb_dessertes,
            ROUND(AVG(distance_km)::numeric, 1)     AS dist_moy_km,
            ROUND(MIN(distance_km)::numeric, 1)     AS dist_min_km,
            ROUND(MAX(distance_km)::numeric, 1)     AS dist_max_km
        FROM dessertes
        GROUP BY type_service, type_ligne
        ORDER BY type_service, type_ligne
    """)).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/stats/qualite", tags=["Qualité"])
def qualite_stats(db: Session = Depends(get_db)):
    """Indicateurs de qualité des données : complétude par champ, par source."""
    total = db.execute(text("SELECT COUNT(*) FROM dessertes")).scalar()
    cols = ['operateur_nom', 'nom_ligne', 'type_service', 'gare_depart_nom',
            'gare_arrivee_nom', 'heure_depart', 'heure_arrivee', 'distance_km',
            'emissions_co2_gkm', 'source_donnee']
    completude = {}
    for col in cols:
        non_null = db.execute(
            text(f"SELECT COUNT(*) FROM dessertes WHERE {col} IS NOT NULL")
        ).scalar()
        completude[col] = round((non_null / total) * 100, 1) if total else 0

    sources = db.execute(text("""
        SELECT source_donnee, COUNT(*) as nb FROM dessertes GROUP BY source_donnee ORDER BY nb DESC
    """)).fetchall()

    return {
        "total_enregistrements": total,
        "completude_par_champ": completude,
        "repartition_par_source": [dict(r._mapping) for r in sources]
    }