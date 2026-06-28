from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import Optional
from prometheus_fastapi_instrumentator import Instrumentator
import os
import joblib
from pydantic import BaseModel

app = FastAPI(
    title="ObRail Europe API",
    description="API REST — Donnees ferroviaires europeennes (SNCF, Deutsche Bahn, SNCB)",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

# Monitoring Prometheus 
Instrumentator().instrument(app).expose(app)

DB_URL = (
    f"postgresql://{os.getenv('DB_USER','postgres')}:{os.getenv('DB_PASSWORD','1234')}"
    f"@{os.getenv('DB_HOST','db')}:{os.getenv('DB_PORT','5432')}/{os.getenv('DB_NAME','obrail_db')}"
)

def get_engine():
    return create_engine(DB_URL)

def db_ok():
    try:
        with get_engine().connect() as c:
            c.execute(text("SELECT 1"))
        return True
    except Exception:
        return False

# Jointure de base
BASE_QUERY = """
    SELECT
        t.id_train,
        o.nom            AS operateur,
        g.nom            AS gare,
        g.pays           AS pays,
        t.type_service,
        t.type_ligne,
        CAST(t.heure_depart  AS TEXT) AS heure_depart,
        CAST(t.heure_arrivee AS TEXT) AS heure_arrivee,
        tr.distance      AS distance_km,
        t.emission_co2_gkm,
        t.source_donnee,
        CAST(t.created_at AS TEXT) AS created_at
    FROM train t
    JOIN operateur o  ON o.id_operateur = t.id_operateur
    JOIN trajet    tr ON tr.id_trajet   = t.id_trajet
    JOIN gare      g  ON g.id_gare      = tr.id_gare
"""

# /health 
@app.get("/health", tags=["Sante"])
def health():
    if not db_ok():
        raise HTTPException(status_code=503, detail="Base de donnees inaccessible")
    try:
        with get_engine().connect() as c:
            nb = c.execute(text("SELECT COUNT(*) FROM train")).scalar()
        return {"status": "ok", "database": "connected", "nb_trains": nb}
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))

# /dessertes 
@app.get("/dessertes", tags=["Dessertes"])
def get_dessertes(skip: int = 0, limit: int = Query(default=100, le=500)):
    try:
        with get_engine().connect() as c:
            rows = c.execute(text(f"{BASE_QUERY} ORDER BY t.id_train LIMIT :limit OFFSET :skip"),
                             {"limit": limit, "skip": skip}).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#  /dessertes/search
@app.get("/dessertes/search", tags=["Dessertes"])
def search_dessertes(
    gare:         Optional[str] = None,
    operateur:    Optional[str] = None,
    type_service: Optional[str] = None,
    type_ligne:   Optional[str] = None,
    dist_min:     Optional[float] = None,
    dist_max:     Optional[float] = None,
    limit:        int = Query(default=100, le=500)
):
    conditions = []
    params = {"limit": limit}
    if gare:         conditions.append("g.nom ILIKE :gare");         params["gare"] = f"%{gare}%"
    if operateur:    conditions.append("o.nom ILIKE :operateur");    params["operateur"] = f"%{operateur}%"
    if type_service: conditions.append("t.type_service = :ts");      params["ts"] = type_service
    if type_ligne:   conditions.append("t.type_ligne = :tl");        params["tl"] = type_ligne
    if dist_min:     conditions.append("tr.distance >= :dmin");      params["dmin"] = dist_min
    if dist_max:     conditions.append("tr.distance <= :dmax");      params["dmax"] = dist_max
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        with get_engine().connect() as c:
            rows = c.execute(text(f"{BASE_QUERY} {where} ORDER BY t.id_train LIMIT :limit"), params).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

#  /dessertes/{id} 
@app.get("/dessertes/{id_train}", tags=["Dessertes"])
def get_desserte(id_train: int):
    try:
        with get_engine().connect() as c:
            row = c.execute(text(f"{BASE_QUERY} WHERE t.id_train = :id"), {"id": id_train}).mappings().first()
        if not row: raise HTTPException(status_code=404, detail="Train non trouve")
        return dict(row)
    except HTTPException: raise
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

#  /operateurs 
@app.get("/operateurs", tags=["Referentiels"])
def get_operateurs():
    try:
        with get_engine().connect() as c:
            rows = c.execute(text("""
                SELECT o.id_operateur, o.nom, o.pays,
                       COUNT(t.id_train)                                   AS nb_trains,
                       COUNT(*) FILTER (WHERE t.type_service = 'Jour')    AS nb_jour,
                       COUNT(*) FILTER (WHERE t.type_service = 'Nuit')    AS nb_nuit
                FROM operateur o
                LEFT JOIN train t ON t.id_operateur = o.id_operateur
                GROUP BY o.id_operateur, o.nom, o.pays
                ORDER BY nb_trains DESC
            """)).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

#  /gares 
@app.get("/gares", tags=["Referentiels"])
def get_gares(nom: Optional[str] = None, limit: int = 100):
    try:
        q = "SELECT id_gare, nom, pays FROM gare"
        params = {"limit": limit}
        if nom:
            q += " WHERE nom ILIKE :nom"; params["nom"] = f"%{nom}%"
        q += " ORDER BY nom LIMIT :limit"
        with get_engine().connect() as c:
            rows = c.execute(text(q), params).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

#  /stats 
@app.get("/stats", tags=["Statistiques"])
def get_stats():
    try:
        with get_engine().connect() as c:
            nb_trains    = c.execute(text("SELECT COUNT(*) FROM train")).scalar()
            nb_operateurs= c.execute(text("SELECT COUNT(*) FROM operateur")).scalar()
            nb_gares     = c.execute(text("SELECT COUNT(*) FROM gare")).scalar()
            nb_trajets   = c.execute(text("SELECT COUNT(*) FROM trajet")).scalar()
            nb_jour      = c.execute(text("SELECT COUNT(*) FROM train WHERE type_service='Jour'")).scalar()
            nb_nuit      = c.execute(text("SELECT COUNT(*) FROM train WHERE type_service='Nuit'")).scalar()
            dist_moy     = c.execute(text("SELECT ROUND(AVG(distance)::NUMERIC,1) FROM trajet")).scalar()
        return {
            "nb_trains": nb_trains, "nb_operateurs": nb_operateurs,
            "nb_gares": nb_gares, "nb_trajets": nb_trajets,
            "nb_jour": nb_jour, "nb_nuit": nb_nuit,
            "distance_moyenne_km": float(dist_moy or 0)
        }
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

#  /stats/co2 
@app.get("/stats/co2", tags=["Statistiques"])
def get_stats_co2():
    try:
        with get_engine().connect() as c:
            rows = c.execute(text("""
                SELECT o.nom AS operateur,
                       ROUND(AVG(t.emission_co2_gkm)::NUMERIC,2) AS co2_moy_gkm,
                       ROUND(SUM(tr.distance * t.emission_co2_gkm / 1000)::NUMERIC,2) AS co2_total_kg
                FROM train t
                JOIN operateur o  ON o.id_operateur = t.id_operateur
                JOIN trajet    tr ON tr.id_trajet   = t.id_trajet
                WHERE t.emission_co2_gkm IS NOT NULL
                GROUP BY o.nom ORDER BY co2_total_kg DESC
            """)).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

# /stats/couverture 
@app.get("/stats/couverture", tags=["Statistiques"])
def get_stats_couverture():
    try:
        with get_engine().connect() as c:
            rows = c.execute(text("""
                SELECT o.nom AS operateur, t.type_service,
                       COUNT(*)                          AS nb,
                       ROUND(MIN(tr.distance)::NUMERIC,1)  AS dist_min,
                       ROUND(AVG(tr.distance)::NUMERIC,1)  AS dist_moy,
                       ROUND(MAX(tr.distance)::NUMERIC,1)  AS dist_max
                FROM train t
                JOIN operateur o  ON o.id_operateur = t.id_operateur
                JOIN trajet    tr ON tr.id_trajet   = t.id_trajet
                GROUP BY o.nom, t.type_service
                ORDER BY o.nom, t.type_service
            """)).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

#  /stats/qualite 
@app.get("/stats/qualite", tags=["Statistiques"])
def get_stats_qualite():
    try:
        with get_engine().connect() as c:
            nb    = c.execute(text("SELECT COUNT(*) FROM train")).scalar()
            logs  = c.execute(text("""
                SELECT etape, source, nb_enregistrements, statut,
                       CAST(run_date AS TEXT) AS run_date, message
                FROM etl_logs ORDER BY run_date DESC LIMIT 10
            """)).mappings().all()
            src   = c.execute(text("""
                SELECT source_donnee, COUNT(*) AS nb
                FROM train WHERE source_donnee IS NOT NULL
                GROUP BY source_donnee ORDER BY nb DESC
            """)).mappings().all()
            co2_null = c.execute(text(
                "SELECT COUNT(*) FROM train WHERE emission_co2_gkm IS NULL")).scalar()
        return {
            "nb_trains_total": nb,
            "co2_manquants": co2_null,
            "completude_co2_pct": round((1 - co2_null/nb)*100, 1) if nb else 0,
            "etl_logs": [dict(r) for r in logs],
            "par_source": [dict(r) for r in src]
        }
    except Exception as e: raise HTTPException(status_code=500, detail=str(e))

    
class PredictRequest(BaseModel):
    distance_km: float
    operateur: str          # "SNCF" | "Deutsche Bahn" | "SNCB"
    type_service: str       # "Jour" | "Nuit"
    type_ligne: str         # "national" | "regional"
    heure_depart: str       # "HH:MM:SS"
 
class PredictResponse(BaseModel):
    co2_prediction_kg: float
    type_service_prediction: str
    co2_avion_kg: float
    economie_pct: float
    ratio_avion_train: float
    modele_co2: str
    modele_classification: str
 
# ── Chargement des modèles au démarrage ───────────────────
# Ajouter dans la section startup de main.py :
 
MODEL_CO2  = None
MODEL_NUIT = None
 
def load_models():
    global MODEL_CO2, MODEL_NUIT
    import os
    path_co2  = "notebooks/outputs_models/best_model_co2.joblib"
    path_nuit = "notebooks/outputs_models/best_model_nuit.joblib"
    if os.path.exists(path_co2):
        MODEL_CO2  = joblib.load(path_co2)
        MODEL_NUIT = joblib.load(path_nuit)
        print(f"✅ Modèles ML chargés : {type(MODEL_CO2).__name__} + {type(MODEL_NUIT).__name__}")
    else:
        print("⚠️ Modèles ML non trouvés — /predict indisponible")
 
# ── Endpoint /predict ─────────────────────────────────────
@app.post("/predict", response_model=PredictResponse, tags=["IA"])
def predict(req: PredictRequest):
    """
    Prédit les émissions CO2 et le type de service d'un trajet.
    
    - **distance_km** : distance du trajet en km
    - **operateur** : SNCF | Deutsche Bahn | SNCB
    - **type_service** : Jour | Nuit
    - **type_ligne** : national | regional
    - **heure_depart** : heure de départ HH:MM:SS
    """
    import math
 
    if MODEL_CO2 is None:
        raise HTTPException(status_code=503, detail="Modèles ML non chargés")
 
    # Encodage des features
    operateur_map = {"Deutsche Bahn": 0, "SNCB": 1, "SNCF": 2}
    operateur_enc = operateur_map.get(req.operateur, 0)
    type_service_enc = 1 if req.type_service == "Nuit" else 0
    type_ligne_enc   = 1 if req.type_ligne == "national" else 0
 
    try:
        heure = int(req.heure_depart.split(":")[0]) % 24
    except Exception:
        heure = 12
 
    heure_sin = math.sin(2 * math.pi * heure / 24)
    heure_cos = math.cos(2 * math.pi * heure / 24)
 
    # Distance bucket
    d = req.distance_km
    if d < 100:   bucket = 0
    elif d < 300: bucket = 1
    elif d < 600: bucket = 2
    else:         bucket = 3
 
    features_co2 = [[
        req.distance_km, operateur_enc, type_service_enc,
        type_ligne_enc, heure_sin, heure_cos, bucket
    ]]
 
    features_nuit = [[
        heure_sin, heure_cos, type_ligne_enc,
        operateur_enc, req.distance_km, bucket
    ]]
 
    co2_pred = float(MODEL_CO2.predict(features_co2)[0])
    nuit_pred = int(MODEL_NUIT.predict(features_nuit)[0])
    type_pred = "Nuit" if nuit_pred == 1 else "Jour"
 
    co2_avion = req.distance_km * 258 / 1000
    economie  = (co2_avion - co2_pred) / co2_avion * 100 if co2_avion > 0 else 0
    ratio     = co2_avion / co2_pred if co2_pred > 0 else 18.4
 
    return PredictResponse(
        co2_prediction_kg       = round(co2_pred, 4),
        type_service_prediction = type_pred,
        co2_avion_kg            = round(co2_avion, 3),
        economie_pct            = round(economie, 2),
        ratio_avion_train       = round(ratio, 2),
        modele_co2              = type(MODEL_CO2).__name__,
        modele_classification   = type(MODEL_NUIT).__name__,
    )