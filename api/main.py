from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, text
from typing import Optional
from prometheus_fastapi_instrumentator import Instrumentator
import os
import math
import joblib
import pandas as pd
from pydantic import BaseModel

app = FastAPI(
    title="ObRail Europe API",
    description="API REST ” Donnees ferroviaires europeennes (SNCF, Deutsche Bahn, SNCB)",
    version="2.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

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

BASE_QUERY = """
    SELECT
        t.id_train,
        o.nom            AS operateur,
        g.nom            AS gare,
        g.pays           AS pays,
        g2.nom           AS gare_arrivee,
        g2.pays          AS pays_arrivee,
        t.type_service,
        t.type_ligne,
        CAST(t.heure_depart  AS TEXT) AS heure_depart,
        CAST(t.heure_arrivee AS TEXT) AS heure_arrivee,
        tr.distance      AS distance_km,
        t.emission_co2_gkm,
        t.source_donnee,
        CAST(t.created_at AS TEXT) AS created_at
    FROM train t
    JOIN operateur o  ON o.id_operateur  = t.id_operateur
    JOIN trajet    tr ON tr.id_trajet    = t.id_trajet
    JOIN gare      g  ON g.id_gare       = tr.id_gare
    LEFT JOIN gare g2 ON g2.id_gare      = tr.id_gare_arrivee
"""

MODEL_CO2  = None
MODEL_NUIT = None
SCALER     = None

def load_models():
    global MODEL_CO2, MODEL_NUIT, SCALER
    path_co2    = "notebooks/outputs_models/best_model_co2.joblib"
    path_nuit   = "notebooks/outputs_models/best_model_nuit.joblib"
    path_scaler = "notebooks/ml_splits/scaler.joblib"
    if os.path.exists(path_co2):
        MODEL_CO2  = joblib.load(path_co2)
        MODEL_NUIT = joblib.load(path_nuit)
        print(f"Modeles ML charges : {type(MODEL_CO2).__name__} + {type(MODEL_NUIT).__name__}")
    else:
        print("Modeles ML non trouves")
    if os.path.exists(path_scaler):
        SCALER = joblib.load(path_scaler)
        print(f"Scaler charge : mean={SCALER.mean_[0]:.4f}")

@app.on_event("startup")
async def startup_event():
    load_models()


@app.get("/health", tags=["Sante"])
def health():
    if not db_ok():
        raise HTTPException(status_code=503, detail="Base de donnees inaccessible")
    try:
        with get_engine().connect() as c:
            nb = c.execute(text("SELECT COUNT(*) FROM train")).scalar()
        return {
            "status": "ok",
            "database": "connected",
            "nb_trains": nb,
            "modeles_ml": MODEL_CO2 is not None
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=str(e))


@app.get("/dessertes", tags=["Dessertes"])
def get_dessertes(skip: int = 0, limit: int = Query(default=100, le=500)):
    try:
        with get_engine().connect() as c:
            rows = c.execute(
                text(f"{BASE_QUERY} ORDER BY t.id_train LIMIT :limit OFFSET :skip"),
                {"limit": limit, "skip": skip}
            ).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dessertes/search", tags=["Dessertes"])
def search_dessertes(
    gare:         Optional[str]   = None,
    operateur:    Optional[str]   = None,
    type_service: Optional[str]   = None,
    type_ligne:   Optional[str]   = None,
    dist_min:     Optional[float] = None,
    dist_max:     Optional[float] = None,
    limit:        int = Query(default=100, le=500)
):
    conditions = []
    params: dict = {"limit": limit}
    if gare:         conditions.append("g.nom ILIKE :gare");         params["gare"] = f"%{gare}%"
    if operateur:    conditions.append("o.nom ILIKE :operateur");    params["operateur"] = f"%{operateur}%"
    if type_service: conditions.append("t.type_service = :ts");      params["ts"] = type_service
    if type_ligne:   conditions.append("t.type_ligne = :tl");        params["tl"] = type_ligne
    if dist_min:     conditions.append("tr.distance >= :dmin");      params["dmin"] = dist_min
    if dist_max:     conditions.append("tr.distance <= :dmax");      params["dmax"] = dist_max
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    try:
        with get_engine().connect() as c:
            rows = c.execute(
                text(f"{BASE_QUERY} {where} ORDER BY t.id_train LIMIT :limit"),
                params
            ).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/dessertes/{id_train}", tags=["Dessertes"])
def get_desserte(id_train: int):
    try:
        with get_engine().connect() as c:
            row = c.execute(
                text(f"{BASE_QUERY} WHERE t.id_train = :id"),
                {"id": id_train}
            ).mappings().first()
        if not row:
            raise HTTPException(status_code=404, detail="Train non trouve")
        return dict(row)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/operateurs", tags=["Referentiels"])
def get_operateurs():
    try:
        with get_engine().connect() as c:
            rows = c.execute(text("""
                SELECT o.id_operateur, o.nom, o.pays,
                       COUNT(t.id_train)                                AS nb_trains,
                       COUNT(*) FILTER (WHERE t.type_service = 'Jour') AS nb_jour,
                       COUNT(*) FILTER (WHERE t.type_service = 'Nuit') AS nb_nuit
                FROM operateur o
                LEFT JOIN train t ON t.id_operateur = o.id_operateur
                GROUP BY o.id_operateur, o.nom, o.pays
                ORDER BY nb_trains DESC
            """)).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/gares", tags=["Referentiels"])
def get_gares(nom: Optional[str] = None, limit: int = 100):
    try:
        q = "SELECT id_gare, nom, pays FROM gare"
        params: dict = {"limit": limit}
        if nom:
            q += " WHERE nom ILIKE :nom"
            params["nom"] = f"%{nom}%"
        q += " ORDER BY nom LIMIT :limit"
        with get_engine().connect() as c:
            rows = c.execute(text(q), params).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/stats", tags=["Statistiques"])
def get_stats():
    try:
        with get_engine().connect() as c:
            nb_trains     = c.execute(text("SELECT COUNT(*) FROM train")).scalar()
            nb_operateurs = c.execute(text("SELECT COUNT(*) FROM operateur")).scalar()
            nb_gares      = c.execute(text("SELECT COUNT(*) FROM gare")).scalar()
            nb_trajets    = c.execute(text("SELECT COUNT(*) FROM trajet")).scalar()
            nb_jour       = c.execute(text("SELECT COUNT(*) FROM train WHERE type_service='Jour'")).scalar()
            nb_nuit       = c.execute(text("SELECT COUNT(*) FROM train WHERE type_service='Nuit'")).scalar()
            dist_moy      = c.execute(text("SELECT ROUND(AVG(distance)::NUMERIC,1) FROM trajet")).scalar()
        return {
            "nb_trains":           nb_trains,
            "nb_operateurs":       nb_operateurs,
            "nb_gares":            nb_gares,
            "nb_trajets":          nb_trajets,
            "nb_jour":             nb_jour,
            "nb_nuit":             nb_nuit,
            "distance_moyenne_km": float(dist_moy or 0)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/co2", tags=["Statistiques"])
def get_stats_co2():
    try:
        with get_engine().connect() as c:
            rows = c.execute(text("""
                SELECT
                    o.nom AS operateur,
                    ROUND(AVG(t.emission_co2_gkm)::NUMERIC, 2) AS co2_moy_gkm,
                    ROUND(SUM(tr.distance * t.emission_co2_gkm / 1000)::NUMERIC, 2) AS co2_total_kg
                FROM train t
                JOIN operateur o  ON o.id_operateur = t.id_operateur
                JOIN trajet    tr ON tr.id_trajet   = t.id_trajet
                WHERE t.emission_co2_gkm IS NOT NULL
                GROUP BY o.nom
                ORDER BY co2_total_kg DESC
            """)).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/couverture", tags=["Statistiques"])
def get_stats_couverture():
    try:
        with get_engine().connect() as c:
            rows = c.execute(text("""
                SELECT
                    o.nom AS operateur,
                    t.type_service,
                    COUNT(*)                             AS nb,
                    ROUND(MIN(tr.distance)::NUMERIC, 1) AS dist_min,
                    ROUND(AVG(tr.distance)::NUMERIC, 1) AS dist_moy,
                    ROUND(MAX(tr.distance)::NUMERIC, 1) AS dist_max
                FROM train t
                JOIN operateur o  ON o.id_operateur = t.id_operateur
                JOIN trajet    tr ON tr.id_trajet   = t.id_trajet
                GROUP BY o.nom, t.type_service
                ORDER BY o.nom, t.type_service
            """)).mappings().all()
        return [dict(r) for r in rows]
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/qualite", tags=["Statistiques"])
def get_stats_qualite():
    try:
        with get_engine().connect() as c:
            nb       = c.execute(text("SELECT COUNT(*) FROM train")).scalar()
            logs     = c.execute(text("""
                SELECT etape, source, nb_enregistrements, statut,
                       CAST(run_date AS TEXT) AS run_date, message
                FROM etl_logs ORDER BY run_date DESC LIMIT 10
            """)).mappings().all()
            src      = c.execute(text("""
                SELECT source_donnee, COUNT(*) AS nb
                FROM train WHERE source_donnee IS NOT NULL
                GROUP BY source_donnee ORDER BY nb DESC
            """)).mappings().all()
            co2_null = c.execute(text(
                "SELECT COUNT(*) FROM train WHERE emission_co2_gkm IS NULL"
            )).scalar()
        return {
            "nb_trains_total":    nb,
            "co2_manquants":      co2_null,
            "completude_co2_pct": round((1 - co2_null / nb) * 100, 1) if nb else 0,
            "etl_logs":           [dict(r) for r in logs],
            "par_source":         [dict(r) for r in src]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/sous-desserte", tags=["Statistiques"])
def get_sous_desserte():
    """
    DÃ©tecte les zones sous-desservies depuis les 99 854 trains rÃ©els.
    CritÃ¨res : gares avec <= 3 trains ET distance moyenne < 150 km.
    """
    try:
        with get_engine().connect() as c:
            zones = c.execute(text("""
                SELECT
                    g.nom                                           AS gare,
                    g.pays                                          AS pays,
                    o.nom                                           AS operateur_principal,
                    COUNT(t.id_train)                               AS nb_trains,
                    ROUND(AVG(tr.distance)::NUMERIC, 1)             AS dist_moy_km,
                    ROUND(MIN(tr.distance)::NUMERIC, 1)             AS dist_min_km,
                    COUNT(*) FILTER (WHERE t.type_service = 'Nuit') AS nb_nuit,
                    t.type_ligne
                FROM gare g
                JOIN trajet    tr ON tr.id_gare     = g.id_gare
                JOIN train     t  ON t.id_trajet    = tr.id_trajet
                JOIN operateur o  ON o.id_operateur = t.id_operateur
                GROUP BY g.nom, g.pays, o.nom, t.type_ligne
                HAVING COUNT(t.id_train) <= 3
                     AND AVG(tr.distance) > 5
                     AND AVG(tr.distance) < 150
                  ORDER BY COUNT(t.id_train) ASC, AVG(tr.distance) DESC
                LIMIT 20
            """)).mappings().all()

            total_gares    = c.execute(text("SELECT COUNT(*) FROM gare")).scalar()
            gares_fragiles = c.execute(text("""
                SELECT COUNT(*) FROM (
                    SELECT g.id_gare
                    FROM gare g
                    JOIN trajet tr ON tr.id_gare  = g.id_gare
                    JOIN train  t  ON t.id_trajet = tr.id_trajet
                    GROUP BY g.id_gare
                    HAVING COUNT(t.id_train) <= 3
                         AND AVG(tr.distance) > 5
                         AND AVG(tr.distance) < 150
                  ) sub
            """)).scalar()

        return {
            "total_gares":    total_gares,
            "gares_fragiles": gares_fragiles,
            "pct_fragile":    round(gares_fragiles / total_gares * 100, 1) if total_gares else 0,
            "zones":          [dict(r) for r in zones]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class PredictRequest(BaseModel):
    distance_km:  float
    operateur:    str   
    type_service: str   
    type_ligne:   str   
    heure_depart: str   

class PredictResponse(BaseModel):
    co2_prediction_kg:       float
    type_service_prediction: str
    co2_avion_kg:            float
    economie_pct:            float
    ratio_avion_train:       float
    modele_co2:              str
    modele_classification:   str

@app.post("/predict", response_model=PredictResponse, tags=["IA"])
def predict(req: PredictRequest):
    """
    Predit les admissions CO2 et le type de service d'un trajet ferroviaire.
    Modele CO2 : XGBoost R²=0.88 ” 6 features (sans distance directe)
    Modele Nuit : Random Forest F1=0.59 ” 4 features (sans heure)
    """
    if MODEL_CO2 is None:
        raise HTTPException(status_code=503, detail="Modeles ML non charges")

    operateur_map = {"Deutsche Bahn": 0, "SNCB": 1, "SNCF": 2}
    operateur_enc = operateur_map.get(req.operateur, 0)
    ts_enc        = 1 if req.type_service == "Nuit" else 0
    tl_enc        = 1 if req.type_ligne == "national" else 0

    try:
        heure = int(req.heure_depart.split(":")[0]) % 24
    except Exception:
        heure = 12

    heure_sin = math.sin(2 * math.pi * heure / 24)
    heure_cos = math.cos(2 * math.pi * heure / 24)

    d = req.distance_km
    if d < 100:   bucket = 0
    elif d < 300: bucket = 1
    elif d < 600: bucket = 2
    else:         bucket = 3

    # Normalisation distance pour le modele Nuit
    if SCALER is not None:
        df_scale = pd.DataFrame([[d, 0.014]], columns=['distance_km', 'co2_par_km'])
        scaled   = SCALER.transform(df_scale)
        d_scaled = float(scaled[0][0])
    else:
        d_scaled = (d - 86.52) / 107.65  # fallback manuel

    # ENJEU 1 
    features_co2 = pd.DataFrame([[
        operateur_enc, ts_enc, tl_enc,
        heure_sin, heure_cos, bucket
    ]], columns=[
        'operateur_enc', 'type_service_enc', 'type_ligne_enc',
        'heure_sin', 'heure_cos', 'distance_bucket_enc'
    ])

    # ENJEU 2 
    features_nuit = pd.DataFrame([[
        d_scaled, bucket, operateur_enc, tl_enc
    ]], columns=[
        'distance_km', 'distance_bucket_enc',
        'operateur_enc', 'type_ligne_enc'
    ])

    try:
        co2_pred  = float(MODEL_CO2.predict(features_co2)[0])
        nuit_pred = int(MODEL_NUIT.predict(features_nuit)[0])
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur prediction : {str(e)}")

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

