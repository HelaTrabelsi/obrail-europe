
import argparse, math, os, sys
import joblib, numpy as np, pandas as pd

# ── Chemins ──────────────────────────────────────────────
BASE     = os.path.dirname(os.path.abspath(__file__))
MODELS   = os.path.join(BASE, "notebooks", "outputs_models")
SPLITS   = os.path.join(BASE, "notebooks", "ml_splits")

PATH_CO2     = os.path.join(MODELS, "best_model_co2.joblib")
PATH_DES     = os.path.join(MODELS, "best_model_desserte.joblib")
PATH_SCALER  = os.path.join(SPLITS, "scaler.joblib")

# ── Encodages ────────────────────────────────────────────
OPERATEUR_MAP = {"Deutsche Bahn": 0, "SNCB": 1, "SNCF": 2}
PAYS_MAP      = {"Deutsche Bahn": 0, "SNCB": 1, "SNCF": 2}  # DE=0, BE=1, FR=2

def get_pays_enc(operateur: str) -> int:
    if operateur == "SNCF":          return 2  # FR
    if operateur == "Deutsche Bahn": return 0  # DE
    return 1                                    # BE (SNCB)

# ── Chargement modèles + scaler ──────────────────────────
def load_models():
    for p, label in [(PATH_CO2,"CO2"), (PATH_DES,"Desserte"), (PATH_SCALER,"Scaler")]:
        if not os.path.exists(p):
            print(f"Erreur — {label} introuvable : {p}")
            print("Lancez : python notebooks/02_preprocessing.py && python notebooks/03_models.py")
            sys.exit(1)
    mod_co2 = joblib.load(PATH_CO2)
    mod_des = joblib.load(PATH_DES)
    scaler  = joblib.load(PATH_SCALER)
    print(f"Enjeu 1 CO2        -> {type(mod_co2).__name__}")
    print(f"Enjeu 2 Desserte   -> {type(mod_des).__name__}")
    print(f"Scaler             -> mean_dist={scaler.mean_[0]:.4f}  scale={scaler.scale_[0]:.4f}")
    return mod_co2, mod_des, scaler

# ── Encodage features ─────────────────────────────────────
def encode(distance_km, operateur, type_service, type_ligne, heure_depart, scaler):
    d = float(distance_km)

    op_enc   = OPERATEUR_MAP.get(operateur, 0)
    pays_enc = get_pays_enc(operateur)
    ts_enc   = 1 if type_service == "Nuit" else 0
    tl_enc   = 1 if type_ligne == "national" else 0

    try:
        h = int(heure_depart.split(":")[0]) % 24
    except Exception:
        h = 12

    heure_sin = math.sin(2 * math.pi * h / 24)
    heure_cos = math.cos(2 * math.pi * h / 24)

    bucket = 0 if d < 100 else 1 if d < 300 else 2 if d < 600 else 3

    # Normalisation co2_par_km pour l'enjeu desserte
    co2_par_km_brut = 0.014  # 14 g/km = 0.014 kg/km (constante ADEME)
    scaled = scaler.transform([[d, co2_par_km_brut]])
    co2_par_km_scaled = float(scaled[0][1])

    # ENJEU 1 — CO2 : 7 features, sans distance directe
    feat_co2 = pd.DataFrame([[
        op_enc, ts_enc, tl_enc,
        pays_enc, heure_sin, heure_cos, bucket
    ]], columns=[
        'operateur_enc', 'type_service_enc', 'type_ligne_enc',
        'pays_enc', 'heure_sin', 'heure_cos', 'distance_bucket_enc'
    ])

    # ENJEU 2 — SOUS-DESSERTE : 6 features, sans distance directe
    feat_des = pd.DataFrame([[
        op_enc, pays_enc, tl_enc,
        heure_sin, heure_cos, co2_par_km_scaled
    ]], columns=[
        'operateur_enc', 'pays_enc', 'type_ligne_enc',
        'heure_sin', 'heure_cos', 'co2_par_km'
    ])

    return feat_co2, feat_des

# ── Prédiction ────────────────────────────────────────────
def predict(mod_co2, mod_des, scaler,
            distance_km, operateur, type_service, type_ligne, heure_depart):

    feat_co2, feat_des = encode(
        distance_km, operateur, type_service, type_ligne, heure_depart, scaler)

    co2_kg    = float(mod_co2.predict(feat_co2)[0])
    des_pred  = int(mod_des.predict(feat_des)[0])

    co2_avion = float(distance_km) * 258 / 1000
    economie  = (co2_avion - co2_kg) / co2_avion * 100 if co2_avion > 0 else 0
    ratio     = co2_avion / co2_kg if co2_kg > 0 else 18.4

    return {
        "distance_km":         round(float(distance_km), 2),
        "operateur":           operateur,
        "type_service":        type_service,
        "type_ligne":          type_ligne,
        "heure_depart":        heure_depart,
        "co2_train_kg":        round(co2_kg, 4),
        "co2_avion_kg":        round(co2_avion, 3),
        "economie_co2_pct":    round(economie, 1),
        "ratio_avion_train":   round(ratio, 1),
        "sous_desserte_pred":  des_pred,
        "sous_desserte_label": "Fragile" if des_pred == 1 else "Normal",
        "modele_co2":          type(mod_co2).__name__,
        "modele_desserte":     type(mod_des).__name__,
    }

# ── Affichage ─────────────────────────────────────────────
def afficher(r):
    print("\n" + "=" * 60)
    print("  ObRail Europe — Prediction ML (2 enjeux)")
    print("=" * 60)
    print(f"  Trajet        : {r['operateur']} — {r['distance_km']} km")
    print(f"  Type service  : {r['type_service']} | Ligne : {r['type_ligne']}")
    print(f"  Heure départ  : {r['heure_depart']}")
    print("-" * 60)
    print(f"  ENJEU 1 — CO2")
    print(f"    CO2 train   : {r['co2_train_kg']:.4f} kg  ({r['modele_co2']})")
    print(f"    CO2 avion   : {r['co2_avion_kg']:.3f} kg")
    print(f"    Economie    : {r['economie_co2_pct']:.1f} % vs avion")
    print(f"    Ratio       : l'avion emet {r['ratio_avion_train']:.1f}x plus")
    print(f"  ENJEU 2 — SOUS-DESSERTE")
    print(f"    Prediction  : {r['sous_desserte_label']}  ({r['modele_desserte']})")
    if r['sous_desserte_pred'] == 1:
        print(f"    => Liaison fragile — candidate au financement TEN-T")
    else:
        print(f"    => Liaison correctement desservie")
    print("=" * 60)

# ── Mode démo ─────────────────────────────────────────────
EXEMPLES = [
    (392,  "SNCF",          "Jour", "national", "08:30:00", "Paris -> Lyon"),
    (878,  "Deutsche Bahn", "Nuit", "national", "22:00:00", "Paris -> Berlin"),
    (265,  "SNCB",          "Jour", "national", "07:15:00", "Bruxelles -> Paris"),
    (612,  "Deutsche Bahn", "Nuit", "national", "22:00:00", "Munich -> Hambourg"),
    (45,   "SNCF",          "Jour", "regional", "07:30:00", "Lyon -> Grenoble"),
    (916,  "SNCB",          "Nuit", "national", "21:00:00", "Bruxelles -> Vienne"),
]

def mode_demo(mod_co2, mod_des, scaler):
    print("\n" + "=" * 75)
    print("  ObRail Europe — Demo predictions ML (2 enjeux)")
    print("  CO2 : Random Forest (R2=0,8592) | Desserte : Logistic Regression (AUC=0,7410)")
    print("=" * 75)
    rows = []
    for dist, op, ts, tl, heure, label in EXEMPLES:
        r = predict(mod_co2, mod_des, scaler, dist, op, ts, tl, heure)
        rows.append({
            "Trajet":    label,
            "Dist(km)":  dist,
            "CO2 train": f"{r['co2_train_kg']:.4f} kg",
            "CO2 avion": f"{r['co2_avion_kg']:.1f} kg",
            "Economie":  f"{r['economie_co2_pct']:.1f}%",
            "Ratio":     f"{r['ratio_avion_train']:.1f}x",
            "Desserte":  r['sous_desserte_label'],
        })
    df = pd.DataFrame(rows)
    print(df.to_string(index=False))

    co2_trains = [float(r["CO2 train"].replace(" kg", "")) for r in rows]
    co2_avions = [float(r["CO2 avion"].replace(" kg", "")) for r in rows]
    print("\n" + "-" * 75)
    print(f"  Total CO2 train   : {sum(co2_trains):.4f} kg")
    print(f"  Total CO2 avion   : {sum(co2_avions):.1f} kg")
    print(f"  CO2 economise     : {sum(co2_avions)-sum(co2_trains):.2f} kg")
    print(f"  Economie moyenne  : {(sum(co2_avions)-sum(co2_trains))/sum(co2_avions)*100:.1f}%")
    print(f"  Gares fragiles    : {sum(1 for r in rows if r['Desserte']=='Fragile')}/{len(rows)}")
    print("=" * 75)

    out = "notebooks/predictions_demo.csv"
    df.to_csv(out, index=False)
    print(f"\nResultats exportes : {out}")

# ── Main ──────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="ObRail Europe — Prediction CO2 + Sous-desserte",
        epilog="""
Exemples :
  python predict.py --demo
  python predict.py --distance 392 --operateur "SNCF" --type_service Jour --type_ligne national --heure 08:30:00
  python predict.py --distance 878 --operateur "Deutsche Bahn" --type_service Nuit --type_ligne national --heure 22:00:00
        """
    )
    parser.add_argument("--demo",         action="store_true")
    parser.add_argument("--distance",     type=float)
    parser.add_argument("--operateur",    type=str)
    parser.add_argument("--type_service", type=str, default="Jour")
    parser.add_argument("--type_ligne",   type=str, default="regional")
    parser.add_argument("--heure",        type=str, default="08:00:00")
    args = parser.parse_args()

    print("=" * 60)
    print("  ObRail Europe — Pipeline de prediction ML")
    print("  2 enjeux : CO2 (regression) + Sous-desserte (classification)")
    print("  Bloc E6.2 — RNCP36581")
    print("=" * 60)

    mod_co2, mod_des, scaler = load_models()

    if args.demo:
        mode_demo(mod_co2, mod_des, scaler)
    elif args.distance is not None and args.operateur is not None:
        r = predict(mod_co2, mod_des, scaler,
                    args.distance, args.operateur,
                    args.type_service, args.type_ligne, args.heure)
        afficher(r)
    else:
        print("\nErreur — Precisez --demo ou --distance + --operateur")
        parser.print_help()
        sys.exit(1)

if __name__ == "__main__":
    main()