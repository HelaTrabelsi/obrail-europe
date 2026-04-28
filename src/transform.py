import pandas as pd
import json
import os
import numpy as np
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2


class DataTransformer:
    def __init__(self):
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.raw_dir = os.path.join(base, 'data', 'raw')
        self.transformed_dir = os.path.join(base, 'data', 'transformed')
        self.processed_dir = os.path.join(base, 'data', 'processed')
        self.dessertes = []

    def haversine_distance(self, lat1, lon1, lat2, lon2):
        if pd.isna(lat1) or pd.isna(lon1) or pd.isna(lat2) or pd.isna(lon2):
            return np.nan
        R = 6371
        lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
        dlat, dlon = lat2 - lat1, lon2 - lon1
        a = sin(dlat/2)**2 + cos(lat1)*cos(lat2)*sin(dlon/2)**2
        return R * 2 * atan2(sqrt(a), sqrt(1-a))

    def normalize_gtfs_time(self, t):
        if pd.isna(t):
            return None
        try:
            parts = str(t).split(':')
            h, m = int(parts[0]) % 24, int(parts[1])
            s = int(parts[2]) if len(parts) > 2 else 0
            return f"{h:02d}:{m:02d}:{s:02d}"
        except Exception:
            return None

    def process_api_source(self, filepath, source_name):
        """
        Les sources API SNCF CO2 n'ont pas d'horaires.
        On les traite pour récupérer emissions_co2_gkm et distance_km
        mais elles seront filtrées plus tard car heure_depart/arrivee = None.
        On les garde quand même pour enrichir les données GTFS par jointure future.
        """
        with open(filepath, 'r') as f:
            records = json.load(f)
        df = pd.DataFrame(records)
        keep = ['transporteur', 'origine', 'destination',
                'distance_entre_les_gares', 'train_empreinte_carbone_kgco2e']
        df = df[[c for c in keep if c in df.columns]].copy()
        df.rename(columns={
            'transporteur': 'operateur_nom', 'origine': 'gare_depart_nom',
            'destination': 'gare_arrivee_nom',
            'distance_entre_les_gares': 'distance_km',
            'train_empreinte_carbone_kgco2e': 'co2_kg'
        }, inplace=True)
        df['emissions_co2_gkm'] = (df['co2_kg'] * 1000 / df['distance_km']).fillna(0)
        df['type_service'] = 'Jour'
        df['type_ligne'] = 'national'
        df['source_donnee'] = source_name
        df['nom_ligne'] = df['gare_depart_nom'] + ' → ' + df['gare_arrivee_nom']
        df['heure_depart'] = None
        df['heure_arrivee'] = None
        cols = ['operateur_nom', 'nom_ligne', 'type_ligne', 'type_service',
                'gare_depart_nom', 'gare_arrivee_nom', 'heure_depart', 'heure_arrivee',
                'distance_km', 'emissions_co2_gkm', 'source_donnee']
        return df[cols]

    #  GTFS 
    def process_gtfs_source(self, source_dir, source_name, pays):
        stops_file     = os.path.join(source_dir, 'stops.txt')
        trips_file     = os.path.join(source_dir, 'trips.txt')
        stop_times_file= os.path.join(source_dir, 'stop_times.txt')

        if not all(os.path.exists(f) for f in [stops_file, trips_file, stop_times_file]):
            print(f"        {source_name} : fichiers GTFS manquants, ignoré")
            return None

        stops      = pd.read_csv(stops_file, dtype=str)
        stop_times = pd.read_csv(stop_times_file, dtype=str)

        stops = stops[['stop_id','stop_name','stop_lat','stop_lon']].copy()
        stops['stop_lat'] = pd.to_numeric(stops['stop_lat'], errors='coerce')
        stops['stop_lon'] = pd.to_numeric(stops['stop_lon'], errors='coerce')

        stop_times['stop_sequence'] = pd.to_numeric(stop_times['stop_sequence'], errors='coerce')
        stop_times = stop_times.dropna(subset=['stop_sequence'])

        first = (stop_times.loc[stop_times.groupby('trip_id')['stop_sequence'].idxmin()]
                 [['trip_id','stop_id','departure_time']]
                 .rename(columns={'stop_id':'depart_stop_id','departure_time':'heure_depart'}))
        last  = (stop_times.loc[stop_times.groupby('trip_id')['stop_sequence'].idxmax()]
                 [['trip_id','stop_id','arrival_time']]
                 .rename(columns={'stop_id':'arrive_stop_id','arrival_time':'heure_arrivee'}))

        journeys = first.merge(last, on='trip_id')
        journeys = journeys.merge(stops[['stop_id','stop_name','stop_lat','stop_lon']],
                                  left_on='depart_stop_id', right_on='stop_id', how='left')
        journeys.rename(columns={'stop_name':'gare_depart_nom','stop_lat':'depart_lat','stop_lon':'depart_lon'}, inplace=True)
        journeys = journeys.merge(stops[['stop_id','stop_name','stop_lat','stop_lon']],
                                  left_on='arrive_stop_id', right_on='stop_id', how='left')
        journeys.rename(columns={'stop_name':'gare_arrivee_nom','stop_lat':'arrive_lat','stop_lon':'arrive_lon'}, inplace=True)

        journeys['distance_km'] = journeys.apply(
            lambda r: self.haversine_distance(r['depart_lat'], r['depart_lon'],
                                              r['arrive_lat'], r['arrive_lon']), axis=1)
        journeys = journeys.dropna(subset=['distance_km','gare_depart_nom','gare_arrivee_nom'])
        journeys = journeys[journeys['distance_km'] > 0]

        journeys['heure_depart']  = journeys['heure_depart'].apply(self.normalize_gtfs_time)
        journeys['heure_arrivee'] = journeys['heure_arrivee'].apply(self.normalize_gtfs_time)

        # Filtre horaires manquants
        before = len(journeys)
        journeys = journeys.dropna(subset=['heure_depart','heure_arrivee'])
        print(f"    Horaires manquants supprimés : {before - len(journeys)} trajets")
        if journeys.empty:
            return None

        op_map = {'sncf':'SNCF', 'db':'Deutsche Bahn', 'sncb':'SNCB'}
        operateur_nom = next((v for k,v in op_map.items() if k in source_name),
                             source_name.replace('_',' ').title())

        journeys['operateur_nom']    = operateur_nom
        journeys['type_service']     = journeys['heure_depart'].apply(
            lambda x: 'Nuit' if int(str(x).split(':')[0]) >= 22 or int(str(x).split(':')[0]) < 5 else 'Jour')
        journeys['type_ligne']       = 'regional' if any(k in source_name for k in ['ter','regional']) else 'national'
        journeys['emissions_co2_gkm']= 3.8
        journeys['source_donnee']    = f"gtfs_{source_name}"
        journeys['nom_ligne']        = journeys['gare_depart_nom'] + ' → ' + journeys['gare_arrivee_nom']

        cols = ['operateur_nom','nom_ligne','type_ligne','type_service',
                'gare_depart_nom','gare_arrivee_nom','heure_depart','heure_arrivee',
                'distance_km','emissions_co2_gkm','source_donnee']
        return journeys[cols]

    #  Pipeline principal 
    def run_transformation(self):
        print("TRANSFORMATION DES DONNEES (API + GTFS)")
        os.makedirs(self.transformed_dir, exist_ok=True)
        os.makedirs(self.processed_dir,   exist_ok=True)

        gtfs_root = os.path.join(self.raw_dir, 'gtfs')
        if os.path.exists(gtfs_root):
            for source_name in sorted(os.listdir(gtfs_root)):
                source_path = os.path.join(gtfs_root, source_name)
                if os.path.isdir(source_path):
                    pays = 'DE' if 'germany' in source_name else ('BE' if 'belgium' in source_name else 'FR')
                    print(f"\n  Traitement GTFS : {source_name} ({pays})")
                    df = self.process_gtfs_source(source_path, source_name, pays)
                    if df is not None and not df.empty:
                        self.dessertes.append(df)
                        print(f"      {len(df):,} trajets retenus")

        if not self.dessertes:
            print("Aucune donnee GTFS transformee")
            return None

        df = pd.concat(self.dessertes, ignore_index=True)
        print(f"\n  Total apres fusion : {len(df):,} trajets")

        before = len(df)
        df = df.dropna(subset=['heure_depart','heure_arrivee'])
        removed_times = before - len(df)
        if removed_times:
            print(f"  Filtre horaires (global) : {removed_times} supprimes")

        before = len(df)
        df = df.drop_duplicates(
            subset=['operateur_nom','gare_depart_nom','gare_arrivee_nom','heure_depart'],
            keep='first')
        removed_dup = before - len(df)
        print(f"  Doublons supprimes : {removed_dup}")
        print(f"  Trajets finaux : {len(df):,}")

        csv_path = os.path.join(self.transformed_dir, 'dessertes.csv')
        df.to_csv(csv_path, index=False, encoding='utf-8')
        print(f"\n  CSV sauvegarde : {csv_path}")

        stats = {
            "avant_doublons": before,
            "apres_doublons": len(df),
            "doublons_supprimes": removed_dup,
            "sans_horaires_supprimes": removed_times,
            "total_trajets": len(df),
            "repartition_operateurs": df['operateur_nom'].value_counts().to_dict(),
            "repartition_type_service": df['type_service'].value_counts().to_dict(),
            "date_transformation": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        with open(os.path.join(self.transformed_dir, 'stats.json'), 'w') as f:
            json.dump(stats, f, indent=2)

        df_dash = df.copy()
        df_dash['co2_emission_kg'] = df_dash['emissions_co2_gkm'] * df_dash['distance_km'] / 1000
        df_dash = df_dash.rename(columns={
            'operateur_nom':'operator',
            'gare_depart_nom':'origin_station',
            'gare_arrivee_nom':'destination_station'
        })
        parquet_path = os.path.join(self.processed_dir, 'all_journeys_cleaned.parquet')
        df_dash.to_parquet(parquet_path, index=False)
        print(f"  Parquet sauvegarde : {parquet_path}")

        return df


if __name__ == "__main__":
    DataTransformer().run_transformation()