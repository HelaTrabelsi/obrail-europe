import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

from pathlib import Path
load_dotenv(dotenv_path=Path(__file__).parent.parent / '.env')

class DataLoader:
    def __init__(self):
        self.db_config = {
            'host':     os.getenv('DB_HOST',     'localhost'),
            'port':     os.getenv('DB_PORT',     '5432'),
            'database': os.getenv('DB_NAME',     'obrail_db'),
            'user':     os.getenv('DB_USER',     'postgres'),
            'password': os.getenv('DB_PASSWORD', '1234')
        }
        self.engine = None
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.transformed_dir = os.path.join(base, 'data', 'transformed')

    def connect(self):
        try:
            url = (f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                   f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}")
            self.engine = create_engine(url)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print("Connexion PostgreSQL etablie")
            return True
        except Exception as e:
            print(f"Erreur connexion : {e}")
            return False

    def create_schema(self):
        sql = """
        CREATE TABLE IF NOT EXISTS operateur (
            id_operateur SERIAL       PRIMARY KEY,
            nom          VARCHAR(100) NOT NULL UNIQUE,
            pays         VARCHAR(10)  NOT NULL
        );

        CREATE TABLE IF NOT EXISTS gare (
            id_gare   SERIAL       PRIMARY KEY,
            nom       VARCHAR(200) NOT NULL,
            pays      VARCHAR(10)  NOT NULL,
            latitude  FLOAT,
            longitude FLOAT,
            UNIQUE (nom, pays)
        );

        CREATE TABLE IF NOT EXISTS trajet (
            id_trajet       SERIAL  PRIMARY KEY,
            id_gare         INTEGER NOT NULL REFERENCES gare(id_gare),
            id_gare_arrivee INTEGER REFERENCES gare(id_gare),
            distance        FLOAT
        );

        CREATE TABLE IF NOT EXISTS train (
            id_train          SERIAL      PRIMARY KEY,
            id_operateur      INTEGER     NOT NULL REFERENCES operateur(id_operateur),
            id_trajet         INTEGER     NOT NULL REFERENCES trajet(id_trajet),
            type_service      VARCHAR(20) NOT NULL CHECK (type_service IN ('Jour','Nuit')),
            type_ligne        VARCHAR(50) NOT NULL CHECK (type_ligne IN ('national','regional')),
            heure_depart      TIME        NOT NULL,
            heure_arrivee     TIME        NOT NULL,
            emission_co2_gkm  FLOAT,
            source_donnee     VARCHAR(100),
            created_at        TIMESTAMP   DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (id_operateur, id_trajet, heure_depart)
        );

        CREATE INDEX IF NOT EXISTS idx_train_operateur    ON train(id_operateur);
        CREATE INDEX IF NOT EXISTS idx_train_trajet       ON train(id_trajet);
        CREATE INDEX IF NOT EXISTS idx_train_type_service ON train(type_service);
        CREATE INDEX IF NOT EXISTS idx_train_heure        ON train(heure_depart);
        CREATE INDEX IF NOT EXISTS idx_trajet_gare        ON trajet(id_gare);
        

        CREATE TABLE IF NOT EXISTS etl_logs (
            id                 SERIAL    PRIMARY KEY,
            run_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            etape              VARCHAR(50),
            source             VARCHAR(200),
            nb_enregistrements INTEGER,
            statut             VARCHAR(20),
            message            TEXT
        );
        """
        with self.engine.begin() as conn:
            for stmt in sql.split(';'):
                s = stmt.strip()
                if s:
                    conn.execute(text(s))
        print("Schema cree (operateur + gare + trajet[depart+arrivee] + train + etl_logs)")

    def log_etl(self, etape, source, nb, statut, message=""):
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl_logs (etape, source, nb_enregistrements, statut, message)
                    VALUES (:etape, :source, :nb, :statut, :message)
                """), {"etape": etape, "source": source, "nb": nb,
                       "statut": statut, "message": message})
        except Exception as e:
            print(f"Log ETL echoue : {e}")

    def load_normalised(self):
        csv_path = os.path.join(self.transformed_dir, 'dessertes.csv')
        if not os.path.exists(csv_path):
            print(f"Fichier introuvable : {csv_path}")
            return 0

        df = pd.read_csv(csv_path)
        df = df.dropna(subset=['heure_depart', 'heure_arrivee'])
        print(f"  {len(df):,} trajets a charger")

        # Vider les tables
        with self.engine.begin() as conn:
            conn.execute(text("TRUNCATE train, trajet, gare, operateur RESTART IDENTITY CASCADE"))

        pays_op = {'SNCF': 'FR', 'Deutsche Bahn': 'DE', 'SNCB': 'BE'}

        # â”€â”€ 1. operateur â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        ops = df['operateur_nom'].dropna().unique()
        op_df = pd.DataFrame([{'nom': op, 'pays': pays_op.get(op, 'EU')} for op in ops])
        with self.engine.begin() as conn:
            op_df.to_sql('operateur', conn, if_exists='append', index=False,
                         method='multi', chunksize=500)
        with self.engine.connect() as conn:
            op_id = pd.read_sql("SELECT id_operateur, nom FROM operateur", conn)
        op_id_map = dict(zip(op_id['nom'], op_id['id_operateur']))
        print(f"  {len(op_id_map)} operateurs inseres")

        # â”€â”€ 2. gare (depart + arrivee) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        df['pays'] = df['operateur_nom'].map(pays_op).fillna('EU')
        gares_dep = df[['gare_depart_nom', 'pays']].rename(columns={'gare_depart_nom': 'nom'})
        gares_arr = df[['gare_arrivee_nom', 'pays']].rename(columns={'gare_arrivee_nom': 'nom'})
        all_gares = pd.concat([gares_dep, gares_arr]).drop_duplicates(subset=['nom', 'pays']).dropna(subset=['nom'])
        with self.engine.begin() as conn:
            all_gares.to_sql('gare', conn, if_exists='append', index=False,
                             method='multi', chunksize=500)
        with self.engine.connect() as conn:
            gare_df = pd.read_sql("SELECT id_gare, nom, pays FROM gare", conn)
        gare_map = dict(zip(zip(gare_df['nom'], gare_df['pays']), gare_df['id_gare']))
        print(f"  {len(gare_map)} gares inserees")

        # â”€â”€ 3. trajet (avec gare depart ET arrivee) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        df['id_gare_dep'] = df.apply(lambda r: gare_map.get((r['gare_depart_nom'], r['pays'])), axis=1)
        df['id_gare_arr'] = df.apply(lambda r: gare_map.get((r['gare_arrivee_nom'], r['pays'])), axis=1)

        trajets_uniq = (df.groupby(['id_gare_dep', 'id_gare_arr'], as_index=False)['distance_km']
                        .mean()
                        .dropna(subset=['id_gare_dep', 'id_gare_arr']))
        trajets_uniq['id_gare_dep'] = trajets_uniq['id_gare_dep'].astype(int)
        trajets_uniq['id_gare_arr'] = trajets_uniq['id_gare_arr'].astype(int)
        trajets_uniq = trajets_uniq.rename(columns={
            'id_gare_dep': 'id_gare',
            'id_gare_arr': 'id_gare_arrivee',
            'distance_km': 'distance'
        })

        with self.engine.begin() as conn:
            trajets_uniq[['id_gare', 'id_gare_arrivee', 'distance']].to_sql(
                'trajet', conn, if_exists='append', index=False,
                method='multi', chunksize=500)
        with self.engine.connect() as conn:
            trajet_df = pd.read_sql("SELECT id_trajet, id_gare, id_gare_arrivee FROM trajet", conn)
        # Map (id_gare_dep, id_gare_arr) -> id_trajet
        trajet_map = dict(zip(
            zip(trajet_df['id_gare'], trajet_df['id_gare_arrivee']),
            trajet_df['id_trajet']
        ))
        print(f"  {len(trajet_map)} trajets inseres (avec gare arrivee)")

        # â”€â”€ 4. train â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
        df['id_operateur'] = df['operateur_nom'].map(op_id_map)
        df['id_trajet'] = df.apply(
            lambda r: trajet_map.get((r['id_gare_dep'], r['id_gare_arr'])),
            axis=1
        )

        train_df = df[['id_operateur', 'id_trajet', 'type_service', 'type_ligne',
                        'heure_depart', 'heure_arrivee', 'emissions_co2_gkm', 'source_donnee']].copy()
        train_df = train_df.rename(columns={'emissions_co2_gkm': 'emission_co2_gkm'})
        train_df = train_df.dropna(subset=['id_operateur', 'id_trajet'])
        train_df['id_operateur'] = train_df['id_operateur'].astype(int)
        train_df['id_trajet']    = train_df['id_trajet'].astype(int)
        train_df = train_df.drop_duplicates(subset=['id_operateur', 'id_trajet', 'heure_depart'])

        with self.engine.begin() as conn:
            train_df.to_sql('train', conn, if_exists='append', index=False,
                            method='multi', chunksize=500)
        print(f"  {len(train_df):,} trains inseres")

        self.log_etl("load", csv_path, len(train_df), "success",
                     f"{len(op_id_map)} operateurs, {len(gare_map)} gares, {len(trajet_map)} trajets avec depart+arrivee")
        return len(train_df)

    def run_load(self):
        if not self.connect():
            return False
        self.create_schema()
        loaded = self.load_normalised()
        if loaded > 0:
            print("Chargement termine")
            self.get_stats()
            return True
        return False

    def load_all_data(self, clean_first=True):
        return self.run_load()

    def get_stats(self):
        try:
            with self.engine.connect() as conn:
                nb = conn.execute(text("SELECT COUNT(*) FROM train")).scalar()
                stats = conn.execute(text("""
                    SELECT o.nom, COUNT(t.id_train) as nb
                    FROM train t JOIN operateur o ON o.id_operateur = t.id_operateur
                    GROUP BY o.nom ORDER BY nb DESC
                """)).fetchall()
                print(f"\nBase de donnees :")
                print(f"  Total trains : {nb:,}")
                for op, n in stats:
                    print(f"    - {op}: {n:,}")
        except Exception as e:
            print(f"Erreur stats : {e}")


if __name__ == "__main__":
    DataLoader().run_load()

