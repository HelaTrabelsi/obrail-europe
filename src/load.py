import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

class DataLoader:
    def __init__(self):
        self.db_config = {
            'host':     os.getenv('DB_HOST',     'localhost'),
            'port':     os.getenv('DB_PORT',     '5432'),
            'database': os.getenv('DB_NAME',     'obrail_db'),
            'user':     os.getenv('DB_USER',     'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
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
        """
        Schema normalise selon le MPD :
        operateur -> gare -> trajet -> train
        """
        sql = """
        -- ── operateur ────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS operateur (
            id_operateur SERIAL       PRIMARY KEY,
            nom          VARCHAR(100) NOT NULL UNIQUE,
            pays         VARCHAR(10)  NOT NULL
        );

        -- ── gare ─────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS gare (
            id_gare SERIAL       PRIMARY KEY,
            nom     VARCHAR(200) NOT NULL,
            pays    VARCHAR(10)  NOT NULL,
            UNIQUE  (nom, pays)
        );

        -- ── trajet ───────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS trajet (
            id_trajet SERIAL  PRIMARY KEY,
            id_gare   INTEGER NOT NULL REFERENCES gare(id_gare),
            distance  FLOAT,
            UNIQUE    (id_gare)
        );

        -- ── train ────────────────────────────────────────────────
        CREATE TABLE IF NOT EXISTS train (
            id_train          SERIAL       PRIMARY KEY,
            id_operateur      INTEGER      NOT NULL REFERENCES operateur(id_operateur),
            id_trajet         INTEGER      NOT NULL REFERENCES trajet(id_trajet),
            type_service      VARCHAR(20)  NOT NULL CHECK (type_service IN ('Jour','Nuit')),
            type_ligne        VARCHAR(50)  NOT NULL CHECK (type_ligne IN ('national','regional')),
            heure_depart      TIME         NOT NULL,
            heure_arrivee     TIME         NOT NULL,
            emission_co2_gkm  FLOAT,
            source_donnee     VARCHAR(100),
            created_at        TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (id_operateur, id_trajet, heure_depart)
        );

        CREATE INDEX IF NOT EXISTS idx_train_operateur    ON train(id_operateur);
        CREATE INDEX IF NOT EXISTS idx_train_trajet       ON train(id_trajet);
        CREATE INDEX IF NOT EXISTS idx_train_type_service ON train(type_service);
        CREATE INDEX IF NOT EXISTS idx_train_heure        ON train(heure_depart);
        CREATE INDEX IF NOT EXISTS idx_trajet_gare        ON trajet(id_gare);

        -- ── etl_logs ─────────────────────────────────────────────
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
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        print("Schema normalise cree (operateur + gare + trajet + train + etl_logs)")

    def log_etl(self, etape, source, nb, statut, message=""):
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl_logs (etape, source, nb_enregistrements, statut, message)
                    VALUES (:etape, :source, :nb, :statut, :message)
                """), {"etape": etape, "source": source, "nb": nb, "statut": statut, "message": message})
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

        # Vider les tables dans l'ordre inverse des FK
        with self.engine.begin() as conn:
            conn.execute(text("TRUNCATE train, trajet, gare, operateur RESTART IDENTITY CASCADE"))

        # ── 1. operateur ─────────────────────────────────────────
        ops = df['operateur_nom'].dropna().unique()
        pays_map = {'SNCF':'FR', 'Deutsche Bahn':'DE', 'SNCB':'BE'}
        op_rows = [{'nom': op, 'pays': pays_map.get(op, 'EU')} for op in ops]
        op_df = pd.DataFrame(op_rows)
        with self.engine.begin() as conn:
            op_df.to_sql('operateur', conn, if_exists='append', index=False,
                         method='multi', chunksize=500)
        # Recuperer les IDs
        with self.engine.connect() as conn:
            op_id = pd.read_sql("SELECT id_operateur, nom FROM operateur", conn)
        op_id_map = dict(zip(op_id['nom'], op_id['id_operateur']))
        print(f"  {len(op_id_map)} operateurs inseres")

        # ── 2. gare ──────────────────────────────────────────────
        # Toutes les gares uniques (depart + arrivee)
        gares_dep = df[['gare_depart_nom','operateur_nom']].rename(
            columns={'gare_depart_nom':'nom'})
        gares_arr = df[['gare_arrivee_nom','operateur_nom']].rename(
            columns={'gare_arrivee_nom':'nom'})
        pays_op = {'SNCF':'FR', 'Deutsche Bahn':'DE', 'SNCB':'BE'}
        gares_dep['pays'] = gares_dep['operateur_nom'].map(pays_op).fillna('EU')
        gares_arr['pays'] = gares_arr['operateur_nom'].map(pays_op).fillna('EU')
        all_gares = pd.concat([gares_dep[['nom','pays']], gares_arr[['nom','pays']]]).drop_duplicates(subset=['nom','pays'])
        all_gares = all_gares.dropna(subset=['nom'])
        with self.engine.begin() as conn:
            all_gares.to_sql('gare', conn, if_exists='append', index=False,
                             method='multi', chunksize=500)
        with self.engine.connect() as conn:
            gare_id = pd.read_sql("SELECT id_gare, nom, pays FROM gare", conn)
        gare_id_map = dict(zip(zip(gare_id['nom'], gare_id['pays']), gare_id['id_gare']))
        print(f"  {len(gare_id_map)} gares inserees")

        # ── 3. trajet ────────────────────────────────────────────

        df['pays'] = df['operateur_nom'].map(pays_op).fillna('EU')
        df['id_gare_depart'] = df.apply(
            lambda r: gare_id_map.get((r['gare_depart_nom'], r['pays'])), axis=1)
        df['id_gare_arrivee'] = df.apply(
            lambda r: gare_id_map.get((r['gare_arrivee_nom'], r['pays'])), axis=1)

        # Trajets uniques par paire depart/arrivee
        trajets_uniq = df.groupby(['id_gare_depart','id_gare_arrivee'], as_index=False)['distance_km'].mean()
        trajets_uniq = trajets_uniq.dropna(subset=['id_gare_depart','id_gare_arrivee'])
        trajets_uniq = trajets_uniq.rename(columns={'id_gare_depart':'id_gare','distance_km':'distance'})
        trajets_uniq['id_gare'] = trajets_uniq['id_gare'].astype(int)

        # Supprimer UNIQUE sur id_gare si plusieurs arrivees par depart
        # -> on utilise la colonne id_gare comme gare_depart
        trajets_uniq = trajets_uniq.drop_duplicates(subset=['id_gare'])

        with self.engine.begin() as conn:
            trajets_uniq[['id_gare','distance']].to_sql(
                'trajet', conn, if_exists='append', index=False,
                method='multi', chunksize=500)
        with self.engine.connect() as conn:
            trajet_id = pd.read_sql("SELECT id_trajet, id_gare FROM trajet", conn)
        trajet_id_map = dict(zip(trajet_id['id_gare'], trajet_id['id_trajet']))
        print(f"  {len(trajet_id_map)} trajets inseres")

        # ── 4. train ─────────────────────────────────────────────
        df['id_operateur'] = df['operateur_nom'].map(op_id_map)
        df['id_trajet']    = df['id_gare_depart'].map(trajet_id_map)

        train_df = df[['id_operateur','id_trajet','type_service','type_ligne',
                        'heure_depart','heure_arrivee','emissions_co2_gkm','source_donnee']].copy()
        train_df = train_df.rename(columns={'emissions_co2_gkm':'emission_co2_gkm'})
        train_df = train_df.dropna(subset=['id_operateur','id_trajet'])
        train_df['id_operateur'] = train_df['id_operateur'].astype(int)
        train_df['id_trajet']    = train_df['id_trajet'].astype(int)
        train_df = train_df.drop_duplicates(subset=['id_operateur','id_trajet','heure_depart'])

        with self.engine.begin() as conn:
            train_df.to_sql('train', conn, if_exists='append', index=False,
                            method='multi', chunksize=500)
        print(f"  {len(train_df):,} trains inseres")

        self.log_etl("load", csv_path, len(train_df), "success",
                     f"Schema normalise : {len(op_id_map)} operateurs, {len(gare_id_map)} gares, {len(trajet_id_map)} trajets")
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