import pandas as pd
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()


class DataLoader:
    def __init__(self):
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': os.getenv('DB_PORT', '5432'),
            'database': os.getenv('DB_NAME', 'obrail_db'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', 'postgres')
        }
        self.engine = None
        base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.transformed_dir = os.path.join(base, 'data', 'transformed')
    def connect(self):
        try:
            db_url = (
                f"postgresql://{self.db_config['user']}:{self.db_config['password']}"
                f"@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['database']}"
            )
            self.engine = create_engine(db_url)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            print(" Connexion PostgreSQL établie")
            return True
        except Exception as e:
            print(f" Erreur de connexion : {e}")
            return False

    def create_schema(self):
        sql = """
        CREATE TABLE IF NOT EXISTS dessertes (
            id                 SERIAL PRIMARY KEY,
            operateur_nom      VARCHAR(100),
            nom_ligne          TEXT,
            type_ligne         VARCHAR(50),
            type_service       VARCHAR(20),
            gare_depart_nom    VARCHAR(200),
            gare_arrivee_nom   VARCHAR(200),
            heure_depart       TIME,
            heure_arrivee      TIME,
            distance_km        FLOAT,
            emissions_co2_gkm  FLOAT,
            source_donnee      VARCHAR(100),
            created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(operateur_nom, gare_depart_nom, gare_arrivee_nom, heure_depart)
        );
        CREATE INDEX IF NOT EXISTS idx_dessertes_operateur ON dessertes(operateur_nom);
        CREATE INDEX IF NOT EXISTS idx_dessertes_gares     ON dessertes(gare_depart_nom, gare_arrivee_nom);
        CREATE INDEX IF NOT EXISTS idx_dessertes_type      ON dessertes(type_service, type_ligne);
        CREATE INDEX IF NOT EXISTS idx_dessertes_heure     ON dessertes(heure_depart);

        CREATE TABLE IF NOT EXISTS etl_logs (
            id                  SERIAL PRIMARY KEY,
            run_date            TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            etape               VARCHAR(50),
            source              VARCHAR(200),
            nb_enregistrements  INTEGER,
            statut              VARCHAR(20),
            message             TEXT
        );
        """
        with self.engine.begin() as conn:
            for stmt in sql.split(';'):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))
        print(" Schéma créé (tables dessertes + etl_logs)")

    def log_etl(self, etape, source, nb, statut, message=""):
        try:
            with self.engine.begin() as conn:
                conn.execute(text("""
                    INSERT INTO etl_logs (etape, source, nb_enregistrements, statut, message)
                    VALUES (:etape, :source, :nb, :statut, :message)
                """), {"etape": etape, "source": source, "nb": nb, "statut": statut, "message": message})
        except Exception as e:
            print(f"    Log ETL échoué : {e}")

    def load_dessertes(self):
        csv_path = f"{self.transformed_dir}/dessertes.csv"
        if not os.path.exists(csv_path):
            print(f" Fichier introuvable : {csv_path}")
            return 0

        df = pd.read_csv(csv_path)
        initial_count = len(df)

        # ─── Filtre défensif : enlever les trajets sans horaires ──────────────
        before = len(df)
        df = df.dropna(subset=['heure_depart', 'heure_arrivee'])
        after = len(df)
        if before > after:
            print(f"    {before - after} trajets sans horaires exclus du chargement")

        # Conversion TIME
        for col in ['heure_depart', 'heure_arrivee']:
            df[col] = df[col].where(pd.notnull(df[col]), None)

        with self.engine.begin() as conn:
            conn.execute(text("TRUNCATE TABLE dessertes RESTART IDENTITY"))
            df.to_sql('dessertes', conn, if_exists='append', index=False, method='multi', chunksize=500)

        print(f"    {len(df)} dessertes chargées (sur {initial_count} avant filtre horaires)")
        self.log_etl("load", csv_path, len(df), "success",
                     f"{initial_count - len(df)} lignes sans horaires exclues")
        return len(df)

    def run_load(self):
        if not self.connect():
            return False
        self.create_schema()
        loaded = self.load_dessertes()
        if loaded > 0:
            print(" Chargement terminé")
            self.get_table_stats()
            return True
        return False

    def load_all_data(self, clean_first=True):
        return self.run_load()

    def get_table_stats(self):
        try:
            with self.engine.connect() as conn:
                count = conn.execute(text("SELECT COUNT(*) FROM dessertes")).scalar()
                stats = conn.execute(text("""
                    SELECT operateur_nom, COUNT(*) as nb
                    FROM dessertes
                    GROUP BY operateur_nom
                    ORDER BY nb DESC
                """)).fetchall()
                print(f"\n📊 Base de données :")
                print(f"   Total dessertes : {count:,}")
                for op, nb in stats:
                    print(f"      - {op}: {nb:,}")
        except Exception as e:
            print(f" Erreur stats : {e}")


if __name__ == "__main__":
    DataLoader().run_load()