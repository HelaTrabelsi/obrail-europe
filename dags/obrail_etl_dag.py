"""
ObRail Europe — DAG Airflow
Pipeline ETL automatise toutes les heures
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── Configuration par defaut ──────────────────────────────────────
default_args = {
    "owner": "obrail",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ── Definition du DAG ─────────────────────────────────────────────
with DAG(
    dag_id="obrail_etl_pipeline",
    description="Pipeline ETL ObRail — Extract GTFS + Transform + Load PostgreSQL",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *", 
    catchup=False,
    tags=["obrail", "etl", "ferroviaire"],
) as dag:

    # ── Tache 1 : EXTRACT ─────────────────────────────────────────
    extract = BashOperator(
        task_id="extract",
        bash_command="cd /opt/airflow/obrail && python src/pipeline.py --step extract",
        doc_md="""
        ## Extract
        Telechargement des 5 sources GTFS :
        - SNCF TER + Intercites (France)
        - Deutsche Bahn FV + RV (Allemagne)
        - SNCB iRail (Belgique)
        + API SNCF CO2 (reference carbone)
        """,
    )

    # ── Tache 2 : TRANSFORM ───────────────────────────────────────
    transform = BashOperator(
        task_id="transform",
        bash_command="cd /opt/airflow/obrail && python src/pipeline.py --step transform",
        doc_md="""
        ## Transform
        - Filtre horaires manquants (NOT NULL)
        - Deduplication sur cle unique
        - Normalisation UTF-8
        - Detection Jour/Nuit
        - Calcul distance Haversine
        - Export CSV + Parquet
        """,
    )

    # ── Tache 3 : LOAD ────────────────────────────────────────────
    load = BashOperator(
        task_id="load",
        bash_command="cd /opt/airflow/obrail && python src/pipeline.py --step load",
        doc_md="""
        ## Load
        Chargement dans PostgreSQL 16 :
        1. operateur (3 lignes)
        2. gare (~3017 lignes)
        3. trajet (~2890 lignes)
        4. train (~99854 lignes)
        + log dans etl_logs
        """,
    )

    # ── Tache 4 : VERIFICATION ────────────────────────────────────
    def verifier_chargement(**context):
        """Verifie que le chargement s'est bien passe"""
        import psycopg2
        import os

        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "db"),
            port=os.getenv("DB_PORT", "5432"),
            database=os.getenv("DB_NAME", "obrail_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM train")
        nb = cur.fetchone()[0]
        conn.close()

        print(f"✅ Verification : {nb:,} trains en base")

        if nb < 1000:
            raise ValueError(f"❌ Nombre de trains insuffisant : {nb}")

        return nb

    verifier = PythonOperator(
        task_id="verifier_chargement",
        python_callable=verifier_chargement,
        doc_md="Verifie que le nombre de trains en base est suffisant (> 1000).",
    )

    # ── Ordre d'execution ─────────────────────────────────────────
    # extract → transform → load → verifier
    extract >> transform >> load >> verifier