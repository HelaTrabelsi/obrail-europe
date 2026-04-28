-- ============================================================
-- ObRail Europe — Initialisation de la base de données
-- RGPD : Aucune donnée personnelle n'est traitée.
-- Toutes les données sont issues de sources open data publiques.
-- ============================================================

-- Déssertes ferroviaires
CREATE TABLE IF NOT EXISTS dessertes (
    id                 SERIAL PRIMARY KEY,
    operateur_nom      VARCHAR(100),
    nom_ligne          TEXT,
    type_ligne         VARCHAR(50),           -- national / regional
    type_service       VARCHAR(20),           -- Jour / Nuit
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

-- Index pour les recherches fréquentes
CREATE INDEX IF NOT EXISTS idx_dessertes_operateur  ON dessertes(operateur_nom);
CREATE INDEX IF NOT EXISTS idx_dessertes_gares       ON dessertes(gare_depart_nom, gare_arrivee_nom);
CREATE INDEX IF NOT EXISTS idx_dessertes_type        ON dessertes(type_service, type_ligne);
CREATE INDEX IF NOT EXISTS idx_dessertes_heure       ON dessertes(heure_depart);

-- Table de traçabilité des chargements ETL (conformité RGPD - transparence)
CREATE TABLE IF NOT EXISTS etl_logs (
    id              SERIAL PRIMARY KEY,
    run_date        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etape           VARCHAR(50),    -- extract / transform / load
    source          VARCHAR(200),
    nb_enregistrements INTEGER,
    statut          VARCHAR(20),    -- success / error
    message         TEXT
);