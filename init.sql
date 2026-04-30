
CREATE TABLE IF NOT EXISTS operateur (
    id_operateur  SERIAL       PRIMARY KEY,
    nom           VARCHAR(100) NOT NULL UNIQUE,
    pays          VARCHAR(10)  NOT NULL
);

CREATE TABLE IF NOT EXISTS gare (
    id_gare  SERIAL       PRIMARY KEY,
    nom      VARCHAR(200) NOT NULL,
    pays     VARCHAR(10)  NOT NULL,
    UNIQUE   (nom, pays)
);

CREATE TABLE IF NOT EXISTS trajet (
    id_trajet  SERIAL   PRIMARY KEY,
    id_gare    INTEGER  NOT NULL REFERENCES gare(id_gare),
    distance   FLOAT
);

CREATE INDEX IF NOT EXISTS idx_trajet_gare ON trajet(id_gare);

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

CREATE TABLE IF NOT EXISTS etl_logs (
    id                 SERIAL    PRIMARY KEY,
    run_date           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etape              VARCHAR(50),
    source             VARCHAR(200),
    nb_enregistrements INTEGER,
    statut             VARCHAR(20),
    message            TEXT
);