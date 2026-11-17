CREATE TABLE IF NOT EXISTS stations (
    station_id    VARCHAR(16)       PRIMARY KEY,
    station_name  TEXT              NOT NULL,
    basin         TEXT              NOT NULL,
    country       TEXT              NOT NULL,
    lat           DOUBLE PRECISION  CHECK (lat BETWEEN -90 AND 90),
    lon           DOUBLE PRECISION  CHECK (lon BETWEEN -180 AND 180)
);

CREATE TABLE IF NOT EXISTS sensor_readings (
    station_id      VARCHAR(16)       NOT NULL REFERENCES stations(station_id),
    event_time      TIMESTAMPTZ       NOT NULL,
    rainfall_mm     DOUBLE PRECISION  CHECK (rainfall_mm >= 0),
    level_raw       DOUBLE PRECISION,
    level_filtered  DOUBLE PRECISION,
    innovation      DOUBLE PRECISION,
    fault_flag      SMALLINT          NOT NULL DEFAULT 0 CHECK (fault_flag IN (0, 1)),
    sent_at         TIMESTAMPTZ,
    ingested_at     TIMESTAMPTZ       NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, event_time)
);

CREATE INDEX IF NOT EXISTS idx_readings_event_time
    ON sensor_readings (event_time);

CREATE INDEX IF NOT EXISTS idx_readings_faults
    ON sensor_readings (station_id, event_time)
    WHERE fault_flag = 1;

CREATE TABLE IF NOT EXISTS predictions (
    station_id  VARCHAR(16)       NOT NULL REFERENCES stations(station_id),
    event_time  TIMESTAMPTZ       NOT NULL,
    scenario    CHAR(1)           NOT NULL CHECK (scenario IN ('A', 'B')),
    level_pred  DOUBLE PRECISION,
    flood_prob  DOUBLE PRECISION  CHECK (flood_prob BETWEEN 0 AND 1),
    created_at  TIMESTAMPTZ       NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, event_time, scenario)
);

CREATE INDEX IF NOT EXISTS idx_predictions_event_time
    ON predictions (event_time);

CREATE TABLE IF NOT EXISTS flood_alerts (
    station_id  VARCHAR(16)       NOT NULL REFERENCES stations(station_id),
    event_time  TIMESTAMPTZ       NOT NULL,
    scenario    CHAR(1)           NOT NULL CHECK (scenario IN ('A', 'B')),
    flood_prob  DOUBLE PRECISION  NOT NULL CHECK (flood_prob BETWEEN 0 AND 1),
    threshold   DOUBLE PRECISION  NOT NULL CHECK (threshold BETWEEN 0 AND 1),
    created_at  TIMESTAMPTZ       NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, event_time, scenario)
);

CREATE INDEX IF NOT EXISTS idx_alerts_created_at
    ON flood_alerts (created_at DESC); 

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id              BIGSERIAL         PRIMARY KEY,
    batch_id        BIGINT            NOT NULL,
    stage           TEXT              NOT NULL,
    records         INTEGER           NOT NULL CHECK (records >= 0),
    throughput_rps  DOUBLE PRECISION,
    latency_ms      DOUBLE PRECISION,
    measured_at     TIMESTAMPTZ       NOT NULL DEFAULT now()
);