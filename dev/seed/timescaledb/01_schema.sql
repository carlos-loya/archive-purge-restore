CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Hypertable: sensor_data (time-series, partitioned by time)
CREATE TABLE sensor_data (
    time        TIMESTAMPTZ NOT NULL,
    sensor_id   INT NOT NULL,
    temperature DOUBLE PRECISION,
    humidity    DOUBLE PRECISION,
    location    TEXT NOT NULL,
    PRIMARY KEY (time, sensor_id)
);

SELECT create_hypertable('sensor_data', 'time', chunk_time_interval => INTERVAL '1 month');

-- Regular table (non-hypertable) to test fallback behavior
CREATE TABLE events (
    id         SERIAL PRIMARY KEY,
    event_type TEXT NOT NULL,
    payload    TEXT,
    created_at TIMESTAMP NOT NULL
);
