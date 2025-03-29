-- main table from .pb
CREATE OR REPLACE TABLE realtime_trip_updates (
    timestamp TIMESTAMP, -- When this update was received (header.timestamp)
    trip_id TEXT, -- trip_descriptor.trip_id
    route_id TEXT, -- trip_descriptor.route_id
    start_date TEXT, -- trip_descriptor.start_date
    start_time TEXT, -- trip_descriptor.start_time
    vehicle_id TEXT, -- vehicle.id
    vehicle_label TEXT, -- vehicle.label
    stop_id TEXT, -- stop_time_update.stop_id
    stop_sequence INTEGER, -- stop_time_update.stop_sequence
    arrival_time TIMESTAMP, -- arrival.time (converted from epoch)
    arrival_delay INTEGER, -- arrival.delay (seconds)
    departure_time TIMESTAMP, -- departure.time (converted)
    departure_delay INTEGER -- departure.delay (seconds)
);


-- ingestion log table for idempotency
CREATE TABLE IF NOT EXISTS realtime_ingestion_log (
    filename TEXT PRIMARY KEY,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
