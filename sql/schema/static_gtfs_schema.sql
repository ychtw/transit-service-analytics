-- ref: https://gtfs.org/documentation/schedule/reference/#
-- note: only include required and common optional files

-- agency.txt
CREATE OR REPLACE TABLE static_agency (
    agency_id TEXT PRIMARY KEY,
    agency_name TEXT NOT NULL,
    agency_url TEXT NOT NULL,
    agency_timezone TEXT NOT NULL,
    agency_lang TEXT,
    agency_phone TEXT,
    agency_fare_url TEXT,
    agency_email TEXT
);

-- stops.txt
CREATE OR REPLACE TABLE static_stops (
    stop_id TEXT PRIMARY KEY,
    stop_code TEXT,
    stop_name TEXT NOT NULL,
    stop_desc TEXT,
    -- TODO: temporary loose not null constraint for stop coordinates
    stop_lat DOUBLE, -- NOT NULL,
    stop_lon DOUBLE, -- NOT NULL,
    zone_id TEXT,
    stop_url TEXT,
    location_type INTEGER,
    parent_station TEXT,
    stop_timezone TEXT,
    wheelchair_boarding INTEGER,
    level_id TEXT,
    platform_code TEXT
);

-- routes.txt
CREATE OR REPLACE TABLE static_routes (
    route_id TEXT PRIMARY KEY,
    agency_id TEXT,
    route_short_name TEXT,
    route_long_name TEXT,
    route_desc TEXT,
    route_type INTEGER NOT NULL,
    route_url TEXT,
    route_color TEXT,
    route_text_color TEXT,
    FOREIGN KEY (agency_id) REFERENCES static_agency(agency_id)
);

-- trips.txt
CREATE OR REPLACE TABLE static_trips (
    trip_id TEXT PRIMARY KEY,
    route_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    trip_headsign TEXT,
    trip_short_name TEXT,
    direction_id INTEGER,
    block_id TEXT,
    shape_id TEXT,
    wheelchair_accessible INTEGER,
    bikes_allowed INTEGER,
    FOREIGN KEY (route_id) REFERENCES static_routes(route_id)
);

-- stop_times.txt
CREATE OR REPLACE TABLE static_stop_times (
    trip_id TEXT NOT NULL,
    arrival_time TEXT,
    departure_time TEXT,
    stop_id TEXT NOT NULL,
    stop_sequence INTEGER NOT NULL,
    stop_headsign TEXT,
    pickup_type INTEGER,
    drop_off_type INTEGER,
    shape_dist_traveled DOUBLE,
    timepoint INTEGER,
    PRIMARY KEY (trip_id, stop_sequence),
    FOREIGN KEY (trip_id) REFERENCES static_trips(trip_id),
    FOREIGN KEY (stop_id) REFERENCES static_stops(stop_id)
);

-- calendar.txt
CREATE OR REPLACE TABLE static_calendar (
    service_id TEXT PRIMARY KEY,
    monday INTEGER NOT NULL,
    tuesday INTEGER NOT NULL,
    wednesday INTEGER NOT NULL,
    thursday INTEGER NOT NULL,
    friday INTEGER NOT NULL,
    saturday INTEGER NOT NULL,
    sunday INTEGER NOT NULL,
    start_date TEXT NOT NULL,
    end_date TEXT NOT NULL
);

-- calendar_dates.txt
CREATE OR REPLACE TABLE static_calendar_dates (
    service_id TEXT NOT NULL,
    date TEXT NOT NULL,
    exception_type INTEGER NOT NULL,
    PRIMARY KEY (service_id, date),
    FOREIGN KEY (service_id) REFERENCES static_calendar(service_id)
);

-- shapes.txt
CREATE OR REPLACE TABLE static_shapes (
    shape_id TEXT NOT NULL,
    shape_pt_lat DOUBLE NOT NULL,
    shape_pt_lon DOUBLE NOT NULL,
    shape_pt_sequence INTEGER NOT NULL,
    shape_dist_traveled DOUBLE,
    PRIMARY KEY (shape_id, shape_pt_sequence)
);

-- frequencies.txt
CREATE OR REPLACE TABLE static_frequencies (
    trip_id TEXT NOT NULL,
    start_time TEXT NOT NULL,
    end_time TEXT NOT NULL,
    headway_secs INTEGER NOT NULL,
    exact_times INTEGER,
    FOREIGN KEY (trip_id) REFERENCES static_trips(trip_id)
);
