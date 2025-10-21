CREATE TABLE airlines (
    airline_id SERIAL PRIMARY KEY,
    iata_code VARCHAR(10) UNIQUE,
    airline_name VARCHAR(255)
);

CREATE TABLE airports (
    airport_id SERIAL PRIMARY KEY,
    iata_code VARCHAR(10) UNIQUE,
    airport_name VARCHAR(255),
    city VARCHAR(255),
    state VARCHAR(255),
    country VARCHAR(255)
);

CREATE TABLE flights (
    flight_id SERIAL PRIMARY KEY,
    airline_iata VARCHAR(10),
    origin_iata VARCHAR(10),
    dest_iata VARCHAR(10),
    dep_time INT,
    arr_time INT,
    air_time FLOAT,
    distance FLOAT,
    cancelled BOOLEAN,
    diverted BOOLEAN,
    FOREIGN KEY (airline_iata) REFERENCES airlines(iata_code),
    FOREIGN KEY (origin_iata) REFERENCES airports(iata_code),
    FOREIGN KEY (dest_iata) REFERENCES airports(iata_code)
);
