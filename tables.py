import csv, sqlite3
from sqlite3 import Error


try:
    conn = sqlite3.connect("database.db")
    c = conn.cursor()
    table_stations = """CREATE TABLE stations( 
    station TEXT PRIMARY KEY NOT NULL,
    latitude NUMERIC,
    longitude NUMERIC,
    elevation NUMERIC,
    name TEXT,
    country TEXT,
    state TEXT
    );"""

    tabel_measure = """CREATE TABLE measures (
    id integer PRIMARY KEY,
    station TEXT,
    date TEXT,
    precip NUMERIC,
    tobs NUMERIC,
    FOREIGN KEY (station) REFERENCES station (station)

    );"""

    c.execute(table_stations)
    c.execute(tabel_measure)

    with open("clean_stations.csv") as csv_file:
        dr = csv.DictReader(csv_file)
        to_db = [
            (
                i["station"],
                i["latitude"],
                i["longitude"],
                i["elevation"],
                i["name"],
                i["country"],
                i["state"],
            )
            for i in dr
        ]

    c.executemany(
        "INSERT INTO stations (station,latitude,longitude,elevation,name,country,state) VALUES(?,?,?,?,?,?,?)",
        to_db,
    )

    with open("clean_measure.csv") as csv_file:
        dr = csv.DictReader(csv_file)
        to_db2 = [
            (
                i["station"],
                i["date"],
                i["precip"],
                i["tobs"],
            )
            for i in dr
        ]

    c.executemany(
        "INSERT INTO measures (station,date,precip,tobs) VALUES(?,?,?,?)",
        to_db2,
    )
    conn.commit()
    c.close()

except Error as e:
    print(e)

finally:
    c.close()
