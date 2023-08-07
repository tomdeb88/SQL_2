import sqlite3

conn = sqlite3.connect("database.db")
c = conn.cursor()

rows = c.execute(
    "SELECT name,date FROM stations,measures WHERE stations.station=measures.station LIMIT 7"
).fetchall()

for r in rows:
    print(r)
