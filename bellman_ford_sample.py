import pickle
import json
from typing import List
from data import Connection, Customer, Employee, Station, Ticket

# try on huge amount of data and see if performance is reasonable
with open('data.pickle', 'rb') as f:
    res = pickle.load(f)    

stations = res['stations']

station_by_id = dict()
for station in stations:
    station_by_id[station.id] = station

connections: List[Connection] = res['connections']

def weight(conn: Connection):
    return conn.cost

FROM = 8502004
TO = 8503000
distances = dict()
parent = dict()
distances[FROM] = 0
done = False
i = 0
while not done:
    done = True
    for conn in connections:
        res_exp = conn.to_station_id in distances
        src_exp = conn.from_station_id in distances
        if (not res_exp and src_exp) or (res_exp and src_exp and distances[conn.to_station_id] > distances[conn.from_station_id] + weight(conn)):
            distances[conn.to_station_id] = distances[conn.from_station_id] + weight(conn)
            parent[conn.to_station_id] = conn.from_station_id
            done = False
    i += 1

with open('dist.json', 'w') as f:
    json.dump(distances, f)

current = TO
while current is not None:
    print(station_by_id[current].name)
    if current not in parent:
        break

    current = parent[current]