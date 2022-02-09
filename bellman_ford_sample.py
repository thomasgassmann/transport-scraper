import pickle
import time
import json
from typing import List
from data import Connection, Customer, Employee, Station, Ticket, BUS, TRAIN, PLANE
from bellman_ford import bellman_ford

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

FROM = 8503000
TO = 8581018

start = time.time()
(distances, parent, via, iterations) = bellman_ford(FROM, connections, weight)
end = time.time()
print(f'Took {end - start}')

with open('dist.json', 'w') as f:
    json.dump(distances, f)

current = TO
total_cost = 0
total_duration = 0
while current is not None:
    print(station_by_id[current].name)
    if current not in parent:
        break

    if via[current].transport_type & BUS == BUS:
        transport_type = 'bus'
    elif via[current].transport_type & TRAIN == TRAIN:
        transport_type = 'train'
    else:
        transport_type = 'plane'
    print(f'{weight(via[current])} - {transport_type}')
    total_cost += via[current].cost
    total_duration += via[current].duration
    current = parent[current]

print(f'\nTotal cost: {total_cost}')
print(f'Total duration: {total_duration}')
